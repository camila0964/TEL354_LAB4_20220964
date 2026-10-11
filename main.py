import yaml
import requests
import asyncio
import aiohttp

CONTROLLER_IP = "127.0.0.1"
FLOW_PUSHER = f"http://{CONTROLLER_IP}:8080/wm/staticflowpusher/json"
SWITCHES = {
    "00:00:a2:23:f2:c0:45:47": "sw1",
    "00:00:1a:74:90:39:89:4a": "sw2",
    "00:00:ca:27:2d:26:57:44": "sw3",
}  # Valores obtenidos con list_switches.py


# ===================== Clases =====================
class Student:
    def __init__(self, name, mac):
        self.name = name
        self.mac = mac


class Service:
    def __init__(self, name, protocol, port):
        self.name = name
        self.protocol = protocol
        self.port = port


class Server:
    def __init__(self, name, ip, services):
        self.name = name
        self.ip = ip
        self.services = [Service(s["name"], s["protocol"], s["port"]) for s in services]

    def get_service(self, name):
        return next((s for s in self.services if s.name == name), None)


class Course:
    def __init__(self, name, state, students, servers, code=None):
        self.name = name
        self.code = code
        self.state = state
        self.students = list(students)  # nombres de alumnos
        self.servers = servers          # [{"name":..., "allowed_services":[...]}]

    def add_student(self, name):
        if name in self.students:
            raise ValueError(f"{name} ya está matriculado en {self.code}")
        self.students.append(name)

    def remove_student(self, name):
        if name not in self.students:
            raise ValueError(f"{name} no está matriculado en {self.code}")
        self.students.remove(name)


# ===================== Datos en memoria =====================
students, servers, courses = [], [], []
connections = {}   # handler -> {"alumno", "servidor", "servicio", "flows"}
contador = 0       # para generar handlers c1, c2, ...


def load_data(filename):
    with open(filename, "r") as file:
        data = yaml.safe_load(file)
    students = [Student(x["name"], x["mac"]) for x in data["students"]]
    servers = [Server(y["name"], y["ip"], y["services"]) for y in data["servers"]]
    courses = [Course(z["name"], z["state"], z["students"], z["servers"], z["code"]) for z in data["courses"]]
    return students, servers, courses


def buscar(lista, valor, campo="name"):
    """Devuelve el objeto de la lista cuyo 'campo' sea igual a 'valor' (o None)."""
    return next((x for x in lista if getattr(x, campo) == valor), None)


# ===================== Consultas al controlador =====================
def get_attachment_point(controller_ip, mac):
    endpoint = '/wm/device/'
    url = 'http://' + controller_ip + ':8080' + endpoint
    response = requests.get(url, params={'mac': mac}, timeout=5)
    result, = response.json()
    attachment_points = result.get("attachmentPoint", [])

    if not attachment_points:
        return None, None

    attachment_point, = attachment_points
    return attachment_point["switchDPID"], attachment_point["port"]


def get_route(controller_ip, src_mac, dst_mac):
    src_sw_dpid, src_sw_port = get_attachment_point(controller_ip, src_mac)
    dst_sw_dpid, dst_sw_port = get_attachment_point(controller_ip, dst_mac)
    endpoint = f'/wm/topology/route/{src_sw_dpid}/{src_sw_port}/{dst_sw_dpid}/{dst_sw_port}/json'
    url = 'http://' + controller_ip + ':8080' + endpoint
    response = requests.get(url, timeout=5)
    return response.json()


def get_mac_by_ip(controller_ip, ip):
    url = f"http://{controller_ip}:8080/wm/device/"
    response = requests.get(url, params={"ipv4": ip}, timeout=5)
    hosts = response.json()  # lista de hosts con esa IP

    if len(hosts) == 0:  # Floodlight todavía no conoce esa IP
        return None

    primer_host = hosts[0]
    mac = primer_host["mac"][0]  # "mac" también es una lista
    return mac


# ===================== Autorización =====================
def is_authorized(student, server, service_name, courses):
    for c in courses:
        if c.state != "ACTIVE" or student.name not in c.students:
            continue
        for s in c.servers:
            if s["name"] == server.name and service_name in s["allowed_services"]:
                return True
    return False


# ===================== Flows (asíncrono) =====================
async def enviar_flow(session, metodo, payload):
    try:
        async with session.request(metodo, FLOW_PUSHER, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as r:
            return payload["name"], r.status, (await r.text()).strip()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        return payload["name"], f"Error ({type(e).__name__})", ""


async def push_flows(flows):
    async with aiohttp.ClientSession() as session:
        tareas = [enviar_flow(session, "POST", f) for f in flows]
        resultados = await asyncio.gather(*tareas)
    for nombre, estado, texto in resultados:
        print(f"  [{estado}] {nombre}: {texto}")


async def delete_flows(names):
    async with aiohttp.ClientSession() as session:
        tareas = [enviar_flow(session, "DELETE", {"name": n}) for n in names]
        resultados = await asyncio.gather(*tareas)
    for nombre, estado, texto in resultados:
        print(f"  [{estado}] {nombre}: {texto}")


def crear_flow(nombre, dpid, puerto_entrada, puerto_salida, match):
    """Arma una flow entry: lo que llega por puerto_entrada y cumple 'match'
    se envía por puerto_salida."""
    flow = {
        "switch": dpid,
        "name": nombre,
        "priority": "100",
        "active": "true",
        "in_port": str(puerto_entrada),
        "actions": f"output={puerto_salida}",
    }
    flow.update(match)  # agrega los campos de matching
    return flow


def build_route(route, student, server, service, handler):
    flows = []

    # La ruta viene en pares: [entrada, salida] por cada switch
    for i in range(0, len(route), 2):
        dpid = route[i]["switch"]
        p_alumno = route[i]["port"]["portNumber"]        # puerto hacia el alumno
        p_servidor = route[i + 1]["port"]["portNumber"]  # puerto hacia el servidor
        sw = SWITCHES.get(dpid, dpid)                    # nombre real del switch
        nombre = f"{handler}_{sw}"

        # 1. Alumno -> Servidor (L2: MAC origen, L3: IP destino, L4: puerto destino)
        flows.append(crear_flow(f"{nombre}_ida", dpid, p_alumno, p_servidor, {
            "eth_type": "0x0800",
            "eth_src": student.mac,
            "ipv4_dst": server.ip,
            "ip_proto": "0x06",
            "tcp_dst": str(service.port),
        }))

        # 2. Servidor -> Alumno (al revés)
        flows.append(crear_flow(f"{nombre}_vuelta", dpid, p_servidor, p_alumno, {
            "eth_type": "0x0800",
            "eth_dst": student.mac,
            "ipv4_src": server.ip,
            "ip_proto": "0x06",
            "tcp_src": str(service.port),
        }))

        # 3 y 4. ARP en ambos sentidos
        flows.append(crear_flow(f"{nombre}_arp_ida", dpid, p_alumno, p_servidor, {"eth_type": "0x0806"}))
        flows.append(crear_flow(f"{nombre}_arp_vuelta", dpid, p_servidor, p_alumno, {"eth_type": "0x0806"}))

    asyncio.run(push_flows(flows))      # inserción asíncrona
    return [f["name"] for f in flows]   # para poder borrarlos después


# ===================== Menús =====================
def menu_cursos():
    op = input("\n--- Cursos ---\n1) Listar\n2) Mostrar detalles\n3) Gestionar alumnos\n> ")
    if op == "1":
        for c in courses:
            print(f"  {c.code}  {c.name}  ({c.state})")
        return

    c = buscar(courses, input("Código del curso: "), "code")
    if not c:
        print("Curso no encontrado")
    elif op == "2":
        print(f"{c.name} ({c.code}) - {c.state}")
        print("Alumnos:", ", ".join(c.students))
        for s in c.servers:
            print(f"Servidor {s['name']}: {', '.join(s['allowed_services'])}")
    elif op == "3":
        accion = input("1) Agregar\n2) Eliminar\n> ")
        nombre = input("Nombre del alumno: ")
        try:
            if accion == "1":
                c.add_student(nombre)
            else:
                c.remove_student(nombre)
            print("Listo")
        except ValueError as e:
            print("Error:", e)


def menu_alumnos():
    op = input("\n--- Alumnos ---\n1) Crear\n2) Listar\n3) Mostrar detalles\n> ")
    if op == "1":
        students.append(Student(input("Nombre: "), input("MAC: ").lower()))
        print("Alumno creado")
    elif op == "2":
        codigo = input("Código de curso para filtrar (Enter = todos): ")
        c = buscar(courses, codigo, "code")
        for s in students:
            if not codigo or (c and s.name in c.students):
                print(f"  {s.name}  {s.mac}")
    elif op == "3":
        s = buscar(students, input("Nombre del alumno: "))
        if s:
            print(f"Nombre: {s.name}\nMAC: {s.mac}")
        else:
            print("Alumno no encontrado")


def menu_servidores():
    op = input("\n--- Servidores ---\n1) Listar\n2) Mostrar servicios\n> ")
    if op == "1":
        for s in servers:
            print(f"  {s.name}  {s.ip}")
    elif op == "2":
        s = buscar(servers, input("Nombre del servidor: "))
        if s:
            print(f"{s.name} - IP: {s.ip}")
            for x in s.services:
                print(f"  {x.name}  {x.protocol}/{x.port}")
        else:
            print("Servidor no encontrado")


def menu_politicas():
    # Cursos que dan acceso a un servicio de un servidor
    servidor = input("Nombre del servidor: ")
    servicio = input("Servicio: ")
    for c in courses:
        for s in c.servers:
            if s["name"] == servidor and servicio in s["allowed_services"]:
                print(f"  {c.code}  {c.name}  ({c.state})")


def menu_conexiones():
    global contador
    op = input("\n--- Conexiones ---\n1) Crear\n2) Listar\n3) Borrar\n> ")

    if op == "1":
        alumno = buscar(students, input("Nombre del alumno: "))
        servidor = buscar(servers, input("Nombre del servidor: "))
        servicio = servidor.get_service(input("Servicio: ")) if servidor else None
        if not alumno or not servicio:
            print("Datos inválidos")
            return
        if not is_authorized(alumno, servidor, servicio.name, courses):
            print(f"Error: {alumno.name} no está autorizado para {servicio.name} en {servidor.name}")
            return

        mac_servidor = get_mac_by_ip(CONTROLLER_IP, servidor.ip)
        if not mac_servidor:
            print("Floodlight no conoce al servidor: haga ping desde H3 y reintente")
            return
        try:
            route = get_route(CONTROLLER_IP, alumno.mac, mac_servidor)
        except ValueError:
            print("Floodlight no conoce al alumno: haga ping desde H1 y reintente")
            return
        
        contador += 1
        handler = f"c{contador}"
        flows = build_route(route, alumno, servidor, servicio, handler)
        connections[handler] = {"alumno": alumno.name, "servidor": servidor.name,"servicio": servicio.name, "flows": flows}
        print(f"Conexión creada: {handler}")

    elif op == "2":
        for h, c in connections.items():
            print(f"  {h}: {c['alumno']} -> {c['servidor']} ({c['servicio']})")

    elif op == "3":
        handler = input("Handler: ")
        if handler in connections:
            asyncio.run(delete_flows(connections.pop(handler)["flows"]))
            print(f"Conexión {handler} borrada")
        else:
            print("Handler no encontrado")


def menu():
    global students, servers, courses
    while True:
        print("\n===== Menú principal =====")
        print("1) Importar\n2) Exportar\n3) Cursos\n4) Alumnos")
        print("5) Servidores\n6) Políticas\n7) Conexiones\n8) Salir")
        op = input("> ")
        if op == "1":
            students, servers, courses = load_data(input("Nombre de archivo: "))
        elif op == "2":
            print("Opción no implementada")
        elif op == "3":
            menu_cursos()
        elif op == "4":
            menu_alumnos()
        elif op == "5":
            menu_servidores()
        elif op == "6":
            menu_politicas()
        elif op == "7":
            menu_conexiones()
        elif op == "8":
            break


# ===================== Main =====================
def main():
    global students, servers, courses
    students, servers, courses = load_data("database.yaml")
    menu()


if __name__ == "__main__":
    main()