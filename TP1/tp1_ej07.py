import csv

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp1_ej07.txt'

# Objeto de configuracion para conectarse a la base de datos usada en este ejercicio
conexion = []


# Funcion que dada la configuracion y ubicacion del archivo, carga la base de datos, genera el reporte, y borra la
# base de datos
def ejecutar(file, conn):
    db = inicializar(conn)
    df_filas = csv.DictReader(open(file, "r", encoding="utf-8"))
    for fila in df_filas:
        procesar_fila(db, fila)
    generar_reporte(db)
    finalizar(db)


# Funcion que dado un archivo abierto y una linea, imprime por consola y guarda al final de archivo esa linea
def grabar_linea(archivo, linea):
    print(linea)
    archivo.write(linea+'\n')


# Funcion para poner el codigo que cree las estructuras a usarse en el este ejercicio
# Debe ser implementada por el alumno
def inicializar(conn):
    return []
    # crear db


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(database, fila):
    registros = {
        'id_especialidad': fila['id_especialidad'],
        'id_torneo': fila['id_torneo'],
        'nombre_especialidad': fila['nombre_especialidad'],
        'nombre_torneo': fila['nombre_torneo'],
        'nombre_deportista': fila['nombre_deportista'],
        'nombre_tipo_especialidad': fila['nombre_tipo_especialidad'],
        'marca': fila['marca'],

    }
    database.append(registros)
    pass
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(database):

    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding='utf-8')

    # Diccionario para agrupar los deportistas por especialidad
    especialidades = {}

    # Agrupar los datos por especialidad
    for fila in database:
        nombre_especialidad = fila['nombre_especialidad']
        nombre_tipo_especialidad = fila['nombre_tipo_especialidad']

        if nombre_especialidad not in especialidades:
            especialidades[nombre_especialidad] = []

        # Agregar el participante a la lista correspondiente dentro de la especialidad
        especialidades[nombre_especialidad].append({
            'nombre_torneo': fila['nombre_torneo'],
            'nombre_deportista': fila['nombre_deportista'],
            'marca': float(fila['marca']),
            'nombre_tipo_especialidad': fila['nombre_tipo_especialidad'],
        })

    # Ordenar las especialidades por nombre alfabéticamente
    for nombre_especialidad in sorted(especialidades.keys()):
        participantes = especialidades[nombre_especialidad]

        # Determinar el tipo de especialidad y ordenar en consecuencia
        if participantes and participantes[0]['nombre_tipo_especialidad'] == 'tiempo':
            # Ordenar por tiempo de menor a mayor
            participantes.sort(key=lambda x: x['marca'])
        else:
            # Ordenar por distancia de mayor a menor
            participantes.sort(key=lambda x: x['marca'], reverse=True)

        # Generar el podio para los top 3 participantes en cada especialidad
        grabar_linea(archivo, f"Podio de la especialidad: {nombre_especialidad}")
        for posicion, deportista in enumerate(participantes[:3], start=1):
            linea = (f"Posición {posicion} - "
                     f"{deportista['nombre_torneo']} - "
                     f"Deportista: {deportista['nombre_deportista']} - "
                     f"Marca: {deportista['marca']}")
            grabar_linea(archivo, linea)
        grabar_linea(archivo, "")  # Línea en blanco para separar las secciones

    pass

# Funcion para el borrado de estructuras generadas para este ejercicio
# Debe ser implementada por el alumno
def finalizar(database):
    pass
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)