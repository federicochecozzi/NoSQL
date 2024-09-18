import csv

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp1_ej06.txt'

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
    podio = {
        'nombre_especialidad': fila['nombre_especialidad'],
        'nombre_torneo': fila['nombre_torneo'],
        'nombre_deportista': fila['nombre_deportista'],
        'marca': fila['marca']
    }
    database.append(podio)
    pass
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(database):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w')

    torneos = {}

    for fila in database:
        if fila['nombre_especialidad'] == 'carrera 100 m':
            nombre_torneo = fila['nombre_torneo']

            if nombre_torneo not in torneos:
                torneos[nombre_torneo] = []

            # Agregar la fila relevante al torneo correspondiente
            torneos[nombre_torneo].append({
                'nombre_torneo': fila['nombre_torneo'],
                'nombre_especialidad': fila['nombre_especialidad'],
                'nombre_deportista': fila['nombre_deportista'],
                'marca': float(fila['marca'])  # Convertimos la marca a número
            })

    archivo = open(nombre_archivo_resultado_ejercicio, 'w')

    for torneo, participantes in torneos.items():
        participantes.sort(key=lambda x: x['marca'])

        grabar_linea(archivo, f"Podio del torneo: {torneo}, Especialidad: carrera 100 m")
        for posicion, deportista in enumerate(participantes[:3], start=1):
            linea = (f"Posición {posicion}: "
                     f"Torneo: {deportista['nombre_torneo']}, "
                     f"Especialidad: {deportista['nombre_especialidad']}, "
                     f"Deportista: {deportista['nombre_deportista']}, "
                     f"Marca: {deportista['marca']}")
            grabar_linea(archivo, linea)
        grabar_linea(archivo, "")

    pass

# Funcion para el borrado de estructuras generadas para este ejercicio
# Debe ser implementada por el alumno
def finalizar(database):
    pass
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
