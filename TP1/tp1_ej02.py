import csv

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp1_ej02.txt'

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
    especialidades = {
        'id_tipo_especialidad': fila['id_tipo_especialidad'],
        'nombre_tipo_especialidad': fila['nombre_tipo_especialidad'],
        'id_especialidad': fila['id_especialidad'],
        'nombre_especialidad': fila['nombre_especialidad']
    }
    database.append(especialidades)
    pass
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(database):

    especialidades_unicas = set()
    archivo = open(nombre_archivo_resultado_ejercicio, 'w')

    # Lista para almacenar las líneas a grabar
    registros = []

    for fila in database:
        par = (fila['id_especialidad'], fila['id_tipo_especialidad'])
        if par not in especialidades_unicas:
            especialidades_unicas.add(par)
            # Crear la línea a grabar
            linea = f"Tipo/Especialidad: {fila['nombre_tipo_especialidad']}, {fila['nombre_especialidad']}"
            registros.append(linea)

    # Ordeno las líneas por nombre_tipo_especialidad y luego por nombre_especialidad
    registros.sort(key=lambda x: (x.split(", ")[0], x.split(", ")[1]))

    # Grabar las líneas ordenadas en el archivo
    for linea in registros:
        grabar_linea(archivo, linea)

    pass

# Funcion para el borrado de estructuras generadas para este ejercicio
# Debe ser implementada por el alumno
def finalizar(database):
    pass
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
