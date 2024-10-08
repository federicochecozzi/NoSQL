import csv
import redis
import time

# Ubicacion del archivo CSV con el contenido provisto por la catedra
#archivo_entrada = 'full_export_version_corta.csv'
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp2_ej04.txt'

# Objeto de configuracion para conectarse a la base de datos usada en este ejercicio
conexion = {
    'redisurl': 'localhost',
    'redispuerto': 6379
}


# Funcion que dada la configuracion y ubicacion del archivo, carga la base de datos, genera el reporte, y borra la
# base de datos
def ejecutar(file, conn):
    start = time.time()
    db = inicializar(conn)
    df_filas = csv.DictReader(open(file, "r", encoding="utf-8"))
    count = 0
    startbloque = time.time()
    for fila in df_filas:
        procesar_fila(db, fila)
        count += 1
        if 0 == count % 100:
            endbloque = time.time()
            tiempo = endbloque - startbloque
            print(str(count) + " en " + str(tiempo) + " segundos")
            startbloque = time.time()
    generar_reporte(db)
    finalizar(db)
    end = time.time()
    print("tiempo total en segundos")
    print(end - start)


# Funcion que dado un archivo abierto y una linea, imprime por consola y guarda al final de archivo esa linea
def grabar_linea(archivo, linea):
    print(linea)
    archivo.write(str(linea) + '\n')

def inicializar(conn):
    r = redis.Redis(conn["redisurl"], conn["redispuerto"], db=0, decode_responses=True)
    return r
    # crear db

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    #print(deportista)
    db.sadd("ids_tipo_especialidad",fila['id_tipo_especialidad'])
    db.set('nombre:id_tipo_especialidad:' + fila['id_tipo_especialidad'],fila['nombre_tipo_especialidad'])
    db.sadd("especialidades:id_tipo_especialidad:"+fila['id_tipo_especialidad'], fila['nombre_especialidad'])

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding='utf-8')
    linea = "id_tipo_especialidad,nombre_tipo_especialidad,cantidad_especialidades"
    grabar_linea(archivo, linea)
    for id_t in db.smembers("ids_tipo_especialidad"):
        nombre = db.get('nombre:id_tipo_especialidad:' + id_t)
        cantidad_especialidades = db.scard("especialidades:id_tipo_especialidad:" + id_t)
        linea = ','.join([id_t,nombre,str(cantidad_especialidades)])
        grabar_linea(archivo, linea)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.flushdb()
    # Borrar la estructura de la base de datos

# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
