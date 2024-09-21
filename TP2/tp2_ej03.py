import csv
import redis
import time

# Ubicacion del archivo CSV con el contenido provisto por la catedra
#archivo_entrada = 'full_export_version_corta.csv'
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp2_ej03.txt'

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

    deportista = {
        'nombre_deportista': fila['nombre_deportista'],
        'fecha_nacimiento': fila['fecha_nacimiento'],
        'nombre_pais_deportista': fila['nombre_pais_deportista'],
    }
    #print(deportista)
    db.hset("id_deportista_info:"+fila['id_deportista'], mapping=deportista)
    db.sadd("id_deportista_especialidades:"+fila['id_deportista'],fila['nombre_especialidad'])

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding='utf-8')
    linea = "id_deportista,deportista,fecha_nacimiento,nombre_pais_deportista,cantidad_especialidades"
    grabar_linea(archivo, linea)
    ids_deportistas = ['10', '20', '30','229']
    for id_deportista in ids_deportistas:
        datos = db.hgetall("id_deportista_info:" + id_deportista)
        cantidad_especialidades = db.scard("id_deportista_especialidades:"+id_deportista)
        #print(cantidad_especialidades)
        linea = ",".join([id_deportista,datos["nombre_deportista"],datos["fecha_nacimiento"],datos["nombre_pais_deportista"],str(cantidad_especialidades)])
        #linea = id_deportista+","+deportista_seleccionado.get("nombre_deportista")+","+deportista_seleccionado.get("fecha_nacimiento")+","+deportista_seleccionado.get("nombre_pais_deportista")+","+str(cantidad_especialidades)
        grabar_linea(archivo, linea)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.flushdb()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)