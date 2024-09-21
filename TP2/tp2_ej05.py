import csv
import redis
import time

# Ubicacion del archivo CSV con el contenido provisto por la catedra
#archivo_entrada = 'full_export_version_corta.csv'
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp2_ej05.txt'

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
    db.set(fila['nombre_especialidad'],fila['nombre_tipo_especialidad'])
    db.zadd(fila['nombre_deportista'] + ":" + fila['nombre_especialidad'],{fila['nombre_torneo'] + ":" + fila['intento']:fila['marca']})
    #db.sadd("deportista:especialidad", fila['nombre_deportista'] + ":" + fila['nombre_especialidad'])
    db.zadd("deportista:especialidad", {fila['nombre_deportista'] + ":" + fila['nombre_especialidad']:0})


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding='utf-8')
    linea = "nombre_deportista,nombre_especialidad,nombre_torneo_peor,intento_peor,marca_peor,nombre_torneo_mejor,intento_mejor,marca_mejor"
    grabar_linea(archivo, linea)
    for d_e in db.zrange("deportista:especialidad",0,-1):#db.smembers("deportista:especialidad"):
        nombre_deportista,nombre_especialidad = d_e.split(sep = ':')
        tipo_especialidad = db.get(nombre_especialidad) 
        if (tipo_especialidad == 'tiempo'):
            datos_intento = db.zrange(d_e,0,0,withscores= True, score_cast_func=str)[0]
            nombre_torneo_mejor, intento_mejor = datos_intento[0].split(sep = ':')
            marca_mejor = datos_intento[1]
            datos_intento = db.zrange(d_e,0,0,desc = True, withscores= True, score_cast_func=str)[0]
            nombre_torneo_peor, intento_peor = datos_intento[0].split(sep = ':')
            marca_peor = datos_intento[1]
        else:
            datos_intento = db.zrange(d_e,0,0,desc = True, withscores= True, score_cast_func=str)[0]
            nombre_torneo_mejor, intento_mejor = datos_intento[0].split(sep = ':')
            marca_mejor = datos_intento[1]
            datos_intento = db.zrange(d_e,0,0, withscores= True, score_cast_func=str)[0]
            nombre_torneo_peor, intento_peor = datos_intento[0].split(sep = ':')
            marca_peor = datos_intento[1]
        linea = ','.join([nombre_deportista,nombre_especialidad,nombre_torneo_peor,intento_peor,marca_peor,nombre_torneo_mejor,intento_mejor,marca_mejor])
        grabar_linea(archivo, linea)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.flushdb()
    # Borrar la estructura de la base de datos

# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)