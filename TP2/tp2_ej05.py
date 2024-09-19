import csv
import redis

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export_version_corta.csv'
#archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp2_ej05.txt'

# Objeto de configuracion para conectarse a la base de datos usada en este ejercicio
conexion = {
    'redisurl': 'localhost',
    'redispuerto': 6379
}


# Funcion que dada la configuracion y ubicacion del archivo, carga la base de datos, genera el reporte, y borra la
# base de datos
def ejecutar(file, conn):
    import time

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
    db.sadd("deportista:especialidad", fila['nombre_deportista'] + ":" + fila['nombre_especialidad'])
    #print(deportista)

#codigo para testear conexion
#db.set("pruebatp","grupo1")
#Pasar la lectura de cada fila hacia redis
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno



def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    linea = "nombre_deportista,nombre_especialidad,nombre_torneo_peor,intento_peor,marca_peor,nombre_torneo_mejor,intento_mejor,marca_mejor"
    grabar_linea(archivo, linea)
    for d_e in db.smembers("deportista:especialidad"):
        nombre_deportista,nombre_especialidad = d_e.split(sep = ':')
        tipo_especialidad = db.get(nombre_especialidad) 
        if (tipo_especialidad == 'tiempo'):
            range_list = db.zrange(d_e,0,0,withscores= True)[0]
            nombre_torneo_mejor, intento_mejor = range_list[0].split(sep = ':')
            marca_mejor = range_list[1]
            range_list = db.zrange(d_e,0,0,desc = True, withscores= True)[0]
            nombre_torneo_peor, intento_peor = range_list[0].split(sep = ':')
            marca_peor = range_list[1]
        else:
            range_list = db.zrange(d_e,0,0,desc = True, withscores= True)[0]
            nombre_torneo_mejor, intento_mejor = range_list[0].split(sep = ':')
            marca_mejor = range_list[1]
            range_list = db.zrange(d_e,0,0, withscores= True)[0]
            nombre_torneo_peor, intento_peor = range_list[0].split(sep = ':')
            marca_peor = range_list[1]
        linea = ','.join([nombre_deportista,nombre_especialidad,nombre_torneo_peor,intento_peor,str(marca_peor),nombre_torneo_mejor,intento_mejor,str(marca_mejor)])
        grabar_linea(archivo, linea)
    # ids_deportistas = ['1', '2', '3']
    # for deportista in ids_deportistas:
    #     deportista_seleccionado = db.hgetall("id_deportista_info:"+deportista)
    #     cantidad_especialidades = db.scard("id_deportista_especialidades:"+deportista)
    #     print(cantidad_especialidades)
    #     linea = deportista+","+deportista_seleccionado.get("nombre_deportista")+","+deportista_seleccionado.get("fecha_nacimiento")+","+deportista_seleccionado.get("nombre_pais_deportista")+","+str(cantidad_especialidades)
    #     grabar_linea(archivo, linea)

    # luego para cada linea generada como reporte:
    # grabar_linea(archivo, linea)




# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    #pass
    db.flushdb()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)

