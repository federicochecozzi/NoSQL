import csv
from cassandra.cluster import Cluster
import traceback
from itertools import chain

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
#archivo_entrada = 'full_export_version_corta.csv'
nombre_archivo_resultado_ejercicio = 'tp3_ej04.txt'

# Objeto de configuracion para conectarse a la base de datos usada en este ejercicio
conexion = {
    'cassandraurl': '172.17.0.2',
    'cassandrapuerto': 9042
}


# Funcion que dada la configuracion y ubicacion del archivo, carga la base de datos, genera el reporte, y borra la
# base de datos
def ejecutar(file, conn):
    import time

    start = time.time()
    db, prepared = inicializar(conn)
    df_filas = csv.DictReader(open(file, "r", encoding="utf-8"))
    count = 0
    startbloque = time.time()
    try:
        for fila in df_filas:
            procesar_fila(db, fila, prepared)
            count += 1
            if 0 == count%100:
                endbloque = time.time()
                tiempo = endbloque-startbloque
                print(str(count) + " en " + str(tiempo) + " segundos")
                startbloque = time.time()
        generar_reporte(db)
    except Exception as error:
        # print(error.args)
        traceback.print_tb(error.__traceback__)
        print(error)
    finalizar(db)
    end = time.time()
    print("tiempo total en segundos")
    print(end - start)


# Funcion que dado un archivo abierto y una linea, imprime por consola y guarda al final de archivo esa linea
def grabar_linea(archivo, linea):
    print(linea)
    archivo.write(str(linea) + '\n')


def inicializar(conn):
    cassandra_session = Cluster(contact_points=[conn["cassandraurl"]], port=conn["cassandrapuerto"]).connect(keyspace="mi_keyspace")
    # crear db
    #tablas que registran las marcas de un deportista por torneo y especialidad
    #La primera tabla ordena las marcas de forma ascendente
    table_query = """
        CREATE TABLE IF NOT EXISTS marcas_deportista (
        nombre_deportista TEXT,
        nombre_tipo_especialidad TEXT,
        nombre_especialidad TEXT,
        marca INT,
        nombre_torneo TEXT,
        intento INT,
        PRIMARY KEY ((nombre_especialidad,nombre_deportista),nombre_tipo_especialidad,marca)
        );
        """
    #La segunca tabla ordena de forma descendente (ayuda a que la lectura sea más simple, ORDER BY es restrictivo)
    table_query2 = """
        CREATE TABLE IF NOT EXISTS marcas_deportista_desc (
        nombre_deportista TEXT,
        nombre_tipo_especialidad TEXT,
        nombre_especialidad TEXT,
        marca INT,
        nombre_torneo TEXT,
        intento INT,
        PRIMARY KEY ((nombre_especialidad,nombre_deportista),nombre_tipo_especialidad,marca)
        )
        WITH CLUSTERING ORDER BY(nombre_tipo_especialidad ASC,marca DESC);
        """
    cassandra_session.execute(table_query)
    cassandra_session.execute(table_query2)
    #uso prepared statements para acelerar la carga de datos, ayuda con consultas repetidas muchas veces
    prepared = []
    prepared.append(cassandra_session.prepare("""INSERT INTO marcas_deportista(nombre_deportista, nombre_tipo_especialidad,
        nombre_especialidad,marca,nombre_torneo,intento) VALUES(?,?,?,?,?,?);
        """))
    prepared.append(cassandra_session.prepare("""INSERT INTO marcas_deportista_desc(nombre_deportista, nombre_tipo_especialidad,
        nombre_especialidad,marca,nombre_torneo,intento) VALUES(?,?,?,?,?,?);
        """))
    return cassandra_session, prepared

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila, prepared):
    db.execute(prepared[0],
               (fila['nombre_deportista'],fila['nombre_tipo_especialidad'],
                fila['nombre_especialidad'],int(fila['marca']), fila['nombre_torneo'], int(fila['intento'])))
    db.execute(prepared[1],
               (fila['nombre_deportista'], fila['nombre_tipo_especialidad'],
                fila['nombre_especialidad'], int(fila['marca']), fila['nombre_torneo'], int(fila['intento'])))
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w',encoding = 'utf-8')
    #Aprovecho que cada partición (combinación de deportista y especialidad) está ordenada por marca
    #por cada partición tomo la primera fila, y dependiendo del orden de marcas va a ser el mayor o menor
    #si es el mejor o peor depende de si el tipo especialidad es de tiempo o no, por lo que se hacen cuatro consultas
    #si se tuviera que usar ORDER BY, uno estaría forzado a establecer una partición en cada consulta
    #por eso se cargan dos tablas con los diferentes ordenamientos
    #igual en una aplicación real quizás uno desearía no estar escaneando la tabla entera
    consulta_mejor_tiempo = db.execute("""
        SELECT nombre_deportista,nombre_especialidad, marca, nombre_torneo, intento FROM marcas_deportista
        WHERE nombre_tipo_especialidad = 'tiempo' 
        PER PARTITION LIMIT 1
        ALLOW FILTERING
        """)

    consulta_peor_tiempo = db.execute("""
        SELECT nombre_deportista,nombre_especialidad, marca, nombre_torneo, intento FROM marcas_deportista_desc
        WHERE nombre_tipo_especialidad = 'tiempo' 
        PER PARTITION LIMIT 1
        ALLOW FILTERING
        """)

    consulta_mejor_distancia = db.execute("""
        SELECT nombre_deportista,nombre_especialidad, marca, nombre_torneo, intento FROM marcas_deportista_desc
        WHERE nombre_tipo_especialidad IN ('altura','largo','lanzamiento') 
        PER PARTITION LIMIT 1
        ALLOW FILTERING
        """)

    consulta_peor_distancia = db.execute("""
        SELECT nombre_deportista,nombre_especialidad, marca, intento, nombre_torneo FROM marcas_deportista
        WHERE nombre_tipo_especialidad IN ('altura','largo','lanzamiento') 
        PER PARTITION LIMIT 1
        ALLOW FILTERING
        """)

    #Se alterna la mejor y peor marca
    linea = "nombre_deportista,nombre_especialidad,mejor_marca,intento,nombre_torneo"
    grabar_linea(archivo, linea)
    linea = "nombre_deportista,nombre_especialidad,peor_marca,intento,nombre_torneo"
    grabar_linea(archivo, linea)

    for fila_mejor,fila_peor in chain(zip(consulta_mejor_tiempo,consulta_peor_tiempo),
                                      zip(consulta_mejor_distancia,consulta_peor_distancia)):
        linea = ",".join([str(fila_mejor.nombre_deportista), fila_mejor.nombre_especialidad,
                          str(fila_mejor.marca),str(fila_mejor.intento),fila_mejor.nombre_torneo])
        grabar_linea(archivo, linea)
        linea = ",".join([str(fila_peor.nombre_deportista), fila_peor.nombre_especialidad,
                          str(fila_peor.marca), str(fila_peor.intento), fila_peor.nombre_torneo])
        grabar_linea(archivo, linea)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.execute("DROP TABLE marcas_deportista")
    db.execute("DROP TABLE marcas_deportista_desc")
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)