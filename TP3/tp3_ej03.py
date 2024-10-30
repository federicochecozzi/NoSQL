import csv
from cassandra.cluster import Cluster
import traceback

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
#archivo_entrada = 'full_export_version_corta.csv'
nombre_archivo_resultado_ejercicio = 'tp3_ej03.txt'

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
    #una variable COUNTER cuenta la cantidad de marcas por especialidad, se actualiza con cada nueva fila ("marca")
    #nombre_tipo_especialidad se necesita en el WHERE
    table_query = """
        CREATE TABLE IF NOT EXISTS tipo_especialidad (
        id_tipo_especialidad INT,
        nombre_tipo_especialidad TEXT,
        nombre_especialidad TEXT,
        cantidad_marcas COUNTER,
        PRIMARY KEY (id_tipo_especialidad,nombre_tipo_especialidad,nombre_especialidad)
        );
        """
    cassandra_session.execute(table_query)
    #uso prepared statements para acelerar la carga de datos, ayuda con consultas repetidas muchas veces
    prepared = cassandra_session.prepare("""UPDATE tipo_especialidad SET cantidad_marcas=cantidad_marcas+1
    WHERE id_tipo_especialidad=? AND nombre_tipo_especialidad=? AND nombre_especialidad=?
    """)
    return cassandra_session, prepared

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila, prepared):
    db.execute(prepared,(int(fila['id_tipo_especialidad']), fila['nombre_tipo_especialidad'], fila['nombre_especialidad']))
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    # todas las especialidades con su tipo asociado
    consulta = db.execute("SELECT * FROM tipo_especialidad")
    for fila in consulta:
        linea = ",".join([str(fila.id_tipo_especialidad), fila.nombre_tipo_especialidad, fila.nombre_especialidad])
        grabar_linea(archivo, linea)

    #agrega la cantidad de marcas por id_tipo_especialidad
    consulta = db.execute("SELECT id_tipo_especialidad, nombre_tipo_especialidad, SUM(cantidad_marcas) as marcas FROM tipo_especialidad GROUP BY id_tipo_especialidad")
    for fila in consulta:
        linea = ",".join([fila.nombre_tipo_especialidad, str(fila.marcas)])
        grabar_linea(archivo, linea)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.execute("DROP TABLE tipo_especialidad")
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
