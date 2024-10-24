import csv
from cassandra.cluster import Cluster
import traceback

# Ubicacion del archivo CSV con el contenido provisto por la catedra
#archivo_entrada = 'full_export.csv'
archivo_entrada = 'full_export_version_corta.csv'
nombre_archivo_resultado_ejercicio = 'tp3_ej04_prueba.txt'

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
    table_query = """
        CREATE TABLE IF NOT EXISTS marcas_deportista (
        nombre_deportista TEXT,
        nombre_tipo_especialidad TEXT,
        nombre_especialidad TEXT,
        marca_peor INT,
        nombre_torneo_peor TEXT,
        intento_peor INT,
        marca_mejor INT,
        nombre_torneo_mejor TEXT,
        intento_mejor INT,
        PRIMARY KEY (nombre_deportista,nombre_especialidad)
        );
        """
    cassandra_session.execute(table_query)
    prepared = []
    prepared.append(cassandra_session.prepare("""INSERT INTO marcas_deportista(nombre_deportista, nombre_tipo_especialidad,
    nombre_especialidad,marca_peor,nombre_torneo_peor,intento_peor,marca_mejor,nombre_torneo_mejor,intento_mejor) 
    VALUES(?,?,?,?,?,?,?,?,?) IF NOT EXISTS;
        """))
    prepared.append(cassandra_session.prepare("""UPDATE marcas_deportista 
    SET nombre_tipo_especialidad=?, marca_peor=?, nombre_torneo_peor = ?, intento_peor = ?
    WHERE nombre_deportista=? AND nombre_especialidad=? 
    IF nombre_tipo_especialidad = 'tiempo' AND marca_peor < ?
    """))
    prepared.append(cassandra_session.prepare("""UPDATE marcas_deportista 
        SET nombre_tipo_especialidad=?, marca_peor=?, nombre_torneo_peor = ?, intento_peor = ?
        WHERE nombre_deportista=? AND nombre_especialidad=? 
        IF nombre_tipo_especialidad != 'tiempo' AND marca_peor > ?
        """))
    prepared.append(cassandra_session.prepare("""UPDATE marcas_deportista 
        SET nombre_tipo_especialidad=?, marca_mejor=?, nombre_torneo_mejor = ?, intento_mejor = ?
        WHERE nombre_deportista=? AND nombre_especialidad=? 
        IF nombre_tipo_especialidad = 'tiempo' AND marca_mejor > ?
        """))
    prepared.append(cassandra_session.prepare("""UPDATE marcas_deportista 
        SET nombre_tipo_especialidad=?, marca_mejor=?, nombre_torneo_mejor = ?, intento_mejor = ?
        WHERE nombre_deportista=? AND nombre_especialidad=? 
        IF nombre_tipo_especialidad != 'tiempo' AND marca_mejor < ?
        """))
    return cassandra_session, prepared

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila, prepared):
    db.execute(prepared[0],
               (fila['nombre_deportista'],fila['nombre_tipo_especialidad'], fila['nombre_especialidad'],
                int(fila['marca']), fila['nombre_torneo'], int(fila['intento']),
                int(fila['marca']), fila['nombre_torneo'], int(fila['intento'])))
    db.execute(prepared[1],
               (fila['nombre_tipo_especialidad'], int(fila['marca']),  fila['nombre_torneo'], int(fila['intento']),
                fila['nombre_deportista'], fila['nombre_especialidad'],
                int(fila['marca'])))
    db.execute(prepared[2],
               (fila['nombre_tipo_especialidad'], int(fila['marca']), fila['nombre_torneo'], int(fila['intento']),
                fila['nombre_deportista'], fila['nombre_especialidad'],
                int(fila['marca'])))
    db.execute(prepared[3],
               (fila['nombre_tipo_especialidad'], int(fila['marca']), fila['nombre_torneo'], int(fila['intento']),
                fila['nombre_deportista'], fila['nombre_especialidad'],
                int(fila['marca'])))
    db.execute(prepared[4],
               (fila['nombre_tipo_especialidad'], int(fila['marca']), fila['nombre_torneo'], int(fila['intento']),
                fila['nombre_deportista'], fila['nombre_especialidad'],
                int(fila['marca'])))
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    # luego para cada linea generada como reporte:
    #consulta = db.execute("SELECT nombre_deportista, nombre_especialidad, nombre_torneo, intento, max(marca) as marca FROM marcas_deportista GROUP BY nombre_deportista, nombre_especialidad")
    consulta = db.execute(
        "SELECT * FROM marcas_deportista")

    for fila in consulta:
        linea = ",".join([str(fila.nombre_deportista), fila.nombre_especialidad,
                          fila.nombre_torneo_peor, str(fila.marca_peor),str(fila.intento_peor),
                          fila.nombre_torneo_mejor, str(fila.marca_mejor),str(fila.intento_mejor)])
        grabar_linea(archivo, linea)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.execute("DROP TABLE marcas_deportista")
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
