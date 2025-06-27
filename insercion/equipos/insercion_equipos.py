import asyncio
import logging
import os
from datetime import datetime, time
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from insercion.equipos.utiles_campos import (
    actualizar_campos,
    cargar_campos_json,
    insertar_campo,
)
from extraccion.utiles.utiles_salida import print_cabecera
from insercion.utiles.utiles_db import crear_pool_bd_async
from insercion.equipos.utiles_equipos import (
    cargar_equipos_json,
    insertar_equipo,
)
from insercion.utiles.utiles_localidad import (
    insertar_localidades_y_paises,
    limpiar_localidad,
)

USER = os.getenv("MYSQL_USER")
if USER is None:
    raise ValueError("MYSQL_USER environment variable is not set.")
PASSWORD = os.getenv("MYSQL_PASSWORD")
if PASSWORD is None:
    raise ValueError("MYSQL_PASSWORD environment variable is not set.")

nombre_carpeta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
ruta_archivo = f"{nombre_carpeta}/{datetime.now().strftime('%d-%m-%y_%H-%M')}"
os.makedirs(nombre_carpeta, exist_ok=True)
logging.basicConfig(
    filename=f"{ruta_archivo}.log", level=logging.INFO, format="%(message)s"
)


async def obtener_localidades_y_paises_unicos(cur, equipos, campos):
    localidades = []
    paises = set()

    for e in equipos:
        if e.localidad:
            loc = await limpiar_localidad(cur, e.localidad)
            localidades.append(loc)
            if loc.pais:
                paises.add(loc.pais)

    for c in campos:
        if c.localidad:
            loc = await limpiar_localidad(cur, c.localidad)
            localidades.append(loc)
            if loc.pais:
                paises.add(loc.pais)

    return localidades, paises


async def procesar_equipos_y_campos():
    try:
        equipos_raw = cargar_equipos_json()
        campos_raw = cargar_campos_json()

        if not equipos_raw:
            logging.warning("No hay equipos para insertar.")
            return
        if not campos_raw:
            logging.warning("No hay campos para insertar.")
            return

        pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")
        if not pool:
            logging.error("No se pudo crear el pool de conexión a la base de datos.")
            return
        async with pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    localidades, paises = await obtener_localidades_y_paises_unicos(
                        cur, equipos_raw, campos_raw
                    )
                    localidades_ids, paises_ids = await insertar_localidades_y_paises(
                        cur, localidades, paises
                    )
                await conn.commit()
            except Exception as e:
                await conn.rollback()
                logging.error(f"Error al obtener localidades y países: {e}")
                return

        fallidos = []

        tareas_equipos = [
            insertar_equipo(
                pool,
                equipo,
                localidades_ids=localidades_ids,
                paises_ids=paises_ids,
            )
            for equipo in equipos_raw
        ]

        resultados_equipos = await asyncio.gather(*tareas_equipos)

        for idx, resultado in enumerate(resultados_equipos):
            if not resultado:
                fallidos.append(equipos_raw[idx])

        tareas_campos = [
            insertar_campo(pool, campo, localidades_ids, paises_ids)
            for campo in campos_raw
        ]
        resultados_campos = await asyncio.gather(*tareas_campos)

        id_campo_por_nombre = {}
        for idx, res in enumerate(resultados_campos):
            if not res:
                fallidos.append(campos_raw[idx])
            else:
                id_campo_por_nombre[campos_raw[idx].nombre] = res

        await actualizar_campos(pool, campos_raw, id_campo_por_nombre)

        pool.close()
        await pool.wait_closed()

        if fallidos:
            logging.info(f"Se produjeron {len(fallidos)} fallos en la inserción.")
        else:
            logging.info("Todas las inserciones finalizaron correctamente.")
    except Exception as e:
        logging.error(f"Error inesperado en procesar_equipos_y_campos: {e}")


async def main():
    start = time.time()
    try:
        logging.info(print_cabecera("Iniciando inserción de equipos, clubes y campos"))
        await procesar_equipos_y_campos()
        logging.info(print_cabecera("Terminada inserción de equipos, clubes y campos"))
    except Exception as e:
        logging.error(f"Error inesperado en main: {e}")
    finally:
        end = time.time()
        minutos, segundos = divmod(end - start, 60)
        logging.info(f"Finalizado en {int(minutos)}m {segundos:.2f}s")


if __name__ == "__main__":
    import time

    asyncio.run(main())
