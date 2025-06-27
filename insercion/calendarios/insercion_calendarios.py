import asyncio
from datetime import datetime
import time
import logging
import os
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from extraccion.utiles.utiles_salida import print_cabecera
from extraccion.utiles.utiles_modelos import PartidoCalendario
from insercion.utiles.utiles_db import crear_pool_bd_async
from insercion.calendarios.utiles_calendarios import (
    cargar_partidos_calendario,
    insertar_partido,
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


async def insertar_partidos(pool, partidos: list[PartidoCalendario]):
    if not partidos:
        logging.info("No hay partidos para insertar.")
        return
    fallidos = []
    try:
        tareas = [insertar_partido(pool, p_bd) for p_bd in partidos]
        resultados = await asyncio.gather(*tareas)
        for p_bd, exito in zip(partidos, resultados):
            if not exito:
                fallidos.append(p_bd)
    finally:
        logging.info("Finalizando inserción de partidos.")
        if fallidos:
            logging.info(f"{len(fallidos)} partidos fallidos.")
        else:
            logging.info("Todos los partidos se insertaron correctamente.")


async def procesar_partidos():
    try:
        partidos = cargar_partidos_calendario()
        if not partidos:
            logging.error("No hay partidos para insertar.")
            return
        pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")
        await insertar_partidos(pool, partidos)
    except Exception as e:
        logging.error(f"Error en la ejecución: {e}")
    finally:
        pool.close()
        await pool.wait_closed()
        logging.info("Pool de conexión cerrado.")


async def main():
    start_time = time.time()
    try:
        logging.info(print_cabecera("Iniciando inserción de partidos"))
        await procesar_partidos()
        logging.info(print_cabecera("Inserción de partidos finalizada"))
    except Exception as e:
        logging.error(f"Error en la ejecución: {e}")
    finally:
        end_time = time.time()
        minutes, seconds = divmod(end_time - start_time, 60)
        logging.info(
            f"\nTiempo total de ejecución: {int(minutes)} minutos y {seconds:.2f} segundos"
        )


if __name__ == "__main__":
    asyncio.run(main())
