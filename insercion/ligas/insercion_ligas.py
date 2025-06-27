import asyncio
import time
import logging
import os
from pathlib import Path
import sys
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from extraccion.utiles.utiles_salida import print_cabecera
from insercion.ligas.utiles_ligas import (
    Liga_BD,
    cargar_ligas,
    insertar_liga,
    procesar_liga_insertar,
)
from insercion.utiles.utiles_db import crear_pool_bd_async
from extraccion.utiles.utiles_modelos import Liga

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


async def insertar_ligas(pool, ligas: list[Liga]):
    if not ligas:
        logging.info("No hay ligas para insertar.")
        return
    ligas_bd: list[Liga_BD] = [procesar_liga_insertar(liga) for liga in ligas]
    logging.info(f"Se van a insertar {len(ligas_bd)} ligas en la base de datos.")

    fallidos = []
    try:
        tareas = [insertar_liga(pool, liga_bd) for liga_bd in ligas_bd]
        resultados = await asyncio.gather(*tareas)
        for liga_bd, exito in zip(ligas_bd, resultados):
            if not exito:
                fallidos.append(liga_bd)
    finally:
        logging.info(print_cabecera("Finalizando inserción de ligas."))
        if fallidos:
            logging.info(f"Se han producido {len(fallidos)} fallos al insertar ligas.")
        else:
            logging.info("Todas las ligas se insertaron correctamente.")


async def procesar_ligas():
    try:
        ligas = cargar_ligas()
        if not ligas:
            logging.error("No hay ligas para insertar.")
            return
        pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")
        await insertar_ligas(pool, ligas)
    except Exception as e:
        logging.error(f"Error en la ejecución: {e}")
    finally:
        pool.close()
        await pool.wait_closed()
        logging.info("Pool de conexión cerrado.")


async def main():
    start_time = time.time()
    try:
        logging.info(print_cabecera("Iniciando inserción de ligas"))
        await procesar_ligas()
        logging.info(print_cabecera("Inserción de ligas finalizada"))
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
