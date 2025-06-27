import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
import sys
import time

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from insercion.utiles.utiles_db import crear_pool_bd_async
from insercion.representantes.utiles_representantes import (
    cargar_representantes_json,
    insertar_representante,
)
from extraccion.utiles.utiles_salida import print_cabecera

USER = os.getenv("MYSQL_USER")
if USER is None:
    raise ValueError("MYSQL_USER environment variable is not set.")
PASSWORD = os.getenv("MYSQL_PASSWORD")
if PASSWORD is None:
    raise ValueError("MYSQL_PASSWORD environment variable is not set.")

nombre_carpeta = Path(__file__).parent / "resultados"
nombre_carpeta.mkdir(exist_ok=True)
ruta_archivo = nombre_carpeta / f"{datetime.now().strftime('%d-%m-%y_%H-%M')}.log"

logging.basicConfig(filename=ruta_archivo, level=logging.INFO, format="%(message)s")


async def procesar_representantes(agentes=None):
    if agentes is None:
        agentes = cargar_representantes_json()
    if not agentes:
        logging.warning("No hay representantes para insertar.")
        return

    pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")

    fallidos = []
    tareas = [insertar_representante(pool, agente) for agente in agentes]
    resultados = await asyncio.gather(*tareas)
    for idx, res in enumerate(resultados):
        if not res:
            fallidos.append(agentes[idx])

    pool.close()
    await pool.wait_closed()

    if fallidos:
        logging.info(
            f"Se produjeron {len(fallidos)} fallos en la inserción de representantes."
        )
    else:
        logging.info("Todos los representantes se insertaron correctamente.")


async def main():
    start = time.time()

    try:
        logging.info(print_cabecera("Iniciando inserción de representantes"))
        await procesar_representantes()
        logging.info(print_cabecera("Finalizada inserción de representantes"))
    except Exception as e:
        logging.error(f"Error general en procesar jugadores: {e}")
    finally:
        end = time.time()
        minutos, segundos = divmod(end - start, 60)
        logging.info(f"Finalizado en {int(minutos)}m {segundos:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
