import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
import sys
import time


sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

nombre_carpeta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
os.makedirs(nombre_carpeta, exist_ok=True)
timestamp = datetime.now().strftime("%d-%m-%y_%H-%M")
ruta_log = f"{nombre_carpeta}/insercion_horarios_jornada_{timestamp}.log"

logging.basicConfig(
    filename=ruta_log,
    level=logging.INFO,
    format="%(message)s",
)


from extraccion.utiles.utiles_salida import print_cabecera
from insercion.previa.utiles_previa import cargar_previas_json, insertar_horario
from insercion.utiles.utiles_db import crear_pool_bd_async


USER = os.getenv("MYSQL_USER")
if USER is None:
    raise ValueError("MYSQL_USER environment variable is not set.")
PASSWORD = os.getenv("MYSQL_PASSWORD")
if PASSWORD is None:
    raise ValueError("MYSQL_PASSWORD environment variable is not set.")


async def procesar_horarios():
    previas = cargar_previas_json()
    pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")
    tareas = []
    for previa in previas:
        tareas.append(insertar_horario(pool, previa))
    resultados = await asyncio.gather(*tareas, return_exceptions=True)

    fallidos = []
    for idx, res in enumerate(resultados):
        if isinstance(res, Exception) or res is False:
            logging.error(
                f"Error actualizando horario para partido {previas[idx].cod_partido}"
            )
            fallidos.append(previas[idx])

    if fallidos:
        logging.info(f"Se produjeron {len(fallidos)} fallos en la inserción horarios.")
    else:
        logging.info("Todos los horarios se insertaron correctamente.")
    pool.close()
    await pool.wait_closed()


async def main():
    start_time = time.time()
    try:
        logging.info(print_cabecera("Iniciando inserción de horarios"))
        await procesar_horarios()
        logging.info(print_cabecera("Inserción de horarios finalizada"))
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
