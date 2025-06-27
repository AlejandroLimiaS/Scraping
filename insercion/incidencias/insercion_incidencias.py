import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
import sys

import time


sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from insercion.incidencias.utiles_valores_mercado import (
    actualizar_valores_mercado,
    cargar_valores_mercado_json,
)


from extraccion.utiles.utiles_salida import print_cabecera
from insercion.incidencias.utiles_incidencias import (
    borrar_incidencias_usuario_null,
    cargar_incidencias_desde_json,
    insertar_incidencia,
)
from insercion.utiles.utiles_db import crear_pool_bd_async


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


async def procesar_incidencias():
    incidencias = cargar_incidencias_desde_json()
    if not incidencias:
        logging.warning("No hay incidencias para insertar.")
        return
    jugadores_valor = cargar_valores_mercado_json()
    if not jugadores_valor:
        logging.warning("No hay valores de mercado para insertar.")
        return
    pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")
    if not pool:
        logging.error("No se pudo crear el pool de conexión a la base de datos.")
        return

    try:
        await borrar_incidencias_usuario_null(pool)
        logging.info("Incidencias con id_usuario NULL borradas correctamente.")
    except Exception as e:
        logging.error(f"Error borrando incidencias con id_usuario NULL: {e}")

    fallidos = []
    tareas = [insertar_incidencia(pool, inc) for inc in incidencias]
    resultados = await asyncio.gather(*tareas)

    for idx, resultado in enumerate(resultados):
        if not resultado:
            fallidos.append(incidencias[idx])

    tareas = [actualizar_valores_mercado(pool, jvm) for jvm in jugadores_valor]
    resultados = await asyncio.gather(*tareas)

    for idx, resultado in enumerate(resultados):
        if not resultado:
            fallidos.append(jugadores_valor[idx])

    pool.close()
    await pool.wait_closed()

    if fallidos:
        logging.info(
            f"Se produjeron {len(fallidos)} fallos en la inserción de incidencias y valores de mercado."
        )
    else:
        logging.info(
            "Todas las incidencias y valores de mercado se insertaron correctamente."
        )


async def main():
    start = time.time()
    try:
        logging.info(
            print_cabecera(
                "Iniciando inserción de incidencias y actualización de valores de mercado."
            )
        )
        await procesar_incidencias()
        logging.info(
            print_cabecera(
                "Finalizada inserción de incidencias y actualización de valores de mercado."
            )
        )
    except Exception as e:
        logging.error(f"Error en la ejecución: {e}")
    finally:
        end = time.time()
        minutos, segundos = divmod(end - start, 60)
        logging.info(f"Finalizado en {int(minutos)}m {segundos:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
