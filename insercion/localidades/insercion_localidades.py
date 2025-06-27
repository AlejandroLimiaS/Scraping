import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
import sys
import time


sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from insercion.utiles.utiles_localidad import Localidad_BD, insertar_localidad
from insercion.utiles.utiles_paises import obtener_o_insertar_pais
from insercion.utiles.utiles_db import crear_pool_bd_async
from insercion.localidades.utiles_localidades import localidades_provincias
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


async def procesar_localidades():
    fallidos = []
    localidades_gallegas: list[Localidad_BD] = []
    galicia = "Galicia"
    pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                id_pais = await obtener_o_insertar_pais(cur, "España")
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            logging.error(f"Error al obtener o insertar el país España: {e}")
            return

    for localidad, provincia in localidades_provincias.items():
        localidades_gallegas.append(
            Localidad_BD(
                nombre=localidad,
                provincia=provincia,
                comunidad_autonoma=galicia,
                id_pais=int(id_pais),
            )
        )

    tareas = [insertar_localidad(pool, localidad) for localidad in localidades_gallegas]
    resultados = await asyncio.gather(*tareas)
    for idx, res in enumerate(resultados):
        if not res:
            fallidos.append(localidades_gallegas[idx])

    pool.close()
    await pool.wait_closed()

    if fallidos:
        logging.info(
            f"Se produjeron {len(fallidos)} fallos en la inserción de localidades gallegas."
        )
    else:
        logging.info("Todos las localidades se insertaron correctamente.")


async def main():
    start = time.time()

    try:
        logging.info(print_cabecera("Iniciando inserción de localidades gallegas"))
        await procesar_localidades()
        logging.info(print_cabecera("Finalizada inserción de localidades_gallegas"))
    except Exception as e:
        logging.error(f"Error general en insertar localidades: {e}")
    end = time.time()
    minutos, segundos = divmod(end - start, 60)
    logging.info(f"Finalizado en {int(minutos)}m {segundos:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
