import asyncio
import logging
import os
import time
from pathlib import Path
import sys


sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from extraccion.utiles.utiles_modelos import HistoricoFichajes, Jugador
from insercion.jugadores.utiles_historico import (
    cargar_historico_fichajes_desde_json,
    insertar_historico_fichajes,
)
from insercion.utiles.utiles_localidad import (
    insertar_localidades_y_paises,
    limpiar_localidad_jugador,
)


from extraccion.utiles.utiles_salida import print_cabecera
from insercion.utiles.utiles_db import (
    crear_pool_bd_async,
)
from insercion.jugadores.utiles_jugadores import (
    cargar_jugadores_desde_json,
    insertar_posiciones,
    insertar_representantes,
    preprocesar_posicion,
    procesar_jugador,
)

USER = os.getenv("MYSQL_USER")
if USER is None:
    raise ValueError("MYSQL_USER environment variable is not set.")
PASSWORD = os.getenv("MYSQL_PASSWORD")
if PASSWORD is None:
    raise ValueError("MYSQL_PASSWORD environment variable is not set.")

nombre_carpeta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
ruta_archivo = f"{nombre_carpeta}/{time.strftime('%d-%m-%y_%H-%M')}"
os.makedirs(nombre_carpeta, exist_ok=True)
logging.basicConfig(
    filename=f"{ruta_archivo}.log", level=logging.INFO, format="%(message)s"
)


async def obtener_unicos_j(cur, jugadores):
    localidades = []
    paises_set = set()
    posiciones = []
    agentes = set()
    for j in jugadores:
        if j.lugar_nacimiento:
            localidad = await limpiar_localidad_jugador(cur, j.lugar_nacimiento)
            localidades.append(localidad)
            paises_set.add(localidad.pais)
        if j.nacionalidad:
            for pais in j.nacionalidad:
                paises_set.add(pais)
        if j.posicion:
            posiciones.append(preprocesar_posicion(j.posicion))
        if j.posiciones_secundarias:
            for p in j.posiciones_secundarias:
                posiciones.append(preprocesar_posicion(p))
        if j.agente:
            agentes.add(j.agente)

    return localidades, paises_set, posiciones, agentes


async def procesar_jugadores(
    jugadores: list[Jugador] = None, historicos: list[HistoricoFichajes] = None
):
    if jugadores is None:
        jugadores = await cargar_jugadores_desde_json()
    if not jugadores:
        logging.warning("No hay jugadores para insertar.")
        return

    pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")

    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                localidades, paises, posiciones, representantes = (
                    await obtener_unicos_j(cur, jugadores)
                )
                localidades_ids, paises_ids = await insertar_localidades_y_paises(
                    cur, localidades, paises
                )
                posiciones_ids = await insertar_posiciones(cur, posiciones)
                representantes_ids = await insertar_representantes(cur, representantes)
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            return

    fallidos = []
    tareas = [
        procesar_jugador(
            pool, j, localidades_ids, paises_ids, posiciones_ids, representantes_ids
        )
        for j in jugadores
    ]
    resultados = await asyncio.gather(*tareas)

    for idx, resultado in enumerate(resultados):
        if not resultado:
            fallidos.append(jugadores[idx])
    if historicos is None:
        historicos = await cargar_historico_fichajes_desde_json()
    if not historicos:
        logging.warning("No hay historicos para insertar")
        return

    tareas = [insertar_historico_fichajes(pool, h) for h in historicos]
    resultados = await asyncio.gather(*tareas)
    for idx, resultado in enumerate(resultados):
        if not resultado:
            fallidos.append(historicos[idx])

    if fallidos:
        logging.info(
            f"Se produjeron {len(fallidos)} fallos en la inserción de jugadores y sus historicos."
        )
    else:
        logging.info("Todos los jugadores se insertaron correctamente.")

    pool.close()
    await pool.wait_closed()


async def main():
    start = time.time()

    try:
        logging.info(print_cabecera("Iniciando inserción de jugadores"))
        await procesar_jugadores()
        logging.info(print_cabecera("Finalizada inserción de jugadores"))
    except Exception as e:
        logging.error(f"Error general en procesar jugadores: {e}")
    finally:
        end = time.time()
        minutos, segundos = divmod(end - start, 60)
        logging.info(f"Finalizado en {int(minutos)}m {segundos:.2f}s")


if __name__ == "__main__":
    import time

    asyncio.run(main())
