# coding=utf-8
import os
import logging
import asyncio
import time
import sys

from typing import List
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime


sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from insercion.utiles.utiles_db import (
    crear_pool_bd_async,
    obtener_enlaces_partidos,
    obtener_jornada_liga,
)

sys.path.append(str(Path(__file__).resolve().parent.parent))

from jornada.utiles_jornada import get_fecha_y_hora
from utiles.utiles_modelos import PartidoJugado, PartidoPrevia, TargetURL
from utiles.utiles_salida import print_cabecera
from utiles.utiles_scraping import (
    CHUNK_SIZE,
    ScrapedURL,
    chunked_iterable,
    guardar_datos_json,
    scrape_urls,
    URLFailReason,
    URLFail,
    MAX_REINTENTOS,
    SEMAPHORE_VALUE,
)
from utiles.utiles_links import paginas_fallidas, paginas_scrapeadas


USER = os.getenv("MYSQL_USER")
if USER is None:
    raise ValueError("MYSQL_USER environment variable is not set.")
PASSWORD = os.getenv("MYSQL_PASSWORD")
if PASSWORD is None:
    raise ValueError("MYSQL_PASSWORD environment variable is not set.")

nombre_carpeta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
ruta_archivo = f"{nombre_carpeta}/{datetime.now().strftime('%d-%m-%y_%H-%M')}"
ruta_archivo_datos = f"{nombre_carpeta}/"
os.makedirs(nombre_carpeta, exist_ok=True)


logging.basicConfig(
    filename=f"{ruta_archivo}.log", level=logging.INFO, format="%(message)s"
)


async def scrapear_partidos(
    links_partidos: List[str], intento=1
) -> tuple[List[PartidoJugado], int]:
    global paginas_fallidas, paginas_scrapeadas
    target_urls: List[TargetURL] = []
    partidos_a_reintentar: List[str] = []
    datos_partidos: List[PartidoPrevia] = []
    partidos = 0
    if intento > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para la jornada."
        )
        logging.info(f"Partidos fallidos: {[link for link in links_partidos]}")
        return []
    logging.info(
        print_cabecera(
            f"Horarios de la proxima jornada intento {intento}. Partidos restantes: {len(links_partidos)}"
        )
    )

    for link in links_partidos:
        target_urls.append(TargetURL(url=link))

    for index_chunk, target_url_batch in enumerate(
        chunked_iterable(target_urls, CHUNK_SIZE), start=0
    ):
        logging.info(f"\nProcesando batch {index_chunk} de URLs")
        scraped_urls: List[ScrapedURL] = await scrape_urls(target_url_batch)

        if not scraped_urls:
            for target_url in target_url_batch:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intento,
                        url=target_url.url,
                        reason=URLFailReason.EMPTY.value,
                        message="No se pudo obtener el HTML.",
                    )
                )
            partidos_a_reintentar.extend(
                links_partidos[index_chunk * CHUNK_SIZE + i]
                for i in range(len(target_url_batch))
            )
            logging.info(f"Batch {index_chunk} fallido")
            continue

        for index_scraped, scraped_url in enumerate(scraped_urls):
            url = scraped_url.url
            html = scraped_url.content
            paginas_fallidas.extend(scraped_url.paginas_fallidas)
            if not html:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intento,
                        url=url,
                        reason=URLFailReason.NO_HTML.value,
                        message="No se pudo obtener el HTML.",
                    )
                )
                partidos_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue

            soup = BeautifulSoup(html, "html.parser")
            partido = PartidoPrevia(
                cod_partido=url.rsplit("/", 1)[-1],
                horario=get_fecha_y_hora(soup),
            )

            if not partido.cod_partido or not partido.horario:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intento,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener el partido.",
                    )
                )
                partidos_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue
            paginas_scrapeadas.add(url)
            logging.info(f"Scrapeados horario de partido: {partido.cod_partido}")
            datos_partidos.append(partido)
            partidos += 1
    if len(partidos_a_reintentar) > 0 and intento < MAX_REINTENTOS:
        datos_partidos_reintentados, num_partidos_reintentados = (
            await scrapear_partidos(partidos_a_reintentar, intento + 1)
        )
        datos_partidos.extend(datos_partidos_reintentados)
        partidos += num_partidos_reintentados
    return datos_partidos, partidos


async def obtener_links_partidos():
    ligas = {
        "Primera Federación - Grupo 1",
        "Primera Federación - Grupo 2",
        "Segunda Federación - Grupo 1",
        "Segunda Federación - Grupo 2",
        "Segunda Federación - Grupo 3",
        "Segunda Federación - Grupo 4",
        "Segunda Federación - Grupo 5",
    }
    enlaces = []
    try:
        pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")
        for liga in ligas:
            division, grupo = liga.split(" - ")
            id_liga, jornada = await obtener_jornada_liga(
                pool, division.strip(), grupo.strip()
            )
            if not id_liga:
                logging.error("No existen las ligas")
                continue
            enlaces_liga = await obtener_enlaces_partidos(
                pool, int(id_liga), int(jornada + 1)
            )
            enlaces.extend(enlace[0] for enlace in enlaces_liga)
        pool.close()
        await pool.wait_closed()
    except Exception as e:
        logging.error(f"Error en la ejecución: {e}")
    return enlaces


async def procesar_calendarios():
    global paginas_fallidas, paginas_scrapeadas

    links_partidos = await obtener_links_partidos()
    if not links_partidos:
        logging.error("No se han podido obtener los enlaces de los partidos.")
        return
    logging.info(print_cabecera("Scraping de horarios de partidos siguiente jornada"))
    datos_partidos, partidos = await scrapear_partidos(links_partidos)
    if not datos_partidos:
        logging.error("No se han podido obtener los datos de los partidos.")
        return
    logging.info(
        print_cabecera(
            f"Scraping de partidos: horarios de {partidos} partidos distintos"
        )
    )
    logging.info(print_cabecera("Estadísticas de scraping"))
    logging.info(f"\nPáginas únicas scrapeadas: {len(paginas_scrapeadas)}")
    logging.info(f"\nAsyncio semaphore: {SEMAPHORE_VALUE}")
    logging.info(f"\nTamaño del chunk: {CHUNK_SIZE}")
    logging.info(f"\nPáginas fallidas: {len(paginas_fallidas)}")
    for i in range(1, MAX_REINTENTOS + 1):
        logging.info(print_cabecera(f"Reintento {i}"))
        for pagina in paginas_fallidas:
            if pagina.attempt == i:
                logging.info(f"· {pagina.reason} | {pagina.url} | {pagina.message}")
    guardar_datos_json(datos_partidos, f"{ruta_archivo_datos}datos_horarios_jornada")
    guardar_datos_json(datos_partidos, f"{ruta_archivo}datos_horarios_jornada")


async def main():
    start_time = time.time()

    try:
        await procesar_calendarios()
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
