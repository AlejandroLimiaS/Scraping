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

sys.path.append(str(Path(__file__).resolve().parent.parent))
from utiles.utiles_modelos import TargetURL, Liga
from utiles.utiles_salida import print_cabecera
from utiles.utiles_scraping import (
    ScrapedURL,
    chunked_iterable,
    guardar_datos_json,
    scrape_urls,
    URLFailReason,
    URLFail,
    MAX_REINTENTOS,
    SEMAPHORE_VALUE,
)
from utiles_liga import *
from utiles.utiles_links import LinksLigas


paginas_fallidas: List[URLFail] = []
paginas_scrapeadas = set()

CHUNK_SIZE_LIGA = 7


nombre_carpeta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
ruta_archivo = f"{nombre_carpeta}/{datetime.now().strftime('%d-%m-%y_%H-%M')}"
ruta_archivo_datos = f"{nombre_carpeta}/"
os.makedirs(nombre_carpeta, exist_ok=True)


logging.basicConfig(
    filename=f"{ruta_archivo}.log", level=logging.INFO, format="%(message)s"
)


async def scrapear_ligas(ligas_a_scrapear=None, intento=1) -> tuple[List[Liga], int]:
    if ligas_a_scrapear is None:
        ligas_a_scrapear = list(LinksLigas.values())
    global paginas_fallidas, paginas_scrapeadas
    datos_ligas: List[Liga] = []
    target_urls: List[TargetURL] = []
    ligas_a_reintentar: List[str] = []
    num_ligas = 0
    if intento > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para las ligas."
        )
        logging.info(f"Ligas fallidas: {[liga for liga in ligas_a_scrapear]}")
        return []
    logging.info(
        print_cabecera(
            f"Ligas intento {intento}. Ligas restantes: {len(ligas_a_scrapear)}"
        )
    )

    for link in ligas_a_scrapear:
        target_urls.append(TargetURL(url=link))

    for index_chunk, target_url_batch in enumerate(
        chunked_iterable(target_urls, CHUNK_SIZE_LIGA), start=0
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
            ligas_a_reintentar.extend(
                ligas_a_scrapear[index_chunk * CHUNK_SIZE_LIGA + i]
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
                ligas_a_reintentar.append(url)
                logging.info(f"Fallida {url}. A reintentar...")
                continue

            soup = BeautifulSoup(html, "html.parser")
            division, grupo = get_division_grupo(soup)

            liga = Liga(
                cod_grupo=url.split("/")[-1],
                temporada=get_temporada(soup),
                division=division,
                grupo=grupo,
            )

            if (
                not liga.cod_grupo
                or liga.division == "-"
                or liga.temporada == "-"
                or liga.grupo == "-"
            ):
                logging.info(f"Fallida {url}. A reintentar...")
                paginas_fallidas.append(
                    URLFail(
                        attempt=intento,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener la liga.",
                    )
                )
                ligas_a_reintentar.append(url)
                continue
            paginas_scrapeadas.add(url)
            logging.info(
                f"Scrapeada liga: {liga.division}, {liga.grupo}, temporada {liga.temporada}"
            )
            datos_ligas.append(liga)
            num_ligas += 1
    if len(ligas_a_reintentar) > 0 and intento < MAX_REINTENTOS:
        datos_ligas_reintentadas, num_ligas_reintentadas = await scrapear_ligas(
            ligas_a_reintentar, intento + 1
        )
        datos_ligas.extend(datos_ligas_reintentadas)
        num_ligas += num_ligas_reintentadas

    return datos_ligas, num_ligas


async def procesar_ligas():
    global paginas_fallidas, paginas_scrapeadas
    datos_ligas, num_ligas = await scrapear_ligas()
    if not datos_ligas:
        logging.error("No se han podido obtener los datos de las ligas.")
        return
    logging.info(f"Scrapeadas ligas: {num_ligas}")
    logging.info(print_cabecera("Estadísticas de scraping"))
    logging.info(f"\nPáginas únicas scrapeadas: {len(paginas_scrapeadas)}")
    logging.info(f"\nAsyncio semaphore: {SEMAPHORE_VALUE}")
    logging.info(f"\nTamaño del chunk: {CHUNK_SIZE_LIGA}")
    logging.info(f"\nPáginas fallidas: {len(paginas_fallidas)}")
    for i in range(1, MAX_REINTENTOS + 1):
        logging.info(print_cabecera(f"Reintento {i}"))
        for pagina in paginas_fallidas:
            if pagina.attempt == i:
                logging.info(f"· {pagina.reason} | {pagina.url} | {pagina.message}")
    guardar_datos_json(datos_ligas, f"{ruta_archivo_datos}datos_ligas")
    guardar_datos_json(datos_ligas, f"{ruta_archivo}datos_ligas")


async def main():
    start_time = time.time()

    try:
        await procesar_ligas()
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
