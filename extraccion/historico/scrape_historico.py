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
from historico.utiles_historico import extraer_fichajes_desde_pre
from utiles.utiles_modelos import (
    HistoricoFichajes,
    TargetURL,
)
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
from utiles.utiles_links import *


nombre_carpeta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
ruta_archivo = f"{nombre_carpeta}/{datetime.now().strftime('%d-%m-%y_%H-%M')}"
ruta_archivo_datos = f"{nombre_carpeta}/"
os.makedirs(nombre_carpeta, exist_ok=True)


logging.basicConfig(
    filename=f"{ruta_archivo}_jugadores.log", level=logging.INFO, format="%(message)s"
)


async def scrapear_historico_jugadores(
    links_jugadores: List[str], intento=1
) -> tuple[List[HistoricoFichajes], int]:
    global paginas_fallidas, paginas_scrapeadas
    target_urls: List[TargetURL] = []
    jugadores_a_reintentar: List[str] = []
    datos_historico_jugadores: List[HistoricoFichajes] = []
    jugadores = 0
    if intento > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para los historicos de los jugadores."
        )
        logging.info(f"Jugadores fallidos: {[link for link in links_jugadores]}")
        return []
    logging.info(
        print_cabecera(
            f"Historico jugadores intento {intento}. Jugadores restantes: {len(links_jugadores)}"
        )
    )

    for link in links_jugadores:
        cod = link.split("/")[-1]
        ruta = f"https://www.transfermarkt.es/ceapi/transferHistory/list/{cod}"
        target_urls.append(TargetURL(url=ruta, json=True))

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
            jugadores_a_reintentar.extend(
                links_jugadores[index_chunk * CHUNK_SIZE + i]
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
                jugadores_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue

            soup = BeautifulSoup(html, "html.parser")
            cod_jugador = url.split("/")[-1]
            historicoFichajes = HistoricoFichajes(
                cod_jugador=cod_jugador,
                fichajes=extraer_fichajes_desde_pre(soup),
            )
            if not historicoFichajes.cod_jugador:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intento,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener el historico del jugador.",
                    )
                )
                jugadores_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue
            paginas_scrapeadas.add(url)
            logging.info(
                f"Scrapeado historico jugador: {historicoFichajes.cod_jugador}"
            )
            datos_historico_jugadores.append(historicoFichajes)
            jugadores += 1
    if len(jugadores_a_reintentar) > 0 and intento < MAX_REINTENTOS:
        datos_historico_jugadores_reintentados, num_jugadores_reintentados = (
            await scrapear_historico_jugadores(jugadores_a_reintentar, intento + 1)
        )
        datos_historico_jugadores.extend(datos_historico_jugadores_reintentados)
        jugadores += num_jugadores_reintentados
    return datos_historico_jugadores, jugadores


async def procesar_jugadores():
    global paginas_fallidas, paginas_scrapeadas

    links_equipos, num_equipos, num_ligas = await scrapear_links_equipos()
    if not links_equipos:
        logging.error("No se han podido obtener los enlaces de los equipos.")
        return
    logging.info(
        print_cabecera(
            f"Scraping de enlaces de equipos: {num_equipos} equipos distintos de {num_ligas} ligas."
        )
    )
    links_jugadores, num_jugadores, num_equipos = await scrapear_links_jugadores(
        links_equipos
    )
    if not links_jugadores:
        logging.error("No se han podido obtener los enlaces de los jugadores.")
        return
    logging.info(
        print_cabecera(
            f"Scraping de enlaces de jugadores: {num_jugadores} jugadores distintos de {num_equipos} equipos."
        )
    )
    logging.info(print_cabecera("Scraping de historico de jugadores"))
    datos_historico_jugadores, jugadores_historico = await scrapear_historico_jugadores(
        links_jugadores
    )
    if not datos_historico_jugadores:
        logging.error("No se han podido obtener los datos históricos de los jugadores.")
        return
    logging.info(
        print_cabecera(
            f"Scraping de jugadores: datos históricos de {jugadores_historico} jugadores distintos"
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
    guardar_datos_json(
        datos_historico_jugadores, f"{ruta_archivo_datos}datos_historico_jugadores"
    )
    guardar_datos_json(
        datos_historico_jugadores, f"{ruta_archivo}datos_historico_jugadores"
    )


async def main():
    start_time = time.time()

    try:
        await procesar_jugadores()
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
