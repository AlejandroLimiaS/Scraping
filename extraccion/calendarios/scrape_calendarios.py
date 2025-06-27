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
from calendarios.utiles_calendarios import *
from utiles.utiles_modelos import PartidoCalendario, TargetURL
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
    filename=f"{ruta_archivo}.log", level=logging.INFO, format="%(message)s"
)


async def scrapear_links_calendarios(
    ligas_a_scrapear: List[str] = None, intentos=1
) -> tuple[List[str], int]:
    if ligas_a_scrapear is None:
        ligas_a_scrapear = list(LinksLigas.values())

    global paginas_fallidas, paginas_scrapeadas
    if intentos > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para la liga."
        )
        logging.info(f"Ligas fallidas: {[liga for liga in ligas_a_scrapear]}")
        return []
    TargetURLs: List[TargetURL] = []
    links_calendarios: List[str] = []
    calendarios_a_reintentar: List[str] = []
    num_ligas = 0
    logging.info(
        print_cabecera(
            f"Scrapeando enlaces de calendarios {intentos}. Ligas restantes: {len(ligas_a_scrapear)}"
        )
    )

    for liga_url in ligas_a_scrapear:
        TargetURLs.append(TargetURL(url=liga_url))

    for index_chunk, target_url_batch in enumerate(
        chunked_iterable(TargetURLs, CHUNK_SIZE), start=0
    ):
        logging.info(f"\nProcesando batch {index_chunk} de URLs")
        scrapedURLs: List[ScrapedURL] = await scrape_urls(target_url_batch)
        if not scrapedURLs:
            for target_url in target_url_batch:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intentos,
                        url=target_url.url,
                        reason=URLFailReason.EMPTY.value,
                        message="No se pudo obtener el HTML.",
                    )
                )
            calendarios_a_reintentar.extend(
                ligas_a_scrapear[index_chunk * CHUNK_SIZE + i]
                for i in range(len(target_url_batch))
            )
            continue
        for index_scraped, scrapedURL in enumerate(scrapedURLs):
            url = scrapedURL.url
            html = scrapedURL.content
            paginas_fallidas.extend(scrapedURL.paginas_fallidas)
            if not html:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intentos,
                        url=url,
                        reason=URLFailReason.NO_HTML.value,
                        message="No se pudo obtener el HTML.",
                    )
                )
                calendarios_a_reintentar.append(url)
                logging.info(f"Fallida {url}. A reintentar...")
                continue

            soup = BeautifulSoup(html, "html.parser")
            enlace_calendario = soup.find("a", string="Calendario")
            if not enlace_calendario:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intentos,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener el enlace al calendario.",
                    )
                )
                calendarios_a_reintentar.append(url)
                logging.info(f"Fallida {url}. A reintentar...")
                continue

            href = enlace_calendario["href"]
            if "/gesamtspielplan/" in href:
                links_calendarios.append(
                    "https://www.transfermarkt.es" + href.rsplit("/", 2)[0]
                )
            logging.info(
                f"Scrapeado enlace de calendario de la liga {next((liga for liga,url in LinksLigas.items() if url==scrapedURL.url),scrapedURL.url)}."
            )
            num_ligas += 1
            paginas_scrapeadas.add(url)
    if len(calendarios_a_reintentar) > 0 and intentos < MAX_REINTENTOS:
        links_calendarios_reintentados, num_ligas_reintentadas = (
            await scrapear_links_calendarios(calendarios_a_reintentar, intentos + 1)
        )
        links_calendarios.extend(links_calendarios_reintentados)
        num_ligas += num_ligas_reintentadas
    if intentos == 1:
        links_calendarios = list(dict.fromkeys(links_calendarios))
    return links_calendarios, num_ligas


async def scrapear_links_partidos(
    links_calendarios: list[str], intentos=1
) -> tuple[List[str], int, int]:

    global paginas_fallidas, paginas_scrapeadas
    if intentos > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para los partidos."
        )
        logging.info(f"Partidos fallidos: {[link for link in links_calendarios]}")
        return []
    target_urls: List[TargetURL] = []
    paginas_a_reintentar: list[str] = {}
    links_partidos: List[str] = []
    num_partidos = 0
    num_calendarios = 0
    temporada = ""
    logging.info(
        print_cabecera(
            f"Scrapeando enlaces de partidos intento {intentos}. Calendarios restantes: {len(links_calendarios)}"
        )
    )

    for link in links_calendarios:
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
                        attempt=intentos,
                        url=target_url.url,
                        reason=URLFailReason.EMPTY.value,
                        message="No se pudo obtener el HTML.",
                    )
                )
            paginas_a_reintentar.extend(
                links_calendarios[index_chunk * CHUNK_SIZE + i]
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
                        attempt=intentos,
                        url=url,
                        reason=URLFailReason.NO_HTML.value,
                        message="No se pudo obtener el HTML.",
                    )
                )
                paginas_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue

            soup = BeautifulSoup(html, "html.parser")

            partidos = get_enlaces_partidos(soup)
            if num_calendarios == 0 and intentos == 1:
                temporada = soup.find("a", class_="chzn-single").get_text(strip=True)
                temporada = (
                    f"20{temporada.split('/')[0]}/20{temporada.split('/')[1]}"
                    if temporada
                    else None
                )
            if not partidos:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intentos,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener el HTML.",
                    )
                )
                paginas_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue

            for partido in partidos:
                if "/spielbericht/" in partido:
                    links_partidos.append("https://www.transfermarkt.es" + partido)

            logging.info(
                f"Scrapeados {len(links_partidos)-num_partidos} enlaces de partidos del calendario {scraped_url.url}."
            )
            num_partidos = len(links_partidos)
            num_calendarios += 1
            paginas_scrapeadas.add(url)
    if len(paginas_a_reintentar) > 0 and intentos < MAX_REINTENTOS:
        (
            links_partidos_reintentados,
            num_partidos_reintentados,
            num_calendarios_reintentados,
            _,
        ) = await scrapear_links_partidos(paginas_a_reintentar, intentos + 1)
        links_partidos.extend(links_partidos_reintentados)
        num_partidos += num_partidos_reintentados
        num_calendarios += num_calendarios_reintentados
    if intentos == 1:
        links_partidos = list(dict.fromkeys(links_partidos))
    return links_partidos, num_partidos, num_calendarios, temporada


async def scrapear_partidos(
    links_partidos: List[str], temporada, intento=1
) -> tuple[List[PartidoCalendario], int]:
    global paginas_fallidas, paginas_scrapeadas
    target_urls: List[TargetURL] = []
    partidos_a_reintentar: List[str] = []
    datos_partidos: List[PartidoCalendario] = []
    partidos = 0
    if intento > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para los partidos del calendario."
        )
        logging.info(f"Partidos fallidos: {[link for link in links_partidos]}")
        return []
    logging.info(
        print_cabecera(
            f"Calendarios intento {intento}. Partidos restantes: {len(links_partidos)}"
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
            partido = PartidoCalendario(
                cod_local=get_cod_local(soup),
                cod_visitante=get_cod_visitante(soup),
                campo=get_cod_estadio_club(soup),
                jornada=get_jornada(soup),
                enlace=scraped_url.url,
                liga=get_nombre_liga(soup),
                cod_partido=url.rsplit("/", 1)[-1],
                temporada=temporada,
            )

            if (
                not partido.cod_partido
                or not partido.cod_local
                or not partido.cod_visitante
            ):
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
            logging.info(
                f"Scrapeado partido del calendario: {partido.cod_partido}, {partido.liga} jornada {partido.jornada}"
            )
            datos_partidos.append(partido)
            partidos += 1
    if len(partidos_a_reintentar) > 0 and intento < MAX_REINTENTOS:
        datos_partidos_reintentados, num_partidos_reintentados = (
            await scrapear_partidos(partidos_a_reintentar, temporada, intento + 1)
        )
        datos_partidos.extend(datos_partidos_reintentados)
        partidos += num_partidos_reintentados
    return datos_partidos, partidos


async def procesar_calendarios():
    global paginas_fallidas, paginas_scrapeadas

    links_calendarios, num_ligas = await scrapear_links_calendarios()
    if not links_calendarios:
        logging.error("No se han podido obtener los enlaces de los calendarios.")
        return
    links_calendarios_dict = {link: 0 for link in links_calendarios}
    logging.info(
        print_cabecera(
            f"Scraping de enlaces de calendarios:  calendarios obtenidos de {num_ligas} ligas."
        )
    )
    links_partidos, num_partidos, num_calendarios, temporada = (
        await scrapear_links_partidos(links_calendarios_dict)
    )
    if not links_partidos:
        logging.error("No se han podido obtener los enlaces de los partidos.")
        return
    logging.info(
        print_cabecera(
            f"Scraping de enlaces de partidos: {num_partidos} partidos distintos de {num_calendarios} calendarios de {num_ligas} ligas."
        )
    )
    logging.info(print_cabecera("Scraping de partidos"))
    datos_partidos, partidos = await scrapear_partidos(links_partidos, temporada)
    if not datos_partidos:
        logging.error("No se han podido obtener los datos de los partidos.")
        return
    logging.info(
        print_cabecera(f"Scraping de partidos: datos de {partidos} partidos distintos")
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
    guardar_datos_json(datos_partidos, f"{ruta_archivo_datos}datos_partidos_calendario")
    guardar_datos_json(datos_partidos, f"{ruta_archivo}datos_partidos_calendario")


async def main():
    global db_pool
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
