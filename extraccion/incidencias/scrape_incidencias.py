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
from jugadores.utiles_jugadores import get_valores_mercado
from incidencias.utiles_incidencias import get_incidencias_y_links
from utiles.utiles_modelos import (
    Incidencia,
    JugadorValorMercado,
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
    filename=f"{ruta_archivo}_incidencias.log", level=logging.INFO, format="%(message)s"
)


async def scrapear_incidencias_links_jugadores(
    links_equipos: List[str], intentos=1
) -> tuple[List[Incidencia], list[str], int, int]:
    global paginas_fallidas, paginas_scrapeadas
    if intentos > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para los jugadores e incidencias."
        )
        logging.info(f"Jugadores fallidos: {[link for link in links_equipos]}")
        return []
    target_urls: List[TargetURL] = []
    paginas_a_reintentar: List[str] = []
    incidencias: List[Incidencia] = []
    links_jugadores: List[str] = []
    num_jugadores = 0
    num_incidencias = 0
    logging.info(
        print_cabecera(
            f"Scrapeando enlaces de jugadores e incidencias intento {intentos}. Equipos restantes: {len(links_equipos)}"
        )
    )

    for link in links_equipos:
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
                links_equipos[index_chunk * CHUNK_SIZE + i]
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

            links_jugadores_equipo, incidencias_equipo = get_incidencias_y_links(soup)
            if not links_jugadores_equipo:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intentos,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se han encontrado jugadores o incidencias.",
                    )
                )
                paginas_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue
            incidencias.extend(incidencias_equipo)
            links_jugadores.extend(links_jugadores_equipo)
            logging.info(
                f"Scrapeadas {len(incidencias_equipo)} incidencias de jugadores y {len(links_jugadores_equipo)} jugadores del equipo {scraped_url.url}."
            )
            num_incidencias += len(incidencias_equipo)
            num_jugadores += len(links_jugadores_equipo)
            paginas_scrapeadas.add(url)
    if len(paginas_a_reintentar) > 0 and intentos < MAX_REINTENTOS:
        (
            incidencias_reintentadas,
            links_jugadores_reintentados,
            num_incidencias_reintentadas,
            num_jugadores_reintentados,
        ) = await scrapear_incidencias_links_jugadores(
            paginas_a_reintentar, intentos + 1
        )
        incidencias.extend(incidencias_reintentadas)
        links_jugadores.extend(links_jugadores_reintentados)
        num_incidencias += num_incidencias_reintentadas
        num_jugadores += num_jugadores_reintentados
    if intentos == 1:
        links_jugadores = list(dict.fromkeys(links_jugadores))
    return incidencias, links_jugadores, num_incidencias, num_jugadores


async def scrapear_valores_mercado_jugadores(
    links_jugadores: List[str], intento=1
) -> tuple[List[JugadorValorMercado], int]:
    global paginas_fallidas, paginas_scrapeadas
    target_urls: List[TargetURL] = []
    jugadores_a_reintentar: List[str] = []
    datos_valores_mercado_jugadores: List[JugadorValorMercado] = []
    jugadores = 0
    if intento > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para los valores de mercado de los jugadores."
        )
        logging.info(f"Jugadores fallidos: {[link for link in links_jugadores]}")
        return []
    logging.info(
        print_cabecera(
            f"Valor mercado jugadores intento {intento}. Jugadores restantes: {len(links_jugadores)}"
        )
    )

    for link in links_jugadores:
        cod = link.split("/")[-1]
        ruta = f"https://www.transfermarkt.es/ceapi/marketValueDevelopment/graph/{cod}"
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

            valor_actual, valor_maximo = get_valores_mercado(soup)
            valores_jugador: JugadorValorMercado = JugadorValorMercado(
                cod_jugador=url.split("/")[-1],
                valor_mercado_actual=valor_actual,
                valor_mercado_maximo=valor_maximo,
            )

            if not valores_jugador.cod_jugador:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intento,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener el gráfico del jugador.",
                    )
                )
                jugadores_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue
            paginas_scrapeadas.add(url)
            logging.info(
                f"Scrapeado grafico valores jugador: {valores_jugador.cod_jugador}"
            )
            datos_valores_mercado_jugadores.append(valores_jugador)
            jugadores += 1
    if len(jugadores_a_reintentar) > 0 and intento < MAX_REINTENTOS:
        datos_valores_mercado_jugadores_reintentados, num_jugadores_reintentados = (
            await scrapear_valores_mercado_jugadores(
                jugadores_a_reintentar, intento + 1
            )
        )
        datos_valores_mercado_jugadores.extend(
            datos_valores_mercado_jugadores_reintentados
        )
        jugadores += num_jugadores_reintentados
    return datos_valores_mercado_jugadores, jugadores


async def procesar_incidencias_jugadores():
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
    incidencias, links_jugadores, num_incidencias, num_jugadores = (
        await scrapear_incidencias_links_jugadores(links_equipos)
    )
    if incidencias is None or not links_jugadores:
        logging.error(
            "No se han podido obtener las incidencias y links de los jugadores."
        )
        return
    logging.info(
        print_cabecera(
            f"Scraping de incidencias y links de jugadores: {num_incidencias} incidencias y {num_jugadores} jugadores distintos."
        )
    )

    logging.info(print_cabecera("Scraping de valores de mercado de jugadores"))
    datos_valores_mercado_jugadores, jugadores_valores = (
        await scrapear_valores_mercado_jugadores(links_jugadores)
    )
    if not datos_valores_mercado_jugadores:
        logging.error(
            "No se han podido obtener los valores de mercado de los jugadores."
        )
        return
    logging.info(
        print_cabecera(
            f"Scraping de jugadores: datos de valores de mercado de {jugadores_valores} jugadores distintos"
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
    guardar_datos_json(incidencias, f"{ruta_archivo_datos}datos_incidencias_jugadores")
    guardar_datos_json(
        datos_valores_mercado_jugadores,
        f"{ruta_archivo_datos}datos_valores_mercado_jugadores",
    )
    guardar_datos_json(incidencias, f"{ruta_archivo}datos_incidencias_jugadores")
    guardar_datos_json(
        datos_valores_mercado_jugadores,
        f"{ruta_archivo}datos_valores_mercado_jugadores",
    )


async def main():
    start_time = time.time()

    try:
        await procesar_incidencias_jugadores()
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
