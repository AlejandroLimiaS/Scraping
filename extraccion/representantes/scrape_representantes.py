# coding=utf-8
import json
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
from utiles.utiles_modelos import Agente, TargetURL
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
from representantes.utiles_representantes import *

paginas_fallidas: List[URLFail] = []
paginas_scrapeadas = set()

nombre_carpeta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
ruta_archivo = f"{nombre_carpeta}/{datetime.now().strftime('%d-%m-%y_%H-%M')}"
ruta_archivo_datos = f"{nombre_carpeta}/"
os.makedirs(nombre_carpeta, exist_ok=True)

logging.basicConfig(
    filename=f"{ruta_archivo}.log", level=logging.INFO, format="%(message)s"
)


def get_agentes_links() -> List[str]:
    try:
        with open(
            "extraccion/jugadores/resultados/links_representantes.json", "r"
        ) as file:
            data = json.load(file)
            links = [link for link in data if "transfermarkt" in link]
            logging.info(f"Se han encontrado {len(links)} enlaces de agentes.")
            return links
    except FileNotFoundError:
        logging.error("El archivo links_agentes.json no se encuentra.")
        return []


async def scrapear_agentes(
    links_agentes: List[str], intento=1
) -> tuple[List[Agente], int]:
    global paginas_fallidas, paginas_scrapeadas
    datos_agentes: List[Agente] = []
    target_urls: List[TargetURL] = []
    agentes_a_reintentar: List[str] = []
    num_agentes = 0
    if intento > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para los agentes."
        )
        logging.info(f"Agentes falliddos: {[link for link in links_agentes]}")
        return [], []
    logging.info(
        print_cabecera(
            f"Agentes intento {intento}. Agentes restantes: {len(links_agentes)}"
        )
    )

    for link in links_agentes:
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
            agentes_a_reintentar.extend(
                links_agentes[index_chunk * CHUNK_SIZE + i]
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
                agentes_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue

            soup = BeautifulSoup(html, "html.parser")
            calle, codigo_postal, localidad, pais = get_direccion_agencia(soup)
            agente: Agente = Agente(
                nombre=get_nombre_agencia(soup),
                telefono=get_telefono_agencia(soup),
                email=get_email_agencia(soup),
                web=get_pagina_web_agencia(soup),
                direccion=calle + " " + codigo_postal + " " + localidad + " " + pais,
            )

            if not agente.nombre:
                logging.info(f"Fallido {url}. A reintentar...")
                paginas_fallidas.append(
                    URLFail(
                        attempt=intento,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener el agente.",
                    )
                )
                agentes_a_reintentar.append(url)
                continue
            paginas_scrapeadas.add(url)
            logging.info(f"Scrapeado agente: {agente.nombre}")
            datos_agentes.append(agente)
            num_agentes += 1
    if len(agentes_a_reintentar) > 0 and intento < MAX_REINTENTOS:
        datos_agentes_reintentados, num_agentes_reintentados = await scrapear_agentes(
            agentes_a_reintentar, intento + 1
        )
        datos_agentes.extend(datos_agentes_reintentados)
        num_agentes += num_agentes_reintentados
    return datos_agentes, num_agentes


async def procesar_agentes():
    global paginas_fallidas, paginas_scrapeadas
    links_agentes = get_agentes_links()
    if not links_agentes:
        logging.error("No se han podido obtener los enlaces de los agentes.")
        return
    logging.info(print_cabecera("Scraping de agentes"))
    datos_agentes, num_agentes = await scrapear_agentes(links_agentes)
    if not datos_agentes:
        logging.error("No se han podido obtener los datos de los agentes.")
        return
    logging.info(print_cabecera(f"Scrapeados {num_agentes} agentes."))
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
    guardar_datos_json(datos_agentes, f"{ruta_archivo_datos}datos_representantes")
    guardar_datos_json(datos_agentes, f"{ruta_archivo}datos_representantes")


async def main():
    start_time = time.time()

    try:
        await procesar_agentes()
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
