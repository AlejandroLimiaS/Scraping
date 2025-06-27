# coding=utf-8
import os
import logging
import asyncio
import time
import sys

from typing import Dict, List
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parent.parent))
from utiles.utiles_modelos import Campo, Club_Equipo, TargetURL
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
from equipos.utiles_equipos import *
from utiles.utiles_links import *


nombre_carpeta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
ruta_archivo = f"{nombre_carpeta}/{datetime.now().strftime('%d-%m-%y_%H-%M')}"
ruta_archivo_datos = f"{nombre_carpeta}/"
os.makedirs(nombre_carpeta, exist_ok=True)


logging.basicConfig(
    filename=f"{ruta_archivo}.log", level=logging.INFO, format="%(message)s"
)


async def scrapear_equipos(
    links_equipos: List[str], intento=1
) -> tuple[List[Club_Equipo], List[str], int]:
    global paginas_fallidas, paginas_scrapeadas
    datos_equipos: List[Club_Equipo] = []
    target_urls: List[TargetURL] = []
    equipos_a_reintentar: List[str] = []
    links_campos: List[str] = []
    equipos = 0
    if intento > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para los equipos."
        )
        logging.info(f"Equipos fallidos: {[equipo for equipo in links_equipos]}")
        return [], []
    logging.info(
        print_cabecera(
            f"Equipos intento {intento}. Equipos restantes: {len(links_equipos)}"
        )
    )

    for link in links_equipos:
        target_urls.append(TargetURL(url=link.replace("startseite", "datenfakten")))

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
            equipos_a_reintentar.extend(
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
                        attempt=intento,
                        url=url,
                        reason=URLFailReason.NO_HTML.value,
                        message="No se pudo obtener el HTML.",
                    )
                )
                equipos_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue

            soup = BeautifulSoup(html, "html.parser")
            link_estadio = get_enlace_estadio(soup)
            if link_estadio != "-":
                links_campos.append("https://www.transfermarkt.es" + link_estadio)
            nombre = get_nombre(soup)
            primer_equipo_nombre, primer_equipo_link = obtener_primer_equipo(
                soup, nombre
            )
            cod_equipo = (url.split("/")[6],)
            club_equipo: Club_Equipo = Club_Equipo(
                cod_equipo=cod_equipo[0],
                nombre=nombre,
                liga=get_liga(soup),
                localidad=get_localidad(soup),
                primer_equipo=(
                    primer_equipo_nombre if primer_equipo_nombre != "-" else nombre
                ),
                cod_club=(
                    primer_equipo_link.split("/")[-1]
                    if primer_equipo_link != "-"
                    else cod_equipo[0]
                ),
            )

            if (
                not club_equipo.cod_club
                or club_equipo.nombre == "-"
                or club_equipo.liga == "-"
            ):
                logging.info(f"Fallido {url}. A reintentar...")
                paginas_fallidas.append(
                    URLFail(
                        attempt=intento,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener el equipo.",
                    )
                )
                equipos_a_reintentar.append(url)
                continue
            paginas_scrapeadas.add(url)
            logging.info(f"Scrapeado equipo: {club_equipo.nombre}")
            datos_equipos.append(club_equipo)
            equipos += 1
    if len(equipos_a_reintentar) > 0 and intento < MAX_REINTENTOS:
        datos_equipos_reintentados, links_equipos_reintentados, equipos_reintentados = (
            await scrapear_equipos(equipos_a_reintentar, intento + 1)
        )
        datos_equipos.extend(datos_equipos_reintentados)
        links_campos.extend(links_equipos_reintentados)
        equipos += equipos_reintentados
    if intento == 1:
        links_campos = list(Dict.fromkeys(links_campos))
    return datos_equipos, links_campos, equipos


async def scrapear_campos(
    links_campos: List[str], intento=1
) -> tuple[List[Club_Equipo], int]:
    global paginas_fallidas, paginas_scrapeadas
    target_urls: List[TargetURL] = []
    campos_a_reintentar: List[str] = []
    datos_campos: List[Campo] = []
    num_campos = 0
    if intento > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para los campos."
        )
        logging.info(f"Campos fallidos: {[campo for campo in links_campos]}")
        return []
    logging.info(
        print_cabecera(f"Campos {intento}. Campos restantes: {len(links_campos)}")
    )

    for link in links_campos:
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
            campos_a_reintentar.extend(
                links_campos[index_chunk * CHUNK_SIZE + i]
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
                campos_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue

            soup = BeautifulSoup(html, "html.parser")
            direccion, localidad = get_direccion_y_localidad(soup)
            campo: Campo = Campo(
                nombre=get_nombre_estadio(soup),
                cod_equipo=url.split("/verein/")[1],
                superficie=get_superficie_estadio(soup),
                dimensiones=get_dimensiones_estadio(soup),
                aforo=get_aforo_estadio(soup),
                direccion=direccion,
                localidad=localidad,
            )

            if campo.nombre == "-" or campo.cod_equipo == "-":
                logging.info(f"Fallido {url}. A reintentar...")
                paginas_fallidas.append(
                    URLFail(
                        attempt=intento,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener el campo.",
                    )
                )
                campos_a_reintentar.append(url)
                continue
            paginas_scrapeadas.add(url)
            logging.info(f"Scrapeado campo: {campo.nombre}, {campo.cod_equipo}")
            datos_campos.append(campo)
            num_campos += 1
    if len(campos_a_reintentar) > 0 and intento < MAX_REINTENTOS:
        datos_campos_reintentados, num_campos_reintentados = await scrapear_campos(
            campos_a_reintentar, intento + 1
        )
        datos_campos.extend(datos_campos_reintentados)
        num_campos += num_campos_reintentados
    return datos_campos, num_campos


async def procesar_equipos():
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
    datos_equipos, links_campos, equipos = await scrapear_equipos(links_equipos)
    if not datos_equipos:
        logging.error("No se han podido obtener los datos de los equipos.")
        return
    logging.info(print_cabecera(f"Scraping de equipos: {equipos} equipos."))
    logging.info(print_cabecera("Scraping campos"))
    datos_campos, num_campos = await scrapear_campos(links_campos)
    if not datos_campos:
        logging.error("No se han podido obtener los datos de los campos.")
        return
    logging.info(
        print_cabecera(
            f"Scraping de campos: {num_campos} campos de {len(links_campos)} campos sobre {equipos} equipos."
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
    guardar_datos_json(datos_equipos, f"{ruta_archivo}datos_equipos")
    guardar_datos_json(datos_equipos, f"{ruta_archivo_datos}datos_equipos")
    guardar_datos_json(datos_campos, f"{ruta_archivo}datos_campos")
    guardar_datos_json(datos_campos, f"{ruta_archivo_datos}datos_campos")


async def main():

    start_time = time.time()

    try:
        await procesar_equipos()
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
