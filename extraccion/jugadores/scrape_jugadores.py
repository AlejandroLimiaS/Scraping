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
from equipos.utiles_equipos import get_liga
from utiles.utiles_modelos import (
    Jugador,
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
from jugadores.utiles_jugadores import *
from utiles.utiles_links import *


nombre_carpeta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
ruta_archivo = f"{nombre_carpeta}/{datetime.now().strftime('%d-%m-%y_%H-%M')}"
ruta_archivo_datos = f"{nombre_carpeta}/"
os.makedirs(nombre_carpeta, exist_ok=True)


logging.basicConfig(
    filename=f"{ruta_archivo}_jugadores.log", level=logging.INFO, format="%(message)s"
)


async def scrapear_links_jugadores_t(
    links_equipos: List[str], intentos=1
) -> tuple[list[tuple[str, str, str]], int, int]:
    global paginas_fallidas, paginas_scrapeadas
    if intentos > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para los jugadores."
        )
        logging.info(f"Jugadores fallidos: {[link for link in links_equipos]}")
        return []
    target_urls: List[TargetURL] = []
    paginas_a_reintentar: List[str] = []
    links_jugadores: List[tuple[str, str, str]] = []
    num_jugadores = 0
    num_equipos = 0
    logging.info(
        print_cabecera(
            f"Scrapeando enlaces de jugadores intento {intentos}. Equipos restantes: {len(links_equipos)}"
        )
    )

    for link in links_equipos:
        target_urls.append(TargetURL(url=link.replace("startseite", "kader")))

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
                links_equipos[index_chunk * CHUNK_SIZE + i] for i in range(CHUNK_SIZE)
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
            jugadores = soup.select("td.hauptlink a")
            if not jugadores:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intentos,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener los enlaces de los jugadores.",
                    )
                )
                paginas_a_reintentar.append(url)
                logging.info(f"Fallido {url}. A reintentar...")
                continue

            for jugador in jugadores:
                href = jugador["href"]
                if "/profil/spieler/" in href:
                    links_jugadores.append(
                        (
                            "https://www.transfermarkt.es" + href,
                            url.split("/verein/")[1].split("/")[0],
                            get_liga(soup),
                        )
                    )

            logging.info(
                f"Scrapeados {len(links_jugadores)-num_jugadores} enlaces de jugadores del equipo {scraped_url.url}."
            )
            num_jugadores = len(links_jugadores)
            num_equipos += 1
            paginas_scrapeadas.add(url)
    if len(paginas_a_reintentar) > 0 and intentos < MAX_REINTENTOS:
        (
            links_jugadores_reintentados,
            num_jugadores_reintentados,
            num_equipos_reintentados,
        ) = await scrapear_links_jugadores(paginas_a_reintentar, intentos + 1)
        links_jugadores.extend(links_jugadores_reintentados)
        num_jugadores += num_jugadores_reintentados
        num_equipos += num_equipos_reintentados
    return links_jugadores, num_jugadores, num_equipos


async def scrapear_jugadores(
    links_jugadores: List[tuple[str, str, str]], intento=1
) -> tuple[List[Jugador], List[str], int]:
    global paginas_fallidas, paginas_scrapeadas
    target_urls: List[TargetURL] = []
    jugadores_a_reintentar: List[tuple[str, str, str]] = []
    links_agentes: List[str] = []
    datos_jugadores: List[Jugador] = []
    jugadores = 0
    if intento > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para los jugadores."
        )
        logging.info(f"Jugadores fallidos: {[link for link in links_jugadores]}")
        return []
    logging.info(
        print_cabecera(
            f"Jugadores intento {intento}. Jugadores restantes: {len(links_jugadores)}"
        )
    )

    for link in links_jugadores:
        target_urls.append(TargetURL(url=link[0]))

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
            index = index_chunk * CHUNK_SIZE + index_scraped
            _, cod_club, liga_club = links_jugadores[index]

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
                jugadores_a_reintentar.append((url, cod_club, liga_club))
                logging.info(f"Fallido {url}. A reintentar...")
                continue

            soup = BeautifulSoup(html, "html.parser")
            fecha_nacimiento, anho_nacimiento = get_fecha_nacimiento(soup)
            agente, link_agente = get_agente(soup)
            if link_agente != "-":
                links_agentes.append("https://www.transfermarkt.es" + link_agente)

            jugador: Jugador = Jugador(
                cod_jugador=url.split("/")[-1],
                dorsal=get_dorsal(soup),
                apodo=get_apodo_negrita(soup, get_dorsal(soup)),
                capitan=get_capitan(soup),
                nombre=get_nombre(soup, get_dorsal(soup)),
                fecha_nacimiento=fecha_nacimiento,
                anho_nacimiento=anho_nacimiento,
                lugar_nacimiento=get_lugar_nacimiento(soup),
                nacionalidad=get_nacionalidades(soup),
                altura=get_altura(soup),
                pie=get_pie_dominante(soup),
                posicion=get_posicion(soup),
                posiciones_secundarias=get_posiciones_secundarias(soup),
                cod_club_actual=cod_club,
                liga_club_actual=liga_club,
                agente=agente,
                fecha_fichado=get_fecha_fichaje(soup),
                contrato_hasta=get_contrato_hasta(soup),
                ultima_renovacion=get_ultima_renovacion(soup),
                club_cedente=get_club_cedente(soup),
                contrato_hasta_cedente=get_contrato_hasta_cedente(soup),
                opcion_cedente=get_opcion_cedente(soup),
            )
            if (
                not jugador.cod_jugador
                or jugador.nombre == "-"
                or jugador.cod_club_actual == "-"
            ):
                paginas_fallidas.append(
                    URLFail(
                        attempt=intento,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener el jugador.",
                    )
                )
                jugadores_a_reintentar.append((url, cod_club, liga_club))
                logging.info(f"Fallido {url}. A reintentar...")
                continue
            paginas_scrapeadas.add(url)
            logging.info(f"Scrapeado jugador: {jugador.nombre}")
            datos_jugadores.append(jugador)
            jugadores += 1
    if len(jugadores_a_reintentar) > 0 and intento < MAX_REINTENTOS:
        (
            datos_jugadores_reintentados,
            links_agentes_reintentados,
            num_jugadores_reintentados,
        ) = await scrapear_jugadores(jugadores_a_reintentar, intento + 1)
        datos_jugadores.extend(datos_jugadores_reintentados)
        links_agentes.extend(links_agentes_reintentados)
        jugadores += num_jugadores_reintentados
    if intento == 1:
        links_agentes = list(dict.fromkeys(links_agentes))
    return datos_jugadores, links_agentes, jugadores


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
    links_jugadores, num_jugadores, num_equipos = await scrapear_links_jugadores_t(
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
    logging.info(print_cabecera("Scraping de jugadores"))
    datos_jugadores, links_agentes, jugadores = await scrapear_jugadores(
        links_jugadores
    )
    if not datos_jugadores:
        logging.error("No se han podido obtener los datos de los jugadores.")
        return
    logging.info(
        print_cabecera(
            f"Scraping de jugadores: datos de {jugadores} jugadores distintos"
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
    guardar_datos_json(datos_jugadores, f"{ruta_archivo_datos}datos_jugadores")
    guardar_datos_json(links_agentes, f"{ruta_archivo_datos}links_representantes")
    guardar_datos_json(datos_jugadores, f"{ruta_archivo}datos_jugadores")
    guardar_datos_json(links_agentes, f"{ruta_archivo}links_representantes")


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
