# coding=utf-8
import logging
import sys

from typing import Dict, List
from pathlib import Path
from bs4 import BeautifulSoup

sys.path.append(str(Path(__file__).resolve().parent.parent))
from utiles.utiles_modelos import TargetURL
from utiles.utiles_salida import print_cabecera
from utiles.utiles_scraping import (
    CHUNK_SIZE,
    ScrapedURL,
    chunked_iterable,
    scrape_urls,
    URLFailReason,
    URLFail,
    MAX_REINTENTOS,
)


paginas_fallidas: List[URLFail] = []
paginas_scrapeadas = set()


LinksLigas: Dict[str, str] = {
    "1RFEFG1": "https://www.transfermarkt.es/primera-division-r-f-e-f-grupo-i/startseite/wettbewerb/E3G1",
    "1RFEFG2": "https://www.transfermarkt.es/primera-division-r-f-e-f-grupo-ii/startseite/wettbewerb/E3G2",
    "2RFEFG1": "https://www.transfermarkt.es/segunda-division-r-f-e-f-grupo-i/startseite/wettbewerb/E4G1",
    "2RFEFG2": "https://www.transfermarkt.es/segunda-division-r-f-e-f-grupo-ii/startseite/wettbewerb/E4G2",
    "2RFEFG3": "https://www.transfermarkt.es/segunda-division-r-f-e-f-grupo-iii/startseite/wettbewerb/E4G3",
    "2RFEFG4": "https://www.transfermarkt.es/segunda-division-r-f-e-f-grupo-iv/startseite/wettbewerb/E4G4",
    "2RFEFG5": "https://www.transfermarkt.es/segunda-division-r-f-e-f-grupo-v/startseite/wettbewerb/E4G5",
}


async def scrapear_links_equipos(
    ligas_a_scrapear: List[str] = None, intentos=1
) -> tuple[List[str], int, int]:
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
    links_equipos: List[str] = []
    equipos_a_reintentar: List[str] = []
    num_equipos = 0
    num_ligas = 0
    logging.info(
        print_cabecera(
            f"Scrapeando enlaces de equipos intento {intentos}. Ligas restantes: {len(ligas_a_scrapear)}"
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
            equipos_a_reintentar.extend(
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
                equipos_a_reintentar.append(url)
                logging.info(f"Fallida {url}. A reintentar...")
                continue

            soup = BeautifulSoup(html, "html.parser")
            equipos = soup.select("td.hauptlink a")
            if not equipos:
                paginas_fallidas.append(
                    URLFail(
                        attempt=intentos,
                        url=url,
                        reason=URLFailReason.NOT_FOUND.value,
                        message="No se pudo obtener el equipo.",
                    )
                )
                equipos_a_reintentar.append(url)
                logging.info(f"Fallida {url}. A reintentar...")
                continue
            for equipo in equipos:
                href = equipo["href"]
                if "/startseite/verein/" in href:
                    links_equipos.append(
                        "https://www.transfermarkt.es" + href.rsplit("/", 2)[0]
                    )
            links_equipos = list(dict.fromkeys(links_equipos))
            logging.info(
                f"Scrapeados {len(links_equipos)-num_equipos} enlaces de equipos de la liga {next((liga for liga,url in LinksLigas.items() if url==scrapedURL.url),scrapedURL.url)}."
            )
            num_equipos = len(links_equipos)
            num_ligas += 1
            paginas_scrapeadas.add(url)
    if len(equipos_a_reintentar) > 0 and intentos < MAX_REINTENTOS:
        links_equipos_reintentados, num_equipos_reintentados, num_ligas_reintentadas = (
            await scrapear_links_equipos(equipos_a_reintentar, intentos + 1)
        )
        links_equipos.extend(links_equipos_reintentados)
        num_equipos += num_equipos_reintentados
        num_ligas += num_ligas_reintentadas
    if intentos == 1:
        links_equipos = list(dict.fromkeys(links_equipos))
    return links_equipos, num_equipos, num_ligas


async def scrapear_links_jugadores(
    links_equipos: List[str], intentos=1
) -> tuple[List[str], int, int]:
    global paginas_fallidas, paginas_scrapeadas
    if intentos > MAX_REINTENTOS:
        logging.error(
            f"Se ha alcanzado el número máximo de reintentos ({MAX_REINTENTOS}) para los jugadores."
        )
        logging.info(f"Jugadores fallidos: {[link for link in links_equipos]}")
        return []
    target_urls: List[TargetURL] = []
    paginas_a_reintentar: List[str] = []
    links_jugadores: List[str] = []
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
                    links_jugadores.append("https://www.transfermarkt.es" + href)

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
    if intentos == 1:
        links_jugadores = list(dict.fromkeys(links_jugadores))
    return links_jugadores, num_jugadores, num_equipos
