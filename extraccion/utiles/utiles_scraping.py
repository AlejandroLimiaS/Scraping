import asyncio
import json
import logging
import os
import sys
from dataclasses import asdict, is_dataclass
from itertools import islice
from pathlib import Path
from typing import Any, List
from playwright.async_api import async_playwright

sys.path.append(str(Path(__file__).resolve().parent))
from utiles.utiles_modelos import (
    URLFail,
    URLFailReason,
    ScrapedURL,
    TargetURL,
)
from utiles.utiles_salida import print_cabecera

SEMAPHORE_VALUE = 10
MAX_REINTENTOS = 7
CHUNK_SIZE = 10


PROXY_IP = "dc.oxylabs.io"
PROXY_PORT = "8000"
USERNAME = os.getenv("PROXY_USERNAME")
PASSWORD = os.getenv("PROXY_PASSWORD")


semaphore = asyncio.Semaphore(SEMAPHORE_VALUE)


async def obtener_contenido_url(
    target_url: TargetURL,
    context,
    reject_cookies: bool,
    retries=MAX_REINTENTOS,
    delay=5,
) -> ScrapedURL:
    paginas_fallidas = []
    for attempt in range(retries):
        try:
            async with semaphore:
                page = await context.new_page()
                await page.route(
                    "**/*",
                    lambda route: (
                        route.abort()
                        if route.request.resource_type in ["stylesheet", "font"]
                        else route.continue_()
                    ),
                )
                await page.set_extra_http_headers(
                    {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
                        "Accept-Encoding": "gzip, deflate",
                    }
                )

                await page.goto(target_url.url, timeout=60000)
                await page.wait_for_timeout(2000)

                if page.url != target_url.url:
                    paginas_fallidas.append(
                        URLFail(
                            attempt=attempt,
                            url=target_url.url,
                            reason=URLFailReason.REDIRECTED,
                            message=f"Destino: {page.url}",
                        )
                    )
                    await page.close()
                    raise Exception()

                if reject_cookies:
                    try:
                        await page.click("text=NO CONSENTIR", timeout=3000)
                    except Exception:
                        None

                if target_url.clicks and len(target_url.clicks) > 0:
                    for click in target_url.clicks:
                        try:
                            await page.wait_for_selector(
                                click.selector, timeout=click.timeout
                            )
                            await page.focus(click.selector)
                            await page.keyboard.press("Enter")
                            if click.wait_for_selector:
                                await page.wait_for_selector(
                                    click.wait_for_selector, timeout=click.timeout
                                )
                        except Exception:
                            logging.error(
                                f"Error al hacer clic en {click.selector} o esperar a {click.wait_for_selector}"
                            )
                            paginas_fallidas.append(
                                URLFail(
                                    attempt=attempt,
                                    url=target_url.url,
                                    reason=URLFailReason.CLICK_FAILED,
                                    message=f"Selector: {click.selector}, Wait for: {click.wait_for_selector}",
                                )
                            )
                            await page.close()
                            raise Exception()

                if target_url.sleep > 0:
                    await asyncio.sleep(target_url.sleep)

                if target_url.selector is not None and target_url.selector != "":
                    try:
                        await page.wait_for_selector(target_url.selector, timeout=5000)
                    except Exception:
                        logging.error(
                            f"Selector {target_url.selector} no encontrado en {target_url.url}"
                        )
                        paginas_fallidas.append(
                            URLFail(
                                attempt=attempt,
                                url=target_url.url,
                                reason=URLFailReason.NO_HTML,
                                message=f"No se pudo encontrar el selector {target_url.selector}",
                            )
                        )
                        await page.close()
                        raise Exception()

                if target_url.scroll == True:
                    previous_height = await page.evaluate("document.body.scrollHeight")
                    scroll_attempts = 5
                    for _ in range(scroll_attempts):
                        print(_)
                        await page.evaluate(
                            "window.scrollTo(0, document.body.scrollHeight)"
                        )
                        await asyncio.sleep(1)
                        new_height = await page.evaluate("document.body.scrollHeight")
                        if new_height == previous_height:
                            break
                        previous_height = new_height

                if (await page.content() == None) or (await page.content() == ""):
                    paginas_fallidas.append(
                        URLFail(
                            attempt=attempt,
                            url=target_url.url,
                            reason=URLFailReason.NO_HTML,
                            message="No se pudo obtener el contenido HTML",
                        )
                    )
                    await page.close()
                    raise Exception()
                content = await page.content()
                if len(content) < 1000 and target_url.json is False:
                    paginas_fallidas.append(
                        URLFail(
                            attempt=attempt,
                            url=target_url.url,
                            reason=URLFailReason.EMPTY,
                            message="Contenido vacío (<1000 caracteres)",
                        )
                    )
                    await page.close()
                    raise Exception()

                await page.close()
                return ScrapedURL(
                    url=target_url.url,
                    content=content,
                    paginas_fallidas=paginas_fallidas,
                )
        except Exception:
            None

        await asyncio.sleep(delay * (2**attempt))

    return ScrapedURL(
        url=target_url.url, content=None, paginas_fallidas=paginas_fallidas
    )


def chunked_iterable(iterable, size):
    it = iter(iterable)
    return iter(lambda: list(islice(it, size)), [])


async def scrape_urls(
    target_urls: List[TargetURL], reject_cookies: bool = False
) -> List[ScrapedURL]:
    if USERNAME is None or PASSWORD is None:
        raise Exception(
            "Credenciales de proxy no encontradas. Asegura que las variables de entorno PROXY_USERNAME y PROXY_PASSWORD están configuradas."
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            proxy={
                "server": f"http://{PROXY_IP}:{PROXY_PORT}",
                "username": USERNAME,
                "password": PASSWORD,
            },
        )
        context = await browser.new_context()
        await context.add_init_script(
            f"Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined }});"
        )
        tasks = [
            obtener_contenido_url(target_url, context, reject_cookies)
            for target_url in target_urls
        ]
        results = await asyncio.gather(*tasks)

        await browser.close()
        return results


def convertir_a_serializable(obj: Any) -> Any:
    if is_dataclass(obj):
        return {k: convertir_a_serializable(v) for k, v in asdict(obj).items()}
    elif isinstance(obj, dict):
        return {k: convertir_a_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convertir_a_serializable(v) for v in obj]
    else:
        return obj


def guardar_datos_json(datos, ruta_archivo):
    nuevos_datos_json = [convertir_a_serializable(entrada) for entrada in datos]

    with open(f"{ruta_archivo}.json", "w", encoding="utf-8") as f:
        json.dump(nuevos_datos_json, f, ensure_ascii=False, indent=4)

    logging.info(print_cabecera("Fichero JSON"))
    logging.info(f"Fichero creado: {os.path.abspath(f'{ruta_archivo}.json')}")
