import re


def get_nombre_agencia(soup) -> str:
    try:
        nombre_span = soup.find("div", class_="advisor-header__name-content")
        return (
            nombre_span.find("h2", class_="advisor-header__headline").text.strip()
            if nombre_span
            else "-"
        )
    except:
        return "-"


def get_telefono_agencia(soup) -> str:
    try:
        telefono_span = soup.find(
            "span", class_="advisor-header__content-left", string="Teléfono:"
        )
        return (
            telefono_span.find_next(
                "span", class_="advisor-header__content-right"
            ).text.strip()
            if telefono_span
            else "-"
        )
    except:
        return "-"


def get_email_agencia(soup) -> str:
    try:
        email_label = soup.find(
            "span", class_="advisor-header__content-left", string="Correo electrónico:"
        )
        contenedor_email = (
            email_label.find_next("span", class_="advisor-header__content-right--bold")
            if email_label
            else None
        )
        enlace = contenedor_email.find("a") if contenedor_email else None

        if enlace and enlace.has_attr("href") and enlace["href"].startswith("mailto:"):
            return enlace["href"].replace("mailto:", "").strip()
        return "-"
    except:
        return "-"


def normalizar_url(url: str) -> str:
    url = re.sub(r"^/+", "", url)

    if url.startswith("www"):
        return url

    if url.startswith(("http://", "https://")):
        return "w" + url.strip("w", 1)[1]

    return "-"


def get_pagina_web_agencia(soup) -> str:
    try:
        web_label = soup.find(
            "span", class_="advisor-header__content-left", string="Página web:"
        )
        contenedor_web = (
            web_label.find_next("span", class_="advisor-header__content-right--bold")
            if web_label
            else None
        )
        enlace = contenedor_web.find("a") if contenedor_web else None

        if enlace and enlace.has_attr("href"):
            href = enlace["href"].strip()
            return normalizar_url(href)
        return "-"
    except:
        return "-"


def get_direccion_agencia(soup) -> tuple[str, str, str, str]:
    try:
        calle_span = soup.find(
            "span", class_="advisor-header__content-left", string="Calle:"
        )
        cod_ubi_span = soup.find(
            "span",
            class_="advisor-header__content-left",
            string="Código postal, ubicación",
        )
        pais_span = soup.find(
            "span", class_="advisor-header__content-left", string="País"
        )

        calle = (
            calle_span.find_next(
                "span", class_="advisor-header__content-right"
            ).text.strip()
            if calle_span
            else "-"
        )
        cod_ubi = (
            cod_ubi_span.find_next(
                "span", class_="advisor-header__content-right"
            ).text.strip()
            if cod_ubi_span
            else "-"
        )
        pais = (
            pais_span.find_next(
                "span", class_="advisor-header__content-right"
            ).text.strip()
            if pais_span
            else "-"
        )

        if "," in cod_ubi:
            codigo_postal, ubicacion = [x.strip() for x in cod_ubi.split(",", 1)]
        else:
            codigo_postal, ubicacion = "-", cod_ubi.strip()

        return calle, codigo_postal, ubicacion, pais
    except:
        return "-", "-", "-", "-"
