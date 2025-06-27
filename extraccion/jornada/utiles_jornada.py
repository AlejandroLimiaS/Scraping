import os
import pickle
from utiles.utiles_modelos import Amonestacion, Cambio, Gol, PenaltiFallado


def cargar_diccionario_posicion_a_minuto():
    ruta_archivo = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "pos_a_minuto.pkl"
    )

    with open(ruta_archivo, "rb") as fichero:
        diccionario = pickle.load(fichero)

    return diccionario


def get_fecha_y_hora(soup):
    try:
        div_datos = soup.find("div", class_="sb-spieldaten")
        if div_datos:
            p_datum = div_datos.find("p", class_="sb-datum hide-for-small")
            if p_datum:
                texto_completo = p_datum.get_text(strip=True)
                partes = texto_completo.split(", ")
                if len(partes) > 1:
                    fecha_hora = partes[1].replace("H", "").strip()
                    fecha_hora = fecha_hora.replace("|", "").strip()
                    return fecha_hora
        return None
    except:
        return None


def get_resultado_final(soup):
    try:

        div_resultado = soup.find("div", class_="sb-endstand")
        if div_resultado:
            resultado = div_resultado.get_text(strip=True)

            if "(" in resultado:
                resultado = resultado.split("(")[0].strip()

            goles = resultado.split(":")
            if len(goles) == 2 and goles[0].isdigit() and goles[1].isdigit():
                return int(goles[0]), int(goles[1])
        return None, None
    except:
        return None, None


def get_codigos_jugadores_de_bloque(alineacion_div) -> list[int]:
    jugadores_divs = alineacion_div.find_all("div", class_="formation-player-container")
    codigos = []

    for jugador_div in jugadores_divs:
        a_tag = jugador_div.find("a", href=True)
        if a_tag and "spieler" in a_tag["href"]:
            codigos.append((a_tag["href"].rsplit("/", 1)[-1], a_tag["href"]))

    return codigos


def get_codigos_suplentes_de_bloque(suplentes_div) -> list[int]:
    codigos = []
    filas = suplentes_div.find_all("tr")

    for fila in filas:
        a_tag = fila.find("a", href=True)
        if a_tag and "spieler" in a_tag["href"]:
            codigos.append((a_tag["href"].rsplit("/", 1)[-1], a_tag["href"]))
    return codigos


def get_codigos_jugadores_local_y_visitante(soup):
    alineaciones = soup.find_all("div", class_="aufstellung-vereinsseite")

    if len(alineaciones) < 8:
        return [], [], [], []

    codigos_local_titulares = get_codigos_jugadores_de_bloque(alineaciones[2])
    codigos_local_suplentes = get_codigos_suplentes_de_bloque(alineaciones[3])
    codigos_visitante_titulares = get_codigos_jugadores_de_bloque(alineaciones[6])
    codigos_visitante_suplentes = get_codigos_suplentes_de_bloque(alineaciones[7])

    return (
        codigos_local_titulares,
        codigos_local_suplentes,
        codigos_visitante_titulares,
        codigos_visitante_suplentes,
    )


def get_goles_local_y_visitante(soup, pos_a_minuto):
    goles_local = []
    goles_visitante = []

    contenedor_goles = soup.find("div", id="sb-tore")
    if not contenedor_goles:
        return goles_local, goles_visitante

    eventos = contenedor_goles.find_all(
        "li", class_=["sb-aktion-heim", "sb-aktion-gast"]
    )

    for evento in eventos:
        gol = Gol()

        res_tag = evento.find("div", class_="sb-aktion-spielstand")
        if res_tag:
            b_tag = res_tag.find("b")
            gol.resultado_gol = b_tag.text.strip() if b_tag else None

        div_uhr = evento.find("div", class_="sb-aktion-uhr")
        gol.minuto = None
        if div_uhr:
            span_uhr = div_uhr.find("span", class_="sb-sprite-uhr-klein")
            if span_uhr and "style" in span_uhr.attrs:
                style = span_uhr["style"]
                minuto = (
                    style.replace("background-position:", "").replace(";", "").strip()
                )
                gol.minuto = pos_a_minuto.get(minuto, None)

        div_accion = evento.find("div", class_="sb-aktion-aktion")
        if div_accion:

            texto_limpio = div_accion.get_text(separator=" ", strip=True)
            partes = texto_limpio.split("asistente:")

            texto_gol = partes[0].strip()
            trozos_gol = [t.strip() for t in texto_gol.split(",")]
            gol_desc = trozos_gol[1] if len(trozos_gol) > 1 else None
            gol.desc = gol_desc

            if len(partes) > 1:
                texto_asist = partes[1].strip()
                trozos_asist = [t.strip() for t in texto_asist.split(",")]
                asist_desc = trozos_asist[1] if len(trozos_asist) > 1 else None
                if asist_desc and asist_desc.lower() != "sin asistencia":
                    gol.desc_asist = asist_desc

            enlaces = div_accion.find_all("a")
            if len(enlaces) >= 1:
                href = enlaces[0].get("href", "")
                if "/spieler/" in href:
                    gol.cod_goleador = href.split("/spieler/")[1].split("/")[0]

            if len(enlaces) >= 2:
                href_asist = enlaces[1].get("href", "")
                if "/spieler/" in href_asist:
                    gol.cod_asistente = href_asist.split("/spieler/")[1].split("/")[0]
            else:
                gol.cod_asistente = None
        if "sb-aktion-heim" in evento["class"]:
            goles_local.append(gol)
        else:
            goles_visitante.append(gol)

    return goles_local, goles_visitante


def get_cambios(soup, pos_a_minuto):
    contenedor_cambios = soup.find("div", id="sb-wechsel")
    cambios_local, cambios_visitante = [], []

    if contenedor_cambios:
        eventos = contenedor_cambios.find_all(
            "li", class_=["sb-aktion-heim", "sb-aktion-gast"]
        )
        for evento in eventos:
            cambio = Cambio()

            div_uhr = evento.find("div", class_="sb-aktion-uhr")
            cambio.minuto = None
            if div_uhr:
                span_uhr = div_uhr.find("span", class_="sb-sprite-uhr-klein")
                if span_uhr and "style" in span_uhr.attrs:
                    style = span_uhr["style"]
                    minuto = (
                        style.replace("background-position:", "")
                        .replace(";", "")
                        .strip()
                    )
                    cambio.minuto = pos_a_minuto.get(minuto, None)

            div_accion = evento.find("div", class_="sb-aktion-aktion")
            if div_accion:

                ein_span = div_accion.find("span", class_="sb-aktion-wechsel-ein")
                if ein_span:
                    a_entrada = ein_span.find("a")
                    if a_entrada and "href" in a_entrada.attrs:
                        href = a_entrada["href"]
                        if "/spieler/" in href:
                            cambio.cod_entra = href.split("/spieler/")[1].split("/")[0]

                aus_span = div_accion.find("span", class_="sb-aktion-wechsel-aus")
                if aus_span:
                    a_salida = aus_span.find("a")
                    if a_salida and "href" in a_salida.attrs:
                        href = a_salida["href"]
                        if "/spieler/" in href:
                            cambio.cod_fuera = href.split("/spieler/")[1].split("/")[0]

                    span_desc = aus_span.find("span", class_="hide-for-small")
                    cambio.desc = (
                        span_desc.get_text().lstrip(", ").strip() if span_desc else None
                    )

            es_local = "sb-aktion-heim" in evento.get("class", [])
            if es_local:
                cambios_local.append(cambio)
            else:
                cambios_visitante.append(cambio)

    return cambios_local, cambios_visitante


def get_amonestaciones_local_visitante(soup, pos_a_minuto):
    div_karten = soup.find("div", id="sb-karten")
    if not div_karten:
        return [], []

    amonestaciones_local = []
    amonestaciones_visitante = []

    for li in div_karten.find_all("li", class_=["sb-aktion-heim", "sb-aktion-gast"]):
        am = Amonestacion()

        a_jugador = li.find("div", class_="sb-aktion-spielerbild").find("a")
        if a_jugador and "href" in a_jugador.attrs:
            href = a_jugador["href"]
            if "/spieler/" in href:
                am.cod_amonestado = href.rsplit("/", 1)[-1]

        span_stand = li.find("div", class_="sb-aktion-spielstand").find("span")
        am.amarilla = False
        am.roja = False
        if span_stand:
            clases = span_stand.get("class", [])
            if "sb-gelb" in clases:
                am.amarilla = True
            elif "sb-rot" in clases:
                am.roja = True
            elif "sb-gelbrot" in clases:
                am.amarilla = True
                am.roja = True

        div_uhr = li.find("div", class_="sb-aktion-uhr")
        am.minuto = None
        if div_uhr:
            span_uhr = div_uhr.find("span", class_="sb-sprite-uhr-klein")
            if span_uhr and "style" in span_uhr.attrs:
                style = span_uhr["style"]
                minuto = (
                    style.replace("background-position:", "").replace(";", "").strip()
                )
                am.minuto = pos_a_minuto.get(minuto, None)

        if "sb-aktion-heim" in li.get("class", []):
            amonestaciones_local.append(am)
        else:
            amonestaciones_visitante.append(am)

    return amonestaciones_local, amonestaciones_visitante


def get_penaltis_fallados(soup, pos_a_minuto):
    div_penaltis = soup.find("div", id="sb-verschossene")
    if not div_penaltis:
        return []

    penaltis = []

    for li in div_penaltis.find_all("li", class_=["sb-aktion-heim", "sb-aktion-gast"]):
        pf = PenaltiFallado()

        div_uhr = li.find("div", class_="sb-aktion-uhr")
        if div_uhr:
            span_uhr = div_uhr.find("span", class_="sb-sprite-uhr-klein")
            if span_uhr and "style" in span_uhr.attrs:
                style = span_uhr["style"]
                pos = style.replace("background-position:", "").replace(";", "").strip()
                pf.minuto = pos_a_minuto.get(pos, None)

        div_aus = li.find("span", class_="sb-aktion-wechsel-aus")
        if div_aus:
            a_portero = div_aus.find("a", href=True)
            if a_portero and "/spieler/" in a_portero["href"]:
                pf.cod_portero = a_portero["href"].split("spieler/", 1)[1].split("/")[0]

            texto = div_aus.get_text(strip=True).lower()
            pf.parado = "parad" in texto or "despej" in texto or "bloq" in texto

        penaltis.append(pf)

    return penaltis
