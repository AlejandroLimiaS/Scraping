import json
import re
from utiles.utiles_modelos import Fichaje, Incidencia, ValorMercado


def safe_find_text(soup, selector, default="-"):
    try:
        return soup.select_one(selector).text.strip()
    except:
        return default


def safe_find_attr(soup, selector, attr, default="-"):
    try:
        return soup.select_one(selector)[attr].strip()
    except:
        return default


def get_dorsal(soup):
    return safe_find_text(soup, "span.data-header__shirt-number").replace("#", "")


def get_apodo(soup, dorsal):
    texto = safe_find_text(soup, "h1.data-header__headline-wrapper")
    return texto.replace(f"#{dorsal}", "").strip() if dorsal in texto else texto


def get_apodo_negrita(soup, dorsal):
    apodo_bold = safe_find_text(soup, "h1.data-header__headline-wrapper strong")
    return apodo_bold if apodo_bold != "-" else get_apodo(soup, dorsal)


def get_capitan(soup):
    return 1 if soup.select_one("img[alt='Capitán'], img[title='Capitán']") else 0


def get_nombre(soup, dorsal):
    try:
        opciones = {"Nombre en país de origen:", "Nombre completo:" "Nombre:"}
        label = soup.find("span", string=lambda s: s in opciones)
        nombre_tag = label.find_next_sibling(
            "span", class_="info-table__content info-table__content--bold"
        )
        if nombre_tag:
            nombre = nombre_tag.text.strip()
            if re.search(r"[^\u0000-\u00FF]", nombre):
                apodo = get_apodo(soup, dorsal)
                return apodo
            return nombre
        else:
            return get_apodo(soup, dorsal)
    except:
        return get_apodo(soup, dorsal)


def get_fecha_nacimiento(soup):
    try:
        label = soup.find("span", string=lambda t: "F. Nacim./Edad:" in t)
        fecha_tag = label.find_next_sibling("span").find("a")
        if fecha_tag:
            fecha = fecha_tag.text.strip().split(" ")[0]
            anho = fecha.split("/")[-1]
        else:
            fecha_span = label.find_next_sibling("span")
            texto = fecha_span.get_text(strip=True)
            fecha = "-"
            anho = texto.split("(")[0].strip()
        return fecha, anho
    except:
        return "-", "-"


def get_lugar_nacimiento(soup):
    try:
        label = soup.find("span", string="Lugar de nac.:")
        lugar_span = label.find_next_sibling("span").find("span")
        img = lugar_span.find("img")
        ciudad = lugar_span.get_text(strip=True)
        pais = img["title"].strip()
        return f"{ciudad}, {pais}"
    except:
        return "-"


def get_nacionalidades(soup):
    try:
        label = soup.find("span", string="Nacionalidad:")
        span = label.find_next_sibling("span")
        return [img["title"] for img in span.find_all("img")]
    except:
        return []


def get_altura(soup):
    try:
        label = soup.find("span", string="Altura:")
        altura_span = label.find_next_sibling("span")
        return altura_span.text.strip()
    except:
        return "-"


def get_pie_dominante(soup):
    try:
        label = soup.find("span", string="Pie:")
        pie_span = label.find_next_sibling("span")
        return pie_span.text.strip()
    except:
        return "-"


def get_posicion(soup):
    try:
        label = soup.find("span", string="Posición:")
        pos_span = label.find_next_sibling("span")
        return pos_span.text.strip()
    except:
        return "-"


def get_posiciones_secundarias(soup):
    try:
        contenedor = soup.find("div", class_="detail-position__position")
        elementos = contenedor.find_all("dd", class_="detail-position__position")
        return [e.get_text(strip=True) for e in elementos]
    except:
        return []


def get_valor_mercado(soup):
    try:
        valor = safe_find_text(soup, "a.data-header__market-value-wrapper")
        return valor.split("\n")[0]
    except:
        return "-"


def get_agente(soup):
    try:
        label = soup.find("span", string=lambda t: "Agente:" in t)
        span = label.find_next_sibling("span")
        a = span.find("a")
        if a:
            title = (
                a.find("span").get("title")
                if a.find("span") and a.find("span").get("title")
                else a.get("title") if a.get("title") else a.text.strip()
            )
            href = a.get("href", "-")
            return title.strip(), href

        return span.text.strip(), "-"
    except:
        return "-", "-"


def get_fecha_fichaje(soup):
    try:
        label = soup.find("span", string=lambda t: "Fichado:" in t)
        fichado = label.find_next_sibling("span").text.strip() if label else "-"
        return fichado
    except:
        return "-"


def get_contrato_hasta(soup):
    try:
        label = soup.find("span", string=lambda t: "Contrato hasta:" in t)
        contrato = label.find_next_sibling("span").text.strip() if label else "-"
        return contrato
    except:
        return "-"


def get_ultima_renovacion(soup):
    try:
        label = soup.find("span", string=lambda t: "Última renovación:" in t)
        renovacion = label.find_next_sibling("span").text.strip() if label else "-"
        return renovacion
    except:
        return "-"


def get_club_cedente(soup):
    try:
        label = soup.find("span", string=lambda t: "Prestado de:" in t)
        club = label.find_next_sibling("span").find("a").text.strip() if label else "-"
        return club
    except:
        return "-"


def get_contrato_hasta_cedente(soup):
    try:
        label = soup.find("span", string=lambda t: "Contrato allí hasta:" in t)
        contrato_alli = label.find_next_sibling("span").text.strip() if label else "-"
        return contrato_alli
    except:
        return "-"


def get_opcion_cedente(soup):
    try:
        label = soup.find("span", string=lambda t: "Opción de compra" in t)
        return label.text.strip() if label else "-"
    except:
        return "-"


def get_valores_mercado(soup):
    try:
        pre = soup.find("pre")

        data = json.loads(pre.get_text())
        valor_mercado_actual = ValorMercado(
            valor=data.get("current", "-"), fecha=data.get("last_change", "-")
        )
        valor_mercado_maximo = ValorMercado(
            valor=data.get("highest", "-"), fecha=data.get("highest_date", "-")
        )
        return valor_mercado_actual, valor_mercado_maximo
    except:
        return []
