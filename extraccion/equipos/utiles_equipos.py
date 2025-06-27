import re


def get_nombre(soup) -> str:
    try:
        nombre_tag = soup.find("div", class_="data-header__headline-container").find(
            "h1", class_="data-header__headline-wrapper"
        )
        return nombre_tag.get_text(strip=True) if nombre_tag else "-"
    except AttributeError:
        return "-"


def get_localidad(soup) -> str:
    try:

        table = soup.find("table", class_="profilheader")
        if not table:
            return "-"

        rows = table.find_all("tr")
        for i, row in enumerate(rows):
            th = row.find("th")
            if th and th.text.strip() == "Dirección:":
                if i + 2 < len(rows):
                    siguiente_row = rows[i + 1]
                    dossig_row = rows[i + 2]
                    td_siguiente = siguiente_row.find("td")
                    td_dossig = dossig_row.find("td")
                    if td_siguiente:
                        texto = td_siguiente.get_text(" ", strip=True)

                        if "(" in texto:
                            texto = texto.split("(")[0].strip()

                        partes = texto.split()
                        if any(char.isdigit() for char in partes[0]):
                            partes.pop(0)

                        localidad = " ".join(partes).strip()
                    if td_dossig:
                        pais = td_dossig.get_text(" ", strip=True)
                        return f"{localidad} {pais}"
        return "-"
    except Exception:
        return "-"


def get_liga(soup) -> str:
    try:
        liga_tag = (
            soup.find("div", class_="data-header__club-info")
            .find("span", class_="data-header__club")
            .find("a")
        )
        return liga_tag.get_text(strip=True) if liga_tag else "-"
    except:
        return "-"


def get_enlace_estadio(soup) -> str:
    try:

        li_tags = soup.find_all("li", class_="data-header__label")
        for li in li_tags:
            if "Estadio" in li.text:
                enlace = li.find("a")
                if enlace and "href" in enlace.attrs:
                    return enlace["href"]
        return "-"
    except Exception as e:
        return "-"


def obtener_primer_equipo(soup, nombre_equipo: str) -> tuple[str, str]:
    try:

        equipos_tags = soup.find("ul", class_="data-header__list-clubs").find_all("li")
        for i in range(len(equipos_tags)):
            primer_equipo_tag = equipos_tags[i].find("a")
            primer_equipo_nombre = primer_equipo_tag.get("title").strip()

            if re.search(r"\b(B|C)\b", primer_equipo_nombre):
                continue

            primer_equipo_link = primer_equipo_tag["href"]
            break

        if nombre_equipo == primer_equipo_nombre:
            return nombre_equipo, "-"
        else:
            return primer_equipo_nombre, primer_equipo_link
    except:
        return "-", "-"


def get_nombre_estadio(soup) -> str:
    try:
        nombre_estadio_tag = soup.find("th", text="Nombre del estadio:").find_next("td")
        return nombre_estadio_tag.get_text(strip=True) if nombre_estadio_tag else "-"
    except:
        return "-"


def get_superficie_estadio(soup) -> str:
    try:
        superficie_tag = soup.find("th", text="Superficie:").find_next("td")
        return superficie_tag.get_text(strip=True) if superficie_tag else "-"
    except:
        return "-"


def get_dimensiones_estadio(soup) -> str:
    try:
        dimensiones_tag = soup.find(
            "th", text="Medidas del terreno de juego:"
        ).find_next("td")
        return dimensiones_tag.get_text(strip=True) if dimensiones_tag else "-"
    except:
        return "-"


def get_aforo_estadio(soup) -> int:
    try:
        aforo_tag = soup.find("th", text="Capacidad total:").find_next("td")
        aforo = aforo_tag.get_text(strip=True) if aforo_tag else "-"
        return int(aforo.replace(".", "").replace(" ", "")) if aforo != "-" else 0
    except:
        return 0


def get_direccion_y_localidad(soup) -> tuple[str, str]:
    try:
        direccion_parts = []
        h2_contacto = soup.find(
            "h2", string=lambda text: text and text.strip().lower() == "contacto"
        )
        if not h2_contacto:
            return "-", "-"
        content_div = soup.find("div", class_="content zentriert")

        if content_div:
            table = content_div.find("table", class_="profilheader")

            if table:
                rows = table.find_all("tr")

                for row in rows:
                    td = row.find("td")
                    if td:
                        direccion_parts.append(td.text.strip())

        if not direccion_parts:
            return "-", "-"

        if len(direccion_parts) >= 4:
            direccion = " ".join(direccion_parts[1:4]).strip()
            cp_ciudad = direccion_parts[2].split()
            if cp_ciudad and (cp_ciudad[0].isdigit() or cp_ciudad[0].starswith("AD")):
                ciudad = " ".join(cp_ciudad[1:])
            else:
                ciudad = direccion_parts[2].strip()
            localidad = f"{ciudad} {direccion_parts[3]}"

        elif len(direccion_parts) >= 3:
            direccion = "-"
            cp_ciudad = direccion_parts[1].split()
            if cp_ciudad and (cp_ciudad[0].isdigit() or cp_ciudad[0].startswith("AD")):
                ciudad = " ".join(cp_ciudad[1:])
            else:
                ciudad = direccion_parts[1].strip()
            localidad = f"{ciudad} {direccion_parts[2]}"
        else:
            localidad = "-"
            direccion = "-"
        return direccion, localidad

    except:
        return "-", "-"
