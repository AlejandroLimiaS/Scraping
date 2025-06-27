def get_cod_local(soup):
    try:
        div_heim = soup.find("div", class_="sb-team sb-heim")

        if div_heim:
            a_tag = div_heim.find("a", class_="sb-vereinslink")
            if a_tag and a_tag.get("href"):
                href = a_tag["href"]
                if "/startseite/" in href:
                    return href.split("startseite/")[1].split("/")[1]
                return 0
    except:
        return 0


def get_cod_visitante(soup):
    try:
        div_heim = soup.find("div", class_="sb-team sb-gast")
        if div_heim:
            a_tag = div_heim.find("a", class_="sb-vereinslink")
            if a_tag and a_tag.get("href"):
                href = a_tag["href"]
                if "/startseite/" in href:
                    return href.split("startseite/")[1].split("/")[1]
                return 0
    except:
        return 0


def get_nombre_liga(soup):
    try:
        h2 = soup.find("h2", class_="direct-headline__header")
        if h2:
            a_tag = h2.find("a", class_="direct-headline__link")
            if a_tag:
                return a_tag.text.strip()
        return ""
    except:
        return ""


def get_cod_estadio_club(soup):
    try:
        p_info = soup.find("p", class_="sb-zusatzinfos")
        if p_info:
            a_tag = p_info.find("a", href=True)
            if a_tag:
                href = a_tag["href"]
                if "/stadion/verein/" in href:
                    return href.split("/verein/")[1].split("/")[0]
        return 0
    except:
        return 0


def get_jornada(soup):
    try:
        div_datos = soup.find("div", class_="sb-spieldaten")
        if div_datos:
            p_datum = div_datos.find("p", class_="sb-datum hide-for-small")
            if p_datum:
                jornada = None
                a_tags = p_datum.find_all("a", href=True)
                for a in a_tags:
                    href = a["href"]
                    if not "datum" in href:
                        text = a.text.strip()
                        if "." in text:
                            jornada = text.split(".")[0]
                        else:
                            jornada = text

                return jornada
        return None
    except:
        return None


def get_enlaces_partidos(soup):
    enlaces = []
    try:
        soup.find("div")

        tds = soup.find_all("td", class_="zentriert hauptlink")

        for td in tds:
            a_tag = td.find("a")
            if a_tag and a_tag.get("href"):
                enlaces.append(a_tag["href"])
        return enlaces
    except:
        return enlaces
