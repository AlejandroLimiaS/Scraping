from utiles.utiles_modelos import Incidencia


def get_incidencias_y_links(soup):
    base_url = "https://www.transfermarkt.es"
    links_jugadores = []
    incidencias: list[Incidencia] = []

    for td in soup.select("td.hauptlink"):
        try:
            a_tag = td.find("a", href=True)
            if a_tag and "/profil/spieler/" in a_tag["href"]:
                href = a_tag["href"]
                url_completa = base_url + href
                links_jugadores.append(url_completa)
                span = a_tag.find("span", title=True)
                if span:
                    incidencia_text = span["title"].strip()
                    if incidencia_text and incidencia_text != "Capitán de equipo":
                        incidencias.append(
                            Incidencia(
                                cod_jugador=href.split("/")[-1],
                                incidencia=incidencia_text,
                            )
                        )

        except:
            continue

    return links_jugadores, incidencias
