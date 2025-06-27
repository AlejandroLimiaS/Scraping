def get_temporada(soup):
    try:
        label = soup.find("select", {"name": "saison_id"})
        temporada = (
            label.find("option", selected=True).get("value").strip() if label else "-"
        )
        temporada_int = int(temporada) if temporada != "-" else "-"
        return f"{temporada_int}/{temporada_int+1}" if temporada != "-" else "-"
    except:
        return "-"


def get_division_grupo(soup):
    try:
        label = soup.find(
            "h1",
            class_="data-header__headline-wrapper data-header__headline-wrapper--oswald",
        )
        texto = label.get_text(strip=True)
        division, grupo = map(str.strip, texto.split(" - "))
        return division, grupo if grupo and division else "-"
    except:
        return "-", "-"
