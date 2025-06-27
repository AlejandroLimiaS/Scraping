import json
from utiles.utiles_modelos import Fichaje


def extraer_fichajes_desde_pre(soup):
    try:
        pre = soup.find("pre")

        data = json.loads(pre.get_text())
        fichajes: list[Fichaje] = []
        for transfer in data.get("transfers", []):
            temporada = transfer.get("season", None)
            temporada = (
                f"20{temporada.split('/')[0]}/20{temporada.split('/')[1]}"
                if temporada
                else None
            )
            fecha = transfer.get("date", None)

            from_club = transfer.get("from", {})
            to_club = transfer.get("to", {})

            club_anterior = (
                from_club.get("clubName", ""),
                (
                    from_club.get("href", "").split("/verein/")[1].split("/")[0]
                    if from_club.get("href")
                    else None
                ),
            )
            club_nuevo = (
                to_club.get("clubName", ""),
                (
                    to_club.get("href", "").split("/verein/")[1].split("/")[0]
                    if to_club.get("href")
                    else None
                ),
            )

            valor = transfer.get("marketValue", None)
            coste = transfer.get("fee", None)

            fichaje = Fichaje(
                temporada=temporada,
                fecha=fecha,
                club_anterior=club_anterior,
                club_nuevo=club_nuevo,
                valor=valor,
                coste=coste,
            )
            fichajes.append(fichaje)

        return fichajes
    except:
        return []
