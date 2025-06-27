from dataclasses import dataclass
from typing import Optional
from insercion.utiles.utiles_db import ejecutar_con_reintentos
from insercion.utiles.utiles_paises import obtener_o_insertar_pais


@dataclass
class Localidad_BD:
    nombre: str
    provincia: Optional[str] = None
    comunidad_autonoma: Optional[str] = None
    pais: Optional[str] = None
    id_pais: Optional[int] = None


async def limpiar_localidad(cur, localidad: str) -> Localidad_BD:
    localidad_bd: Localidad_BD = Localidad_BD(
        "-", "-", "-", "-", await obtener_o_insertar_pais(cur, "-")
    )
    if not localidad or localidad == "-":
        return localidad_bd

    if "(" in localidad:
        partes = [p.strip() for p in localidad.split("(", 1)]
        localidad_bd.nombre = partes[0]
        localidad_bd.provincia, localidad_bd.pais = [
            p.strip() for p in partes[1].split(")", 1)
        ]
        localidad_bd.comunidad_autonoma = comunidad_autonoma_por_provincia(
            localidad_bd.provincia
        )
        localidad_bd.id_pais = await obtener_o_insertar_pais(cur, localidad_bd.pais)
        return localidad_bd

    if "," in localidad:
        partes = [p.strip() for p in localidad.split(",", 1)]
        localidad_bd.nombre = partes[0]
        if " " in partes[1]:
            localidad_bd.provincia, localidad_bd.pais = [
                p.strip() for p in partes[1].rsplit(" ", 1)
            ]
            localidad_bd.comunidad_autonoma = comunidad_autonoma_por_provincia(
                localidad_bd.provincia
            )
        else:
            localidad_bd.pais = partes[1]
            localidad_bd.provincia = provincia_de_localidad(localidad_bd.nombre)
            localidad_bd.comunidad_autonoma = comunidad_autonoma_por_provincia(
                localidad_bd.provincia
            )
        localidad_bd.id_pais = await obtener_o_insertar_pais(cur, localidad_bd.pais)
        return localidad_bd

    localidad_bd.nombre, localidad_bd.pais = localidad.strip().rsplit(" ", 1)
    localidad_bd.provincia = provincia_de_localidad(localidad_bd.nombre)
    localidad_bd.comunidad_autonoma = comunidad_autonoma_por_provincia(
        localidad_bd.provincia
    )
    localidad_bd.id_pais = await obtener_o_insertar_pais(cur, localidad_bd.pais)
    return localidad_bd


async def limpiar_localidad_jugador(cur, localidad: str) -> Localidad_BD:
    localidad_bd: Localidad_BD = Localidad_BD(
        "-", "-", "-", "-", await obtener_o_insertar_pais(cur, "-")
    )
    if not localidad or localidad == "-":
        return localidad_bd

    partes = [p.strip() for p in localidad.split(",", 1)]
    localidad_bd.nombre = partes[0]
    if "," in partes[1]:
        localidad_bd.provincia, localidad_bd.pais = [
            p.strip() for p in partes[1].rsplit(",", 1)
        ]
        if localidad_bd.pais == "España":
            localidad_bd.comunidad_autonoma = comunidad_autonoma_por_provincia(
                localidad_bd.provincia
            )
    elif "(" in partes[0]:
        localidad_bd.nombre, localidad_bd.provincia = [
            p.strip() for p in partes[0].split("(", 1)
        ]
        localidad_bd.provincia = localidad_bd.provincia.replace(")", "")
        localidad_bd.pais = partes[1]
        if localidad_bd.pais == "España":
            localidad_bd.comunidad_autonoma = comunidad_autonoma_por_provincia(
                localidad_bd.provincia
            )
    else:
        localidad_bd.pais = partes[1]
        if localidad_bd.pais == "España":
            localidad_bd.provincia = provincia_de_localidad(localidad_bd.nombre)
            localidad_bd.comunidad_autonoma = comunidad_autonoma_por_provincia(
                localidad_bd.provincia
            )
    localidad_bd.id_pais = await obtener_o_insertar_pais(cur, localidad_bd.pais)
    return localidad_bd


async def insertar_localidades_y_paises(cur, localidades, paises):
    paises_ids = {}
    localidades_ids = {}

    for pais in paises:
        id_pais = await obtener_o_insertar_pais(cur, pais)
        paises_ids[pais] = id_pais

    for loc in localidades:
        id_loc = await obtener_o_insertar_localidad(cur, loc, paises_ids)
        localidades_ids[loc.nombre] = id_loc

    return localidades_ids, paises_ids


async def _insertar_localidad(pool, loc: Localidad_BD) -> int:
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id_localidad FROM localidad WHERE nombre = %s",
                    (loc.nombre,),
                )
                existe = await cur.fetchone()
                if existe:
                    return existe[0]

                await cur.execute(
                    """
                    INSERT INTO localidad (nombre, provincia, comunidad_autonoma, id_pais)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        loc.nombre,
                        loc.provincia,
                        loc.comunidad_autonoma,
                        int(loc.id_pais),
                    ),
                )
                await conn.commit()
                return cur.lastrowid
        except Exception as e:
            await conn.rollback()
            raise e


async def insertar_localidad(pool, localidad_bd: Localidad_BD):
    return await ejecutar_con_reintentos(
        _insertar_localidad,
        pool,
        localidad_bd,
        descripcion=f"Insertar localidades gallegas: {localidad_bd.nombre}",
    )


async def obtener_o_insertar_localidad(
    cur, localidad_bd: Localidad_BD, paises_ids: dict
) -> int:
    await cur.execute(
        "SELECT id_localidad FROM localidad WHERE nombre = %s", (localidad_bd.nombre,)
    )
    existe = await cur.fetchone()
    if existe:
        return existe[0]
    if localidad_bd.id_pais is not None:
        await cur.execute(
            """
                INSERT INTO localidad (nombre, provincia, comunidad_autonoma, id_pais)
                VALUES (%s, %s, %s, %s)
                """,
            (
                localidad_bd.nombre,
                localidad_bd.provincia,
                localidad_bd.comunidad_autonoma,
                int(localidad_bd.id_pais),
            ),
        )
        return cur.lastrowid
    elif paises_ids and localidad_bd.pais in paises_ids:
        localidad_bd.id_pais = paises_ids[localidad_bd.pais]
    else:
        localidad_bd.id_pais = await obtener_o_insertar_pais(cur, localidad_bd.pais)
    await cur.execute(
        """
                INSERT INTO localidad (nombre, provincia, comunidad_autonoma, id_pais)
                VALUES (%s, %s, %s, %s)
                """,
        (
            localidad_bd.nombre,
            localidad_bd.provincia,
            localidad_bd.comunidad_autonoma,
            int(localidad_bd.id_pais),
        ),
    )
    return cur.lastrowid


def comunidad_autonoma_por_provincia(provincia_input: str) -> str | None:
    if provincia_input == "-":
        return "-"
    provincia_normalizada = provincia_input.strip().lower()

    mapa_provincias = {
        # Galicia
        "a coruña": "Galicia",
        "coruña": "Galicia",
        "la coruña": "Galicia",
        "lugo": "Galicia",
        "ourense": "Galicia",
        "orense": "Galicia",
        "pontevedra": "Galicia",
        # Asturias
        "asturias": "Principado de Asturias",
        # Castilla y León
        "león": "Castilla y León",
        "leon": "Castilla y León",
        "zamora": "Castilla y León",
        "salamanca": "Castilla y León",
        "ávila": "Castilla y León",
        "avila": "Castilla y León",
        "segovia": "Castilla y León",
        "soria": "Castilla y León",
        "burgos": "Castilla y León",
        "palencia": "Castilla y León",
        "valladolid": "Castilla y León",
        # Castilla-La Mancha
        "albacete": "Castilla-La Mancha",
        "ciudad real": "Castilla-La Mancha",
        "cuenca": "Castilla-La Mancha",
        "guadalajara": "Castilla-La Mancha",
        "toledo": "Castilla-La Mancha",
        # Cataluña
        "barcelona": "Cataluña",
        "tarragona": "Cataluña",
        "lleida": "Cataluña",
        "lerida": "Cataluña",
        "girona": "Cataluña",
        "gerona": "Cataluña",
        # Comunidad Valenciana
        "valencia": "Comunidad Valenciana",
        "alicante": "Comunidad Valenciana",
        "castellón": "Comunidad Valenciana",
        "castellon": "Comunidad Valenciana",
        # Aragón
        "zaragoza": "Aragón",
        "huesca": "Aragón",
        "teruel": "Aragón",
        # Navarra
        "navarra": "Navarra",
        # La Rioja
        "la rioja": "La Rioja",
        "rioja": "La Rioja",
        # País Vasco
        "bizkaia": "País Vasco",
        "vizcaya": "País Vasco",
        "gipuzkoa": "País Vasco",
        "guipúzcoa": "País Vasco",
        "guipuzcoa": "País Vasco",
        "áraba": "País Vasco",
        "araba": "País Vasco",
        "álava": "País Vasco",
        "alava": "País Vasco",
        # Madrid
        "madrid": "Comunidad de Madrid",
        # Extremadura
        "cáceres": "Extremadura",
        "caceres": "Extremadura",
        "badajoz": "Extremadura",
        # Andalucía
        "sevilla": "Andalucía",
        "huelva": "Andalucía",
        "cádiz": "Andalucía",
        "cadiz": "Andalucía",
        "córdoba": "Andalucía",
        "cordoba": "Andalucía",
        "granada": "Andalucía",
        "jaén": "Andalucía",
        "jaen": "Andalucía",
        "málaga": "Andalucía",
        "malaga": "Andalucía",
        "almería": "Andalucía",
        "almeria": "Andalucía",
        # Murcia
        "murcia": "Región de Murcia",
        # Islas Baleares
        "illes balears": "Islas Baleares",
        "islas baleares": "Islas Baleares",
        "baleares": "Islas Baleares",
        # Canarias
        "santa cruz de tenerife": "Canarias",
        "las palmas": "Canarias",
        # Ceuta y Melilla
        "ceuta": "Ceuta",
        "melilla": "Melilla",
        # Cantabria
        "cantabria": "Cantabria",
    }
    clave = mapa_provincias.get(provincia_normalizada)
    return clave if clave else "-"


def provincia_de_localidad(localidad: str) -> str | None:
    capital_a_provincia = {
        # Galicia
        "a coruña": "A Coruña",
        "coruña": "A Coruña",
        "la coruña": "A Coruña",
        "lugo": "Lugo",
        "ourense": "Ourense",
        "orense": "Ourense",
        "pontevedra": "Pontevedra",
        "vigo": "Pontevedra",
        # Asturias
        "oviedo": "Asturias",
        # Cantabria
        "santander": "Cantabria",
        # País Vasco
        "bilbao": "Bizkaia",
        "bilbo": "Bizkaia",
        "donostia": "Gipuzkoa",
        "san sebastián": "Gipuzkoa",
        "san sebastian": "Gipuzkoa",
        "donostia-san sebastian": "Gipuzkoa",
        "donostia-san sebastián": "Gipuzkoa",
        "vitoria": "Álava",
        "vitoria-gasteiz": "Álava",
        # Castilla y León
        "león": "León",
        "leon": "León",
        "zamora": "Zamora",
        "salamanca": "Salamanca",
        "valladolid": "Valladolid",
        "palencia": "Palencia",
        "burgos": "Burgos",
        "soria": "Soria",
        "segovia": "Segovia",
        "ávila": "Ávila",
        "avila": "Ávila",
        # Castilla-La Mancha
        "guadalajara": "Guadalajara",
        "cuenca": "Cuenca",
        "toledo": "Toledo",
        "ciudad real": "Ciudad Real",
        "albacete": "Albacete",
        # Madrid
        "madrid": "Madrid",
        # Andalucía
        "sevilla": "Sevilla",
        "cádiz": "Cádiz",
        "cadiz": "Cádiz",
        "huelva": "Huelva",
        "córdoba": "Córdoba",
        "cordoba": "Córdoba",
        "jaén": "Jaén",
        "jaen": "Jaén",
        "granada": "Granada",
        "almería": "Almería",
        "almeria": "Almería",
        "málaga": "Málaga",
        "malaga": "Málaga",
        # Extremadura
        "cáceres": "Cáceres",
        "caceres": "Cáceres",
        "badajoz": "Badajoz",
        "merida": "Cáceres",
        "mérida": "Cáceres",
        # Murcia
        "murcia": "Murcia",
        # Comunidad Valenciana
        "valencia": "Valencia",
        "castellón de la plana": "Castellón",
        "castelló": "Castellón",
        "castellon": "Castellón",
        "castellón": "Castellón",
        "alicante": "Alicante",
        "alacant": "Alicante",
        # Aragón
        "zaragoza": "Zaragoza",
        "huesca": "Huesca",
        "teruel": "Teruel",
        # Cataluña
        "barcelona": "Barcelona",
        "girona": "Girona",
        "gerona": "Girona",
        "lleida": "Lleida",
        "lerida": "Lleida",
        "lérida": "Lleida",
        "tarragona": "Tarragona",
        # Navarra
        "pamplona": "Navarra",
        "iruña": "Navarra",
        "iruñea": "Navarra",
        # La Rioja
        "logroño": "La Rioja",
        "logrono": "La Rioja",
        # Canarias
        "santa cruz de tenerife": "Santa Cruz de Tenerife",
        "tenerife": "Santa Cruz de Tenerife",
        "las palmas de gran canaria": "Las Palmas",
        "las palmas": "Las Palmas",
        # Baleares
        "palma": "Illes Balears",
        "palma de mallorca": "Illes Balears",
        # Ceuta y Melilla
        "ceuta": "Ceuta",
        "melilla": "Melilla",
    }

    clave = localidad.strip().lower()
    p = capital_a_provincia.get(clave)
    return p if p else "-"
