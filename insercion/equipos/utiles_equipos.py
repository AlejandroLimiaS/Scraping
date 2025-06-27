from dataclasses import dataclass
import json
import logging
from typing import Optional

from extraccion.utiles.utiles_modelos import Club_Equipo
from insercion.utiles.utiles_db import ejecutar_con_reintentos, obtener_id_liga
from insercion.utiles.utiles_localidad import (
    limpiar_localidad,
    obtener_o_insertar_localidad,
)


@dataclass
class Equipo_BD:
    id_club: int
    nombre: str
    cod_equipo: int


@dataclass
class Club_BD:
    nombre: str
    cod_club: int
    id_localidad: Optional[int] = None


def cargar_equipos_json(
    ruta="extraccion/equipos/resultados/datos_equipos.json",
) -> list[Club_Equipo]:
    try:
        with open(ruta, "r", encoding="utf-8") as file:
            equipos_raw = json.load(file)
            equipos = [Club_Equipo(**e) for e in equipos_raw]
            logging.info(f"Se han cargado {len(equipos)} equipos.")
            return equipos
    except FileNotFoundError:
        logging.error("El archivo datos_equipos.json no se encuentra.")
        return []


async def insertar_club(
    cur, equipo_raw: Club_Equipo, localidades_ids: dict = None, paises_ids: dict = None
) -> int:
    localidad = await limpiar_localidad(cur, equipo_raw.localidad)
    if localidades_ids and localidad.nombre in localidades_ids:
        id_localidad = localidades_ids[localidad.nombre]
    else:
        id_localidad = await obtener_o_insertar_localidad(cur, localidad, paises_ids)
    club = Club_BD(
        nombre=equipo_raw.primer_equipo,
        cod_club=int(equipo_raw.cod_club),
        id_localidad=int(id_localidad),
    )
    await cur.execute(
        "SELECT id_club FROM club WHERE cod_club = %s", (int(club.cod_club),)
    )
    existe = await cur.fetchone()
    if existe:
        await cur.execute(
            "UPDATE club SET nombre=%s, id_localidad=%s ,cod_club=%s WHERE id_club=%s",
            (
                club.nombre,
                int(club.id_localidad),
                int(club.cod_club),
                int(existe[0]),
            ),
        )

        return existe[0]
    await cur.execute(
        "INSERT INTO club (nombre, cod_club, id_localidad) VALUES (%s, %s, %s)",
        (club.nombre, int(club.cod_club), int(club.id_localidad)),
    )

    return cur.lastrowid


async def _insertar_equipo(cur, equipo: Equipo_BD):
    await cur.execute(
        "SELECT id_equipo FROM equipo WHERE cod_equipo = %s",
        (int(equipo.cod_equipo),),
    )
    existe = await cur.fetchone()
    if existe:
        await cur.execute(
            """
                    UPDATE equipo SET id_club=%s, cod_equipo=%s, nombre=%s
                    WHERE id_equipo=%s
                    """,
            (
                int(equipo.id_club),
                int(equipo.cod_equipo),
                equipo.nombre,
                int(existe[0]),
            ),
        )

        return existe[0]
    await cur.execute(
        """
                INSERT INTO equipo (id_club, nombre, cod_equipo)
                VALUES (%s, %s, %s)
                """,
        (
            int(equipo.id_club),
            equipo.nombre,
            int(equipo.cod_equipo),
        ),
    )
    return cur.lastrowid


async def _insertar_equipo_liga(cur, id_equipo: int, id_liga: int):
    await cur.execute(
        "SELECT 1 FROM equipo_liga WHERE id_equipo = %s AND id_liga = %s",
        (int(id_equipo), int(id_liga)),
    )
    existe = await cur.fetchone()
    if existe:
        return True
    await cur.execute(
        "INSERT INTO equipo_liga (id_equipo, id_liga) VALUES (%s, %s)",
        (int(id_equipo), int(id_liga)),
    )
    return True


async def insertar_clubequipo(
    pool,
    equipo_raw: Club_Equipo,
    localidades_ids: dict,
    paises_ids: dict,
) -> bool:
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                id_club = await insertar_club(
                    cur, equipo_raw, localidades_ids, paises_ids
                )
                equipo = Equipo_BD(
                    id_club=int(id_club),
                    nombre=equipo_raw.nombre,
                    cod_equipo=int(equipo_raw.cod_equipo),
                )
                id_equipo = await _insertar_equipo(cur, equipo)
                id_liga = await obtener_id_liga(cur, equipo_raw.liga)
                await _insertar_equipo_liga(cur, id_equipo, id_liga)
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e
    return True


async def insertar_equipo(pool, equipo_raw, localidades_ids, paises_ids):
    return await ejecutar_con_reintentos(
        insertar_clubequipo,
        pool,
        equipo_raw,
        localidades_ids,
        paises_ids,
        descripcion=f"Introducir {equipo_raw.nombre}",
    )
