from dataclasses import dataclass
import json
import logging
from extraccion.utiles.utiles_modelos import Liga
from insercion.utiles.utiles_db import ejecutar_con_reintentos


@dataclass
class Liga_BD:
    cod_grupo: str
    temporada: str
    division: str
    grupo: str


def procesar_liga_insertar(liga: Liga):
    return Liga_BD(
        cod_grupo=liga.cod_grupo,
        temporada=liga.temporada,
        division=liga.division.upper(),
        grupo=liga.grupo.upper(),
    )


def cargar_ligas() -> list[Liga]:
    try:
        with open(
            "extraccion/ligas/resultados/datos_ligas.json", "r", encoding="utf-8"
        ) as file:
            ligas = [Liga(**liga) for liga in json.load(file)]
            logging.info(f"Se han cargado {len(ligas)} ligas.")
            return ligas
    except FileNotFoundError:
        logging.error("El archivo ligas.json no se encuentra.")
        return []


async def _insertar_liga(pool, liga_bd: Liga_BD):
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT 1 FROM liga 
                    WHERE cod_grupo = %s AND temporada = %s AND division = %s AND grupo = %s
                    """,
                    (
                        liga_bd.cod_grupo,
                        liga_bd.temporada,
                        liga_bd.division,
                        liga_bd.grupo,
                    ),
                )
                existe = await cur.fetchone()
                if existe:
                    logging.info(
                        f"La liga {liga_bd.cod_grupo} ya existe en la base de datos, no se insertará."
                    )
                    return False
                await cur.execute(
                    """
                    INSERT INTO liga (cod_grupo, temporada, division, grupo)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        liga_bd.cod_grupo,
                        liga_bd.temporada,
                        liga_bd.division,
                        liga_bd.grupo,
                    ),
                )
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e
    return True


async def insertar_liga(pool, liga_bd: Liga_BD):
    return await ejecutar_con_reintentos(
        _insertar_liga, pool, liga_bd, descripcion=f"liga {liga_bd.cod_grupo}"
    )
