import json
import logging
from dataclasses import dataclass
from insercion.utiles.utiles_db import (
    ejecutar_con_reintentos,
    obtener_id_equipo,
    obtener_id_equipo_campo,
    obtener_id_liga,
)
from extraccion.utiles.utiles_modelos import PartidoCalendario


@dataclass
class PartidoCalendario_BD:
    id_liga: int
    id_local: int
    id_visitante: int
    id_campo: int
    cod_partido: int
    jornada: int = None
    enlace: str = None


def cargar_partidos_calendario(
    ruta="extraccion/calendarios/resultados/datos_partidos_calendario.json",
) -> list[PartidoCalendario]:
    try:
        with open(ruta, "r", encoding="utf-8") as file:
            partidos = [PartidoCalendario(**p) for p in json.load(file)]
            logging.info(f"Se han cargado {len(partidos)} partidos.")
            return partidos
    except FileNotFoundError:
        logging.error("El archivo datos_partidos_calendario.json no se encuentra.")
        return []


async def _insertar_partido(pool, partido: PartidoCalendario):
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                id_local, id_campo = await obtener_id_equipo_campo(
                    cur, partido.cod_local
                )
                id_visitante = await obtener_id_equipo(cur, partido.cod_visitante)
                id_liga = await obtener_id_liga(cur, partido.liga)

                await cur.execute(
                    "SELECT 1 FROM partido WHERE cod_partido = %s",
                    (int(partido.cod_partido),),
                )
                existe = await cur.fetchone()

                if existe:
                    await cur.execute(
                        """
                        UPDATE partido
                        SET id_liga = %s,
                            id_local = %s,
                            id_visitante = %s,
                            id_campo = %s,
                            jornada = %s,
                            enlace = %s
                        WHERE cod_partido = %s
                        """,
                        (
                            int(id_liga),
                            int(id_local),
                            int(id_visitante),
                            id_campo if id_campo is not None else None,
                            int(partido.jornada),
                            partido.enlace,
                            int(partido.cod_partido),
                        ),
                    )
                else:
                    await cur.execute(
                        """
                        INSERT INTO partido 
                        (id_liga, id_local, id_visitante, id_campo, cod_partido, jornada, enlace)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            int(id_liga),
                            int(id_local),
                            int(id_visitante),
                            int(id_campo) if id_campo is not None else None,
                            int(partido.cod_partido),
                            int(partido.jornada),
                            partido.enlace,
                        ),
                    )
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e
    return True


async def insertar_partido(pool, partido_bd: PartidoCalendario_BD):
    return await ejecutar_con_reintentos(
        _insertar_partido,
        pool,
        partido_bd,
        descripcion=f"partido {partido_bd.cod_partido}",
    )
