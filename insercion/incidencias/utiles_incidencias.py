from dataclasses import dataclass
from datetime import datetime
import json
import logging

from extraccion.utiles.utiles_modelos import Incidencia
from insercion.utiles.utiles_db import ejecutar_con_reintentos, obtener_id_jugador


@dataclass
class Incidencia_BD:
    id_jugador: int
    fecha: str
    observacion: str


def cargar_incidencias_desde_json(
    ruta="extraccion/incidencias/resultados/datos_incidencias_jugadores.json",
) -> list[Incidencia]:
    try:
        with open(ruta, "r", encoding="utf-8") as f:
            datos = json.load(f)
            incidencias = [Incidencia(**item) for item in datos]
            logging.info(f"Cargadas {len(incidencias)} incidencias desde JSON.")
            return incidencias
    except Exception as e:
        logging.error(f"Error cargando incidencias JSON: {e}")
        return []


async def _borrar_incidencias_usuario_null(pool):
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM incidencia WHERE id_usuario IS NULL")
                await conn.commit()
        except Exception as e:
            await conn.rollback()
            logging.error(f"Error al borrar incidencias con id_usuario NULL: {e}")
            raise e
    return True


async def borrar_incidencias_usuario_null(pool):
    return await ejecutar_con_reintentos(
        _borrar_incidencias_usuario_null,
        pool,
        descripcion="borrar incidencias con id_usuario NULL",
    )


async def _insertar_incidencia(pool, incidencia: Incidencia):
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                id_jugador = await obtener_id_jugador(cur, incidencia.cod_jugador)
                if not id_jugador:
                    logging.warning(
                        f"Jugador con código {incidencia.cod_jugador} no encontrado, incidencia omitida."
                    )
                    return False

                incidencia_bd = Incidencia_BD(
                    id_jugador=int(id_jugador),
                    fecha=datetime.now().strftime("%Y-%m-%d"),
                    observacion=incidencia.incidencia,
                )

                await cur.execute(
                    """
                    INSERT INTO incidencia (id_jugador, fecha, observacion)
                    VALUES (%s, %s, %s)
                    """,
                    (
                        int(incidencia_bd.id_jugador),
                        incidencia_bd.fecha,
                        incidencia_bd.observacion,
                    ),
                )
                await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e
    return True


async def insertar_incidencia(pool, incidencia: Incidencia):
    return await ejecutar_con_reintentos(
        _insertar_incidencia,
        pool,
        incidencia,
        descripcion=f"insertar incidencia jugador_id={incidencia.cod_jugador}",
    )
