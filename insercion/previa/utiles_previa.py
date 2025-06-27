import json
from datetime import datetime
import logging

from extraccion.utiles.utiles_modelos import PartidoPrevia
from insercion.utiles.utiles_db import ejecutar_con_reintentos


def cargar_previas_json(
    ruta="extraccion/previa/resultados/datos_horarios_jornada.json",
) -> list[PartidoPrevia]:
    try:
        with open(ruta, "r", encoding="utf-8") as file:
            datos = json.load(file)
            previas = [PartidoPrevia(**p) for p in datos]
            logging.info(f"Se han cargado {len(previas)} previas de horarios.")
            return previas
    except Exception as e:
        logging.error(f"Error cargando previas JSON: {e}")
        return []


async def _insertar_horario(pool, previa: PartidoPrevia):
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                if ":" in previa.horario:
                    dt = datetime.strptime(previa.horario, "%d/%m/%y %H:%M")
                else:
                    dt = datetime.strptime(previa.horario, "%d/%m/%y")

                horario_mysql = dt.strftime("%Y-%m-%d %H:%M:%S")
                await cur.execute(
                    "SELECT id_partido FROM partido WHERE cod_partido = %s",
                    (int(previa.cod_partido),),
                )
                res = await cur.fetchone()
                if res:
                    await cur.execute(
                        "UPDATE partido SET fecha= %s WHERE id_partido = %s",
                        (horario_mysql, int(res[0])),
                    )
    return True


async def insertar_horario(pool, previa: PartidoPrevia):
    return await ejecutar_con_reintentos(
        _insertar_horario,
        pool,
        previa,
        descripcion=f"actualizar horario partido {previa.cod_partido}",
    )
