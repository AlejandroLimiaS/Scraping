import json
import logging
from typing import List
from extraccion.utiles.utiles_modelos import Agente
from insercion.utiles.utiles_db import ejecutar_con_reintentos


async def _insertar_actualizar_representante(pool, agente: Agente):
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id_representante FROM representante WHERE nombre = %s",
                    (agente.nombre,),
                )
                existe = await cur.fetchone()
                if existe:
                    await cur.execute(
                        """
                        UPDATE representante 
                        SET telefono=%s, email=%s, pag_web=%s, direccion=%s
                        WHERE id_representante=%s
                        """,
                        (
                            agente.telefono,
                            agente.email,
                            agente.web,
                            agente.direccion,
                            existe[0],
                        ),
                    )
                    await conn.commit()
                    return True
                await cur.execute(
                    """
                    INSERT INTO representante (nombre, telefono, email, pag_web, direccion)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        agente.nombre,
                        agente.telefono,
                        agente.email,
                        agente.web,
                        agente.direccion,
                    ),
                )
                await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e
    return True


async def insertar_representante(pool, agente: Agente):
    return await ejecutar_con_reintentos(
        _insertar_actualizar_representante,
        pool,
        agente,
        descripcion=f"representante {agente.nombre}",
    )


def cargar_representantes_json(
    ruta="extraccion/representantes/resultados/datos_representantes.json",
) -> List[Agente]:
    try:
        with open(ruta, "r", encoding="utf-8") as file:
            agentes_raw = json.load(file)
            agentes = [Agente(**a) for a in agentes_raw]
            logging.info(f"Se han cargado {len(agentes)} representantes.")
            return agentes
    except FileNotFoundError:
        logging.error("El archivo datos_representantes.json no se encuentra.")
        return []
    except Exception as e:
        logging.error(f"Error cargando representantes JSON: {e}")
        return []
