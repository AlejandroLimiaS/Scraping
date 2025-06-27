import logging
from extraccion.utiles.utiles_modelos import Fichaje, HistoricoFichajes
from insercion.utiles.utiles_db import (
    ejecutar_con_reintentos,
    normalizar_fecha,
    obtener_id_jugador,
)


async def cargar_historico_fichajes_desde_json(
    ruta="extraccion/historico/resultados/datos_historico_jugadores.json",
):
    import json

    try:
        with open(ruta, "r", encoding="utf-8") as f:
            datos = json.load(f)
            historicos = []
            for item in datos:
                fichajes = []
                for f in item.get("fichajes", []):
                    fichajes.append(
                        Fichaje(
                            temporada=f.get("temporada"),
                            fecha=f.get("fecha"),
                            club_anterior=tuple(f.get("club_anterior", (None, None))),
                            club_nuevo=tuple(f.get("club_nuevo", (None, None))),
                            valor=f.get("valor"),
                            coste=f.get("coste"),
                        )
                    )
                historicos.append(
                    HistoricoFichajes(
                        cod_jugador=item.get("cod_jugador"), fichajes=fichajes
                    )
                )
            return historicos
    except Exception as e:
        logging.error(f"Error cargando histórico fichajes JSON: {e}")
        return []


async def _insertar_fichaje(cur, id_jugador: int, fichaje: Fichaje):
    fecha = normalizar_fecha(fichaje.fecha)

    await cur.execute(
        """
                    SELECT 1 FROM historico
                    WHERE id_jugador = %s AND temporada = %s AND fecha = %s
                    AND nombre_equipo_antiguo = %s AND nombre_equipo_nuevo = %s
                    """,
        (
            int(id_jugador),
            fichaje.temporada,
            fecha,
            fichaje.club_anterior[0],
            fichaje.club_nuevo[0],
        ),
    )
    existe = await cur.fetchone()
    if existe:
        return True
    await cur.execute(
        """
                    INSERT INTO historico
                    (id_jugador, temporada, fecha, nombre_equipo_antiguo, nombre_equipo_nuevo, valor, coste)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
        (
            int(id_jugador),
            fichaje.temporada,
            fecha,
            fichaje.club_anterior[0],
            fichaje.club_nuevo[0],
            fichaje.valor,
            fichaje.coste,
        ),
    )

    return True


async def _insertar_historico_fichajes(pool, historico: HistoricoFichajes):
    resultados = []
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                id_jugador = await obtener_id_jugador(cur, historico.cod_jugador)
                for fichaje in historico.fichajes or []:
                    res = await _insertar_fichaje(cur, id_jugador, fichaje)
                    resultados.append(res)
                await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e
    return resultados if resultados else True


async def insertar_historico_fichajes(pool, historico: HistoricoFichajes):
    return await ejecutar_con_reintentos(
        _insertar_historico_fichajes,
        pool,
        historico,
        descripcion=f"Insertar historico {historico.cod_jugador}",
    )
