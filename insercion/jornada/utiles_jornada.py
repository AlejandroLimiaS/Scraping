from dataclasses import dataclass
from datetime import datetime
import json
import logging
from typing import List, Optional
from extraccion.utiles.utiles_modelos import (
    PartidoJugado,
    Cambio,
    Amonestacion,
    Gol,
    PenaltiFallado,
)
from insercion.utiles.utiles_db import (
    ejecutar_con_reintentos,
)


@dataclass
class JugadorPartido:
    id_jugador: int
    id_partido: int
    id_equipo: int
    titular: int
    sustituido: int
    minutos: int
    goles: int
    goles_penalti: int
    amarillas: int
    rojas: int
    goles_falta: int
    goles_cabeza: int
    goles_propia: int
    goles_abajo: int
    asistencias: int
    penaltis_parados: int = 0
    desc_goles: Optional[str] = None
    desc_asist: Optional[str] = None
    razon_sustituido: Optional[str] = None


@dataclass
class JugadorLiga:
    id_jugador: int
    id_equipo: int
    id_liga: int
    convocatorias: int
    titularidades: int
    minutos: int
    goles: int
    goles_penalti: int
    amarillas: int
    rojas: int
    goles_falta: int
    goles_propia: int
    goles_abajo: int
    goles_cabeza: int
    porterias_imbatidas: int
    asistencias: int
    goles_sesenta: int
    goles_setentaycinco: int
    racha: int
    jugados: int
    penaltis_parados: int = 0
    desc_goles: Optional[str] = None
    desc_asist: Optional[str] = None


@dataclass
class equipoLiga:
    id_equipo: int = None
    id_liga: int = None
    puntos: int = None
    jugados: int = None
    victorias: int = None
    empates: int = None
    derrotas: int = None
    goles_favor: int = None
    goles_contra: int = None


async def cargar_partidos_desde_json(
    ruta="extraccion/jornada/resultados/datos_partidos_jornada.json",
) -> List[PartidoJugado]:
    try:
        with open(ruta, "r", encoding="utf-8") as f:
            datos = json.load(f)

        partidos = []
        for p in datos:
            cambios_local = [Cambio(**c) for c in p.get("cambios_local", [])]
            cambios_visitante = [Cambio(**c) for c in p.get("cambios_visitante", [])]
            amonestaciones_local = [
                Amonestacion(**a) for a in p.get("amonestaciones_local", [])
            ]
            amonestaciones_visitante = [
                Amonestacion(**a) for a in p.get("amonestaciones_visitante", [])
            ]
            goles_local_desc = [Gol(**g) for g in p.get("goles_local_desc", [])]
            goles_visitante_desc = [Gol(**g) for g in p.get("goles_visitante_desc", [])]

            penaltis_fallados = [
                PenaltiFallado(**pf) for pf in p.get("penaltis_fallados", [])
            ]

            partido = PartidoJugado(
                cod_partido=p.get("cod_partido"),
                goles_local=p.get("goles_local"),
                goles_visitante=p.get("goles_visitante"),
                cod_titulares_local=p.get("cod_titulares_local", []),
                cod_titulares_visitante=p.get("cod_titulares_visitante", []),
                cod_suplentes_local=p.get("cod_suplentes_local", []),
                cod_suplentes_visitante=p.get("cod_suplentes_visitante", []),
                cambios_local=cambios_local,
                cambios_visitante=cambios_visitante,
                amonestaciones_local=amonestaciones_local,
                amonestaciones_visitante=amonestaciones_visitante,
                goles_local_desc=goles_local_desc,
                goles_visitante_desc=goles_visitante_desc,
                fecha=p.get("fecha"),
                penaltis_fallados=penaltis_fallados,
            )
            partidos.append(partido)
        return partidos

    except Exception as e:
        import logging

        logging.error(f"Error cargando partidos JSON: {e}")
        return []


def suma_null(actual, nuevo):
    if actual is None:
        return nuevo
    if nuevo is None:
        return actual
    return actual + nuevo


async def insertar_actualizar_partido(
    cur, cod_partido, fecha, goles_local, goles_visitante
):
    if ":" in fecha:
        dt = datetime.strptime(fecha, "%d/%m/%y %H:%M")
    else:
        dt = datetime.strptime(fecha, "%d/%m/%y")

    horario_mysql = dt.strftime("%Y-%m-%d %H:%M:%S")

    await cur.execute(
        "SELECT id_partido FROM partido WHERE cod_partido = %s",
        (int(cod_partido),),
    )
    res = await cur.fetchone()
    if res:
        await cur.execute(
            """
                    UPDATE partido
                    SET fecha=%s, goles_local=%s, goles_visitante=%s, jugado=1
                    WHERE id_partido=%s
                    """,
            (horario_mysql, int(goles_local), int(goles_visitante), res[0]),
        )
        return True
    else:
        await cur.execute(
            """
                    INSERT INTO partido (cod_partido, fecha, goles_local, goles_visitante, jugado)
                    VALUES (%s, %s, %s, %s, 1)
                    """,
            (
                int(cod_partido),
                horario_mysql,
                int(goles_local),
                int(goles_visitante),
            ),
        )
        return True


async def insertar_actualizar_jugador_partido(cur, jp: JugadorPartido):

    await cur.execute(
        "SELECT 1 FROM jugador_partido WHERE id_jugador=%s AND id_partido=%s AND id_equipo=%s",
        (int(jp.id_jugador), int(jp.id_partido), int(jp.id_equipo)),
    )
    existe = await cur.fetchone()
    if existe:
        await cur.execute(
            """
                    UPDATE jugador_partido
                    SET titular=%s, sustituido=%s, razon_sustituido=%s, minutos=%s,
                        goles=%s, goles_penalti=%s, amarillas=%s, rojas=%s,
                        goles_falta=%s, goles_cabeza=%s, goles_propia=%s, goles_abajo=%s,
                        asistencias=%s, desc_goles=%s, desc_asist=%s, penaltis_parados=%s
                    WHERE id_jugador=%s AND id_partido=%s
                    """,
            (
                int(jp.titular),
                int(jp.sustituido),
                jp.razon_sustituido,
                int(jp.minutos),
                int(jp.goles),
                int(jp.goles_penalti),
                int(jp.amarillas),
                int(jp.rojas),
                int(jp.goles_falta),
                int(jp.goles_cabeza),
                int(jp.goles_propia),
                int(jp.goles_abajo),
                int(jp.asistencias),
                jp.desc_goles,
                jp.desc_asist,
                int(jp.penaltis_parados),
                int(jp.id_jugador),
                int(jp.id_partido),
            ),
        )
        return True
    else:
        await cur.execute(
            """
                    INSERT INTO jugador_partido
                    (id_jugador, id_partido, id_equipo, titular, sustituido, razon_sustituido, minutos,
                    goles, goles_penalti, amarillas, rojas, goles_falta, goles_cabeza,
                    goles_propia, goles_abajo, asistencias, desc_goles, desc_asist, penaltis_parados)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
            (
                int(jp.id_jugador),
                int(jp.id_partido),
                int(jp.id_equipo),
                int(jp.titular),
                int(jp.sustituido),
                jp.razon_sustituido,
                int(jp.minutos),
                int(jp.goles),
                int(jp.goles_penalti),
                int(jp.amarillas),
                int(jp.rojas),
                int(jp.goles_falta),
                int(jp.goles_cabeza),
                int(jp.goles_propia),
                int(jp.goles_abajo),
                int(jp.asistencias),
                jp.desc_goles,
                jp.desc_asist,
                int(jp.penaltis_parados),
            ),
        )
        return True


async def insertar_actualizar_jugador_liga(cur, stats: JugadorLiga):

    await cur.execute(
        """
                SELECT convocatorias, titularidades, minutos, goles, goles_penalti, amarillas, rojas,
                    goles_falta, goles_propia, goles_abajo, goles_cabeza, porterias_imbatidas, asistencias,
                    desc_goles, desc_asist, goles_sesenta, goles_setentaycinco, racha, penaltis_parados, jugados
                FROM jugador_liga
                WHERE id_jugador=%s AND id_liga=%s AND id_equipo=%s
                """,
        (int(stats.id_jugador), int(stats.id_liga), int(stats.id_equipo)),
    )
    existente = await cur.fetchone()
    if existente:

        convocatorias = suma_null(existente[0], int(stats.convocatorias))
        titularidades = suma_null(existente[1], int(stats.titularidades))
        minutos = suma_null(existente[2], int(stats.minutos))
        goles = suma_null(existente[3], int(stats.goles))
        goles_penalti = suma_null(existente[4], int(stats.goles_penalti))
        amarillas = suma_null(existente[5], int(stats.amarillas))
        rojas = suma_null(existente[6], int(stats.rojas))
        goles_falta = suma_null(existente[7], int(stats.goles_falta))
        goles_propia = suma_null(existente[8], int(stats.goles_propia))
        goles_abajo = suma_null(existente[9], int(stats.goles_abajo))
        goles_cabeza = suma_null(existente[10], int(stats.goles_cabeza))
        porterias_imbatidas = suma_null(existente[11], int(stats.porterias_imbatidas))
        asistencias = suma_null(existente[12], int(stats.asistencias))

        desc_goles_bd = existente[13] or ""
        desc_asist_bd = existente[14] or ""
        desc_goles = (
            f"{desc_goles_bd},{stats.desc_goles}" if stats.desc_goles else desc_goles_bd
        )
        desc_asist = (
            f"{desc_asist_bd},{stats.desc_asist}" if stats.desc_asist else desc_asist_bd
        )
        goles_sesenta = suma_null(existente[15], int(stats.goles_sesenta))
        goles_setentaycinco = suma_null(existente[16], int(stats.goles_setentaycinco))
        racha_actual = 0
        if stats.racha == 1:
            racha_actual = suma_null(existente[17], 1)
        elif existente[17] > racha_actual and existente[17] >= 2:
            await cur.execute(
                """ 
                        INSERT INTO jugador_racha (id_jugador, id_liga, racha) VALUES (%s, %s, %s)
                        """,
                (int(stats.id_jugador), int(stats.id_liga), int(existente[17])),
            )
        penaltis_parados = suma_null(existente[18], int(stats.penaltis_parados))
        jugados = suma_null(existente[19], int(stats.jugados))
        await cur.execute(
            """
                    UPDATE jugador_liga SET
                        convocatorias=%s, titularidades=%s, minutos=%s, goles=%s,
                        goles_penalti=%s, amarillas=%s, rojas=%s, goles_falta=%s,
                        goles_propia=%s, goles_abajo=%s, goles_cabeza=%s,
                        porterias_imbatidas=%s, asistencias=%s, desc_goles=%s, desc_asist=%s, goles_sesenta=%s, goles_setentaycinco=%s, racha=%s, penaltis_parados=%s, jugados=%s
                    WHERE id_jugador=%s AND id_liga=%s AND id_equipo=%s
                    """,
            (
                convocatorias,
                titularidades,
                minutos,
                goles,
                goles_penalti,
                amarillas,
                rojas,
                goles_falta,
                goles_propia,
                goles_abajo,
                goles_cabeza,
                porterias_imbatidas,
                asistencias,
                desc_goles,
                desc_asist,
                goles_sesenta,
                goles_setentaycinco,
                int(racha_actual),
                int(penaltis_parados),
                int(jugados),
                int(stats.id_jugador),
                int(stats.id_liga),
                int(stats.id_equipo),
            ),
        )
        return True
    else:
        await cur.execute(
            """
                    INSERT INTO jugador_liga
                    (id_jugador, id_liga, id_equipo, convocatorias, titularidades, minutos,
                     goles, goles_penalti, amarillas, rojas, goles_falta, goles_propia,
                     goles_abajo, goles_cabeza, porterias_imbatidas, asistencias, desc_goles, desc_asist, goles_sesenta, goles_setentaycinco, racha, penaltis_parados, jugados)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
            (
                int(stats.id_jugador),
                int(stats.id_liga),
                int(stats.id_equipo),
                int(stats.convocatorias),
                int(stats.titularidades),
                int(stats.minutos),
                int(stats.goles),
                int(stats.goles_penalti),
                int(stats.amarillas),
                int(stats.rojas),
                int(stats.goles_falta),
                int(stats.goles_propia),
                int(stats.goles_abajo),
                int(stats.goles_cabeza),
                int(stats.porterias_imbatidas),
                int(stats.asistencias),
                stats.desc_goles,
                stats.desc_asist,
                int(stats.goles_sesenta),
                int(stats.goles_setentaycinco),
                int(stats.racha),
                int(stats.penaltis_parados),
                int(stats.jugados),
            ),
        )
        return True


async def insertar_actualizar_equipo_liga(cur, equipo_liga: equipoLiga):
    await cur.execute(
        """
                SELECT puntos, jugados, victorias, empates, derrotas, goles_favor, goles_contra
                FROM equipo_liga
                WHERE id_equipo=%s AND id_liga=%s
                """,
        (int(equipo_liga.id_equipo), int(equipo_liga.id_liga)),
    )
    existente = await cur.fetchone()
    if existente:
        puntos = suma_null(existente[0], int(equipo_liga.puntos))
        jugados = suma_null(existente[1], int(equipo_liga.jugados))
        victorias = suma_null(existente[2], int(equipo_liga.victorias))
        empates = suma_null(existente[3], int(equipo_liga.empates))
        derrotas = suma_null(existente[4], int(equipo_liga.derrotas))
        goles_favor = suma_null(existente[5], int(equipo_liga.goles_favor))
        goles_contra = suma_null(existente[6], int(equipo_liga.goles_contra))

        await cur.execute(
            """
                    UPDATE equipo_liga SET puntos=%s, jugados=%s, victorias=%s, empates=%s,
                    derrotas=%s, goles_favor=%s, goles_contra=%s
                    WHERE id_equipo=%s AND id_liga=%s
                    """,
            (
                puntos,
                jugados,
                victorias,
                empates,
                derrotas,
                goles_favor,
                goles_contra,
                int(equipo_liga.id_equipo),
                int(equipo_liga.id_liga),
            ),
        )
        return True
    else:
        await cur.execute(
            """
                    INSERT INTO equipo_liga
                    (id_equipo, id_liga, puntos, jugados, victorias, empates, derrotas, goles_favor, goles_contra)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
            (
                int(equipo_liga.id_equipo),
                int(equipo_liga.id_liga),
                int(equipo_liga.puntos),
                int(equipo_liga.jugados),
                int(equipo_liga.victorias),
                int(equipo_liga.empates),
                int(equipo_liga.derrotas),
                int(equipo_liga.goles_favor),
                int(equipo_liga.goles_contra),
            ),
        )
        return True


async def _actualizar_jornada_liga(pool, id_liga: int, nueva_jornada: int):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT ultima_jornada FROM liga WHERE id_liga = %s",
                (int(id_liga),),
            )
            res = await cur.fetchone()
            if res is None:
                return False
            ultima_jornada = res[0]
            if int(nueva_jornada) > ultima_jornada:
                await cur.execute(
                    "UPDATE liga SET ultima_jornada = %s WHERE id_liga = %s",
                    (int(nueva_jornada), int(id_liga)),
                )
        await conn.commit()
        return True


async def actualizar_jornada_liga(pool, id_liga: int, nueva_jornada: int):
    return await ejecutar_con_reintentos(
        _actualizar_jornada_liga,
        pool,
        id_liga,
        nueva_jornada,
        descripcion=f"Actualizar jornada liga {id_liga} a {nueva_jornada}",
    )
