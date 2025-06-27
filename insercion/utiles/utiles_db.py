import asyncio
import logging
from typing import Any, Callable
import aiomysql

from datetime import datetime
from typing import Optional


MAX_INTENTOS = 5
SEMAPHORE = 100
semaphore_db = asyncio.Semaphore(SEMAPHORE)
TEMPORADA_ACTUAL = "2024/2025"


async def crear_pool_bd_async(user, password, db):
    try:
        pool = await aiomysql.create_pool(
            host="185.253.155.137",
            port=3306,
            user=user,
            password=password,
            db=db,
            minsize=1,
            maxsize=101,
            charset="utf8mb4",
            autocommit=False,
            connect_timeout=10,
        )
        return pool
    except Exception as e:
        logging.error(f"Error al crear el pool de conexión: {e}")
        raise


async def ejecutar_con_reintentos(
    funcion: Callable[..., Any],
    *args,
    descripcion: str = "",
    max_intentos: int = MAX_INTENTOS,
) -> Any:
    intentos = 0
    while intentos < max_intentos:
        try:
            async with semaphore_db:
                return await funcion(*args)
        except Exception as e:
            intentos += 1
            logging.warning(f"Error en {descripcion} (intento {intentos}): {e}")
            if intentos >= max_intentos:
                logging.error(f"Falló {descripcion} tras {max_intentos} intentos.")
                return False


def normalizar_fecha(fecha_str: Optional[str]) -> Optional[str]:
    if not fecha_str or "-" in fecha_str:
        return None
    try:
        dt = datetime.strptime(fecha_str, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


async def obtener_id_liga(cur, liga_str: str) -> Optional[int]:
    if not liga_str:
        return None
    partes = [p.strip() for p in liga_str.split("-")]
    division = partes[0].upper() if partes else liga_str
    grupo = partes[1].upper() if len(partes) > 1 else None
    await cur.execute(
        "SELECT id_liga FROM liga WHERE division = %s AND grupo = %s AND temporada = %s",
        (
            division,
            grupo,
            TEMPORADA_ACTUAL,
        ),
    )
    res = await cur.fetchone()
    return res[0] if res else None


async def obtener_id_equipo(cur, cod_equipo: str) -> Optional[int]:
    if not cod_equipo:
        return None

    await cur.execute(
        "SELECT id_equipo FROM equipo WHERE cod_equipo = %s", (int(cod_equipo),)
    )
    res = await cur.fetchone()
    return int(res[0]) if res else None


async def obtener_id_equipo_campo(cur, cod_equipo: str) -> Optional[int]:
    if not cod_equipo:
        return None

    await cur.execute(
        "SELECT id_equipo, id_campo FROM equipo WHERE cod_equipo = %s",
        (int(cod_equipo),),
    )
    res = await cur.fetchone()
    if res:
        return int(res[0]), res[1]
    else:
        return None, None


async def obtener_id_jugador(cur, cod_jugador: str) -> Optional[int]:
    if not cod_jugador:
        return None

    await cur.execute(
        "SELECT id_jugador FROM jugador WHERE cod_jugador = %s",
        (int(cod_jugador),),
    )
    res = await cur.fetchone()
    return int(res[0]) if res else None


async def obtener_id_representante(cur, nombre_agente: str) -> Optional[int]:
    if not nombre_agente:
        return None

    await cur.execute(
        "SELECT id_representante FROM representante WHERE nombre = %s",
        (nombre_agente,),
    )
    res = await cur.fetchone()
    if res:
        return res[0]
    await cur.execute(
        "INSERT INTO representante (nombre, telefono, email, pag_web, direccion) VALUES (%s,%s,%s,%s,%s)",
        (nombre_agente, "-", "-", "-", "- - - -"),
    )
    return cur.lastrowid


async def obtener_id_partido_liga_equipos(cur, cod_partido: str) -> Optional[int]:
    if not cod_partido:
        return None

    await cur.execute(
        "SELECT id_partido, id_liga, id_local, id_visitante, jornada, jugado FROM partido WHERE cod_partido = %s",
        (int(cod_partido),),
    )
    res = await cur.fetchone()
    return (
        int(res[0]) if res else None,
        int(res[1]) if res else None,
        int(res[2]) if res else None,
        int(res[3]) if res else None,
        int(res[4]) if res else None,
        int(res[5]) if res else None,
    )


async def _obtener_jornadas_ligas(pool, division: str, grupo: str):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:

            await cur.execute(
                "SELECT id_liga, ultima_jornada FROM liga WHERE division = %s AND grupo = %s AND temporada = %s",
                (
                    division.upper(),
                    grupo.upper(),
                    TEMPORADA_ACTUAL,
                ),
            )
            res = await cur.fetchone()
            return res[0] if res else None, res[1] if res else None


async def obtener_jornada_liga(pool, division: str, grupo: str):
    return await ejecutar_con_reintentos(
        _obtener_jornadas_ligas,
        pool,
        division,
        grupo,
        descripcion=f"Recuperar jornada liga {division}-{grupo}",
    )


async def _obtener_enlaces_partidos(pool, id_liga: int, ultima_jornada: int):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:

            await cur.execute(
                "SELECT enlace FROM partido WHERE id_liga = %s AND jornada <= %s AND jugado = 0",
                (
                    int(id_liga),
                    int(ultima_jornada),
                ),
            )
            res = await cur.fetchall()
            return res if res else []


async def obtener_enlaces_partidos(pool, id_liga: int, ultima_jornada: int):
    return await ejecutar_con_reintentos(
        _obtener_enlaces_partidos,
        pool,
        id_liga,
        ultima_jornada,
        descripcion=f"Recuperar partidos {id_liga} J{ultima_jornada}",
    )


async def obtener_cod_equipo(cur, id_equipo: int):
    if not id_equipo:
        return None
    await cur.execute(
        "SELECT cod_equipo FROM equipo WHERE id_equipo = %s",
        (int(id_equipo),),
    )
    res = await cur.fetchone()
    return res[0] if res else None


async def obtener_str_liga(cur, id_liga: int):
    if not id_liga:
        return None
    await cur.execute(
        "SELECT division, grupo FROM liga WHERE id_liga = %s",
        (int(id_liga),),
    )
    res = await cur.fetchone()
    if res:
        division, grupo = res
        return f"{division} - {grupo}"
    return None
