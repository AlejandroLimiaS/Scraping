import json
import logging


from extraccion.utiles.utiles_modelos import JugadorValorMercado, ValorMercado
from insercion.utiles.utiles_db import (
    ejecutar_con_reintentos,
    normalizar_fecha,
    obtener_id_jugador,
)


def cargar_valores_mercado_json(
    ruta="extraccion/incidencias/resultados/datos_valores_mercado_jugadores.json",
) -> list[JugadorValorMercado]:
    try:
        with open(ruta, "r", encoding="utf-8") as file:
            datos = json.load(file)
            jugadores_valor = []
            for jvm in datos:
                actual = jvm.get("valor_mercado_actual", {})
                maximo = jvm.get("valor_mercado_maximo", {})
                jugador_valor = JugadorValorMercado(
                    cod_jugador=jvm.get("cod_jugador"),
                    valor_mercado_actual=ValorMercado(
                        valor=actual.get("valor"),
                        fecha=actual.get("fecha"),
                    ),
                    valor_mercado_maximo=ValorMercado(
                        valor=maximo.get("valor"),
                        fecha=maximo.get("fecha"),
                    ),
                )
                jugadores_valor.append(jugador_valor)
            logging.info(f"Cargados {len(jugadores_valor)} valores de mercado.")
            return jugadores_valor
    except FileNotFoundError:
        logging.error("Archivo datos_valores_mercado_jugadores.json no encontrado.")
        return []
    except Exception as e:
        logging.error(f"Error cargando valores de mercado: {e}")
        return []


async def _actualizar_valor_mercado(
    pool, cod_jugador, valor_actual, fecha_actual, valor_max, fecha_max
):
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                id_jugador = await obtener_id_jugador(cur, int(cod_jugador))
                if not id_jugador:
                    logging.warning(
                        f"Jugador con código {cod_jugador} no encontrado, actualización omitida."
                    )
                    return False
                await cur.execute(
                    """
                        UPDATE jugador 
                        SET valor_mercado_actual = %s, valor_mercado_maximo = %s, fecha_valor_mercado_actual = %s, fecha_valor_mercado_maximo = %s
                        WHERE id_jugador = %s
                        """,
                    (valor_actual, valor_max, fecha_actual, fecha_max, int(id_jugador)),
                )
                await conn.commit()

        except Exception as e:
            await conn.rollback()
            logging.error(
                f"Error al actualizar valor de mercado del jugador {cod_jugador}: {e}"
            )
            raise e
    return True


def convertir_valor_mercado(valor_str: str) -> float:
    if not valor_str or valor_str.strip() == "-":
        return 0.0

    valor_str = valor_str.lower()
    multiplicador = 1

    if "mill" in valor_str:
        multiplicador = 1_000_000
    elif "mil" in valor_str:
        multiplicador = 1_000

    valor_str = (
        valor_str.replace("€", "")
        .replace("mill", "")
        .replace("mil", "")
        .replace(",", ".")
        .strip()
    )

    if valor_str.endswith("."):
        valor_str = valor_str[:-1].strip()

    try:
        return float(valor_str) * multiplicador
    except ValueError:
        logging.warning(f"No se pudo convertir el valor: {valor_str}")
        return 0.0


async def actualizar_valores_mercado(pool, jugador_valor: JugadorValorMercado):
    valor_actual = convertir_valor_mercado(jugador_valor.valor_mercado_actual.valor)
    valor_max = convertir_valor_mercado(jugador_valor.valor_mercado_maximo.valor)
    fecha_actual = normalizar_fecha(jugador_valor.valor_mercado_actual.fecha)
    fecha_max = normalizar_fecha(jugador_valor.valor_mercado_maximo.fecha)

    return await ejecutar_con_reintentos(
        _actualizar_valor_mercado,
        pool,
        jugador_valor.cod_jugador,
        valor_actual,
        fecha_actual,
        valor_max,
        fecha_max,
        descripcion=f"actualizar valor mercado cod_jugador={jugador_valor.cod_jugador}",
    )
