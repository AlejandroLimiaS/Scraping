import asyncio
from collections import defaultdict
import logging
import os
import time
from datetime import datetime
from pathlib import Path
import sys


sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
nombre_carpeta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")
os.makedirs(nombre_carpeta, exist_ok=True)
timestamp = datetime.now().strftime("%d-%m-%y_%H-%M")
ruta_log = f"{nombre_carpeta}/insercion_jornada_{timestamp}.log"

logging.basicConfig(
    filename=ruta_log,
    level=logging.INFO,
    format="%(message)s",
)
from extraccion.incidencias.scrape_incidencias import scrapear_valores_mercado_jugadores
from insercion.incidencias.utiles_valores_mercado import actualizar_valores_mercado

from extraccion.historico.scrape_historico import scrapear_historico_jugadores
from extraccion.representantes.scrape_representantes import scrapear_agentes
from insercion.jugadores.utiles_jugadores import (
    Jugador_BD,
    insertar_o_actualizar_jugador,
)
from insercion.representantes.insercion_representantes import procesar_representantes
from extraccion.jugadores.scrape_jugadores import (
    scrapear_jugadores,
)
from insercion.jugadores.insercion_jugadores import procesar_jugadores
from insercion.jornada.utiles_jornada import (
    JugadorLiga,
    JugadorPartido,
    actualizar_jornada_liga,
    cargar_partidos_desde_json,
    equipoLiga,
    insertar_actualizar_equipo_liga,
    insertar_actualizar_jugador_liga,
    insertar_actualizar_jugador_partido,
    insertar_actualizar_partido,
)
from insercion.utiles.utiles_db import (
    crear_pool_bd_async,
    ejecutar_con_reintentos,
    obtener_cod_equipo,
    obtener_id_jugador,
    obtener_id_partido_liga_equipos,
    obtener_str_liga,
)
from extraccion.utiles.utiles_salida import print_cabecera


USER = os.getenv("MYSQL_USER")
if USER is None:
    raise ValueError("MYSQL_USER environment variable is not set.")
PASSWORD = os.getenv("MYSQL_PASSWORD")
if PASSWORD is None:
    raise ValueError("MYSQL_PASSWORD environment variable is not set.")


async def _procesar_partido(pool, partido):
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                enlaces = []
                cod_partido = int(partido.cod_partido)
                fecha = partido.fecha
                goles_local = int(partido.goles_local or 0)
                goles_visitante = int(partido.goles_visitante or 0)
                id_partido, id_liga, id_local, id_visitante, jornada, jugado = (
                    await obtener_id_partido_liga_equipos(cur, cod_partido)
                )
                if jugado:
                    logging.info(
                        f"Partido {cod_partido} ya procesado anteriormente. Liga: {id_liga}, Jornada: {jornada}, Local: {id_local}, Visitante: {id_visitante}"
                    )
                    return [], (id_liga, jornada)

                valor_jornada = (id_liga, jornada)

                puntos_local = 0
                puntos_visitante = 0
                if goles_local > goles_visitante:
                    puntos_local = 3
                elif goles_local == goles_visitante:
                    puntos_local = puntos_visitante = 1
                else:
                    puntos_visitante = 3

                equipo_liga_local = equipoLiga(
                    id_equipo=id_local,
                    id_liga=id_liga,
                    puntos=puntos_local,
                    jugados=1,
                    victorias=1 if puntos_local == 3 else 0,
                    empates=1 if puntos_local == 1 else 0,
                    derrotas=1 if puntos_local == 0 else 0,
                    goles_favor=goles_local,
                    goles_contra=goles_visitante,
                )
                await insertar_actualizar_equipo_liga(cur, equipo_liga_local)

                equipo_liga_visitante = equipoLiga(
                    id_equipo=id_visitante,
                    id_liga=id_liga,
                    puntos=puntos_visitante,
                    jugados=1,
                    victorias=1 if puntos_visitante == 3 else 0,
                    empates=1 if puntos_visitante == 1 else 0,
                    derrotas=1 if puntos_visitante == 0 else 0,
                    goles_favor=goles_visitante,
                    goles_contra=goles_local,
                )
                await insertar_actualizar_equipo_liga(cur, equipo_liga_visitante)

                cambios_local = {
                    c.cod_entra: c.minuto for c in (partido.cambios_local or [])
                }
                cambios_visitante = {
                    c.cod_entra: c.minuto for c in (partido.cambios_visitante or [])
                }
                cambios_local_sustituido = {
                    c.cod_fuera: (c.minuto, c.desc)
                    for c in (partido.cambios_local or [])
                }
                cambios_visitante_sustituido = {
                    c.cod_fuera: (c.minuto, c.desc)
                    for c in (partido.cambios_visitante or [])
                }
                amonestaciones_local = defaultdict(int)
                amonestaciones_visitante = defaultdict(int)
                rojas_local = defaultdict(int)
                rojas_visitante = defaultdict(int)
                for a in partido.amonestaciones_local or []:
                    if a.amarilla:
                        amonestaciones_local[a.cod_amonestado] += 1
                    if a.roja:
                        rojas_local[a.cod_amonestado] += 1
                for a in partido.amonestaciones_visitante or []:
                    if a.amarilla:
                        amonestaciones_visitante[a.cod_amonestado] += 1
                    if a.roja:
                        rojas_visitante[a.cod_amonestado] += 1

                jugadores_local = (partido.cod_titulares_local or []) + (
                    partido.cod_suplentes_local or []
                )
                jugadores_visitante = (partido.cod_titulares_visitante or []) + (
                    partido.cod_suplentes_visitante or []
                )

                for cod_jugador_t in jugadores_local:
                    cod_jugador, enlace = cod_jugador_t
                    id_jugador = await obtener_id_jugador(cur, cod_jugador)

                    if id_jugador is None:

                        cod_local = await obtener_cod_equipo(cur, id_local)
                        liga_str = await obtener_str_liga(cur, id_liga)
                        enlaces.append(
                            (
                                "https://www.transfermarkt.es" + enlace,
                                cod_local,
                                liga_str,
                            )
                        )
                        j: Jugador_BD = Jugador_BD(
                            cod_jugador=cod_jugador,
                            nombre="-",
                            ano=0,
                            fecha_nacimiento=None,
                            altura=None,
                            lateralidad="-",
                            apodo="-",
                            id_localidad=1,
                            id_representante=1,
                            gallego=0,
                        )
                        id_jugador = await insertar_o_actualizar_jugador(cur, j)

                    sustituido = 1 if cod_jugador in cambios_local_sustituido else 0
                    minuto_salida, razon_sustituido = cambios_local_sustituido.get(
                        cod_jugador, (90, "")
                    )
                    if cod_jugador in (cj[0] for cj in partido.cod_titulares_local):
                        titular = 1
                        minutos = minuto_salida
                    else:
                        minuto_entrada = cambios_local.get(cod_jugador, 90)
                        minutos = minuto_salida - minuto_entrada
                        titular = 0

                    goles_jugador = [
                        g
                        for g in (partido.goles_local_desc or [])
                        if g.cod_goleador == cod_jugador
                    ]
                    asistencias_jugador = [
                        g
                        for g in (partido.goles_local_desc or [])
                        if g.cod_asistente == cod_jugador
                    ]
                    penaltis_parados_jugador = [
                        p
                        for p in (partido.penaltis_fallados or [])
                        if p.cod_portero == cod_jugador
                    ]
                    penaltis_parados = len(penaltis_parados_jugador)
                    goles = len(goles_jugador)
                    racha = 0
                    if goles > 0:
                        racha = 1
                    goles_penalti = sum(
                        1 for g in goles_jugador if "penalti" in (g.desc or "").lower()
                    )
                    goles_falta = sum(
                        1
                        for g in goles_jugador
                        if "tiro libre" in (g.desc or "").lower()
                    )
                    goles_cabeza = sum(
                        1 for g in goles_jugador if "cabeza" in (g.desc or "").lower()
                    )

                    goles_abajo = 0
                    goles_sesenta = 0
                    goles_setentaycinco = 0
                    for gol in goles_jugador:
                        if gol.minuto >= 75:
                            goles_sesenta += 1
                            goles_setentaycinco += 1
                        elif gol.minuto >= 60:
                            goles_sesenta += 1
                        nums = gol.resultado_gol.split(":", 1)
                        num1 = int(nums[0])
                        num2 = int(nums[1])
                        if (num1 - 1) <= num2:
                            goles_abajo += 1
                    goles_propia = 0
                    for gol in partido.goles_visitante_desc or []:
                        if cod_jugador == gol.cod_goleador:
                            goles_propia += 1

                    amarillas = amonestaciones_local.get(cod_jugador, 0)
                    rojas = rojas_local.get(cod_jugador, 0)
                    desc_goles = ",".join([g.desc or "" for g in goles_jugador])
                    desc_asist = ",".join(
                        [g.desc_asist or "" for g in asistencias_jugador]
                    )
                    asistencias = len(asistencias_jugador)

                    jp = JugadorPartido(
                        id_jugador=int(id_jugador),
                        id_partido=int(id_partido),
                        id_equipo=int(id_local),
                        titular=int(titular),
                        razon_sustituido=razon_sustituido,
                        sustituido=int(sustituido),
                        minutos=int(minutos),
                        goles=int(goles),
                        goles_penalti=int(goles_penalti),
                        amarillas=int(amarillas),
                        rojas=int(rojas),
                        goles_falta=int(goles_falta),
                        goles_cabeza=int(goles_cabeza),
                        goles_abajo=int(goles_abajo),
                        goles_propia=int(goles_propia),
                        asistencias=int(asistencias),
                        desc_goles=desc_goles,
                        desc_asist=desc_asist,
                        penaltis_parados=int(penaltis_parados),
                    )
                    await insertar_actualizar_jugador_partido(cur, jp)

                    jl = JugadorLiga(
                        id_jugador=int(id_jugador),
                        id_liga=int(id_liga),
                        id_equipo=int(id_local),
                        convocatorias=1,
                        titularidades=int(titular),
                        minutos=int(minutos),
                        goles=int(goles),
                        goles_penalti=int(goles_penalti),
                        amarillas=int(amarillas),
                        rojas=int(rojas),
                        goles_propia=int(goles_propia),
                        goles_falta=int(goles_falta),
                        goles_abajo=int(goles_abajo),
                        goles_cabeza=int(goles_cabeza),
                        porterias_imbatidas=1 if goles_visitante == 0 else 0,
                        asistencias=int(asistencias),
                        desc_goles=desc_goles,
                        desc_asist=desc_asist,
                        goles_setentaycinco=int(goles_setentaycinco),
                        goles_sesenta=int(goles_sesenta),
                        racha=int(racha),
                        penaltis_parados=int(penaltis_parados),
                        jugados=1 if minutos > 0 else 0,
                    )

                    await insertar_actualizar_jugador_liga(cur, jl)

                for cod_jugador_t in jugadores_visitante:
                    cod_jugador, enlace = cod_jugador_t
                    id_jugador = await obtener_id_jugador(cur, cod_jugador)

                    if id_jugador is None:

                        cod_visitante = await obtener_cod_equipo(cur, id_visitante)
                        liga_str = await obtener_str_liga(cur, id_liga)
                        enlaces.append(
                            (
                                "https://www.transfermarkt.es" + enlace,
                                cod_visitante,
                                liga_str,
                            )
                        )

                        j: Jugador_BD = Jugador_BD(
                            cod_jugador=cod_jugador,
                            nombre="-",
                            ano=0,
                            fecha_nacimiento=None,
                            altura=None,
                            lateralidad="-",
                            apodo="-",
                            id_localidad=1,
                            id_representante=1,
                            gallego=0,
                        )
                        id_jugador = await insertar_o_actualizar_jugador(cur, j)

                    if not id_jugador:
                        logging.warning(
                            f"Jugador {cod_jugador} no encontrado en partido {cod_partido}"
                        )
                        continue

                    sustituido = 1 if cod_jugador in cambios_visitante_sustituido else 0
                    minuto_salida, razon_sustituido = cambios_visitante_sustituido.get(
                        cod_jugador, (90, "")
                    )
                    if cod_jugador in (cj[0] for cj in partido.cod_titulares_visitante):
                        titular = 1
                        minutos = minuto_salida
                    else:
                        minuto_entrada = cambios_visitante.get(cod_jugador, 90)
                        minutos = minuto_salida - minuto_entrada
                        titular = 0

                    goles_jugador = [
                        g
                        for g in (partido.goles_visitante_desc or [])
                        if g.cod_goleador == cod_jugador
                    ]
                    asistencias_jugador = [
                        g
                        for g in (partido.goles_visitante_desc or [])
                        if g.cod_asistente == cod_jugador
                    ]
                    penaltis_parados_jugador = [
                        p
                        for p in (partido.penaltis_fallados or [])
                        if p.cod_portero == cod_jugador
                    ]
                    penaltis_parados = len(penaltis_parados_jugador)

                    goles = len(goles_jugador)
                    racha = 0
                    if goles > 0:
                        racha = 1
                    goles_penalti = sum(
                        1 for g in goles_jugador if "penalti" in (g.desc or "").lower()
                    )
                    goles_falta = sum(
                        1
                        for g in goles_jugador
                        if "tiro libre" in (g.desc or "").lower()
                    )
                    goles_cabeza = sum(
                        1 for g in goles_jugador if "cabeza" in (g.desc or "").lower()
                    )

                    goles_abajo = 0
                    goles_sesenta = 0
                    goles_setentaycinco = 0
                    for gol in goles_jugador:
                        if gol.minuto >= 75:
                            goles_sesenta += 1
                            goles_setentaycinco += 1
                        elif gol.minuto >= 60:
                            goles_sesenta += 1
                        nums = gol.resultado_gol.split(":", 1)
                        num1 = int(nums[0])
                        num2 = int(nums[1])
                        if (num1 - 1) <= num2:
                            goles_abajo += 1
                    goles_propia = 0
                    for gol in partido.goles_local_desc or []:
                        if cod_jugador == gol.cod_goleador:
                            goles_propia += 1

                    amarillas = amonestaciones_visitante.get(cod_jugador, 0)
                    rojas = rojas_visitante.get(cod_jugador, 0)
                    desc_goles = ",".join([g.desc or "" for g in goles_jugador])
                    desc_asist = ",".join(
                        [g.desc_asist or "" for g in asistencias_jugador]
                    )
                    asistencias = len(asistencias_jugador)

                    jp = JugadorPartido(
                        id_jugador=int(id_jugador),
                        id_partido=int(id_partido),
                        id_equipo=int(id_visitante),
                        titular=int(titular),
                        razon_sustituido=razon_sustituido,
                        sustituido=int(sustituido),
                        minutos=int(minutos),
                        goles=int(goles),
                        goles_penalti=int(goles_penalti),
                        amarillas=int(amarillas),
                        rojas=int(rojas),
                        goles_falta=int(goles_falta),
                        goles_cabeza=int(goles_cabeza),
                        goles_abajo=int(goles_abajo),
                        goles_propia=int(goles_propia),
                        asistencias=int(asistencias),
                        desc_goles=desc_goles,
                        desc_asist=desc_asist,
                        penaltis_parados=int(penaltis_parados),
                    )
                    await insertar_actualizar_jugador_partido(cur, jp)

                    jl = JugadorLiga(
                        id_jugador=int(id_jugador),
                        id_liga=int(id_liga),
                        id_equipo=int(id_visitante),
                        convocatorias=1,
                        titularidades=int(titular),
                        minutos=int(minutos),
                        goles=int(goles),
                        goles_penalti=int(goles_penalti),
                        amarillas=int(amarillas),
                        rojas=int(rojas),
                        goles_propia=int(goles_propia),
                        goles_falta=int(goles_falta),
                        goles_abajo=int(goles_abajo),
                        goles_cabeza=int(goles_cabeza),
                        porterias_imbatidas=1 if goles_local == 0 else 0,
                        asistencias=int(asistencias),
                        desc_goles=desc_goles,
                        desc_asist=desc_asist,
                        goles_setentaycinco=int(goles_setentaycinco),
                        goles_sesenta=int(goles_sesenta),
                        racha=int(racha),
                        penaltis_parados=int(penaltis_parados),
                        jugados=1 if minutos > 0 else 0,
                    )
                    await insertar_actualizar_jugador_liga(cur, jl)
                await insertar_actualizar_partido(
                    cur, cod_partido, fecha, goles_local, goles_visitante
                )
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e

    return enlaces, valor_jornada


async def procesar_partido(pool, partido):
    return await ejecutar_con_reintentos(
        _procesar_partido,
        pool,
        partido,
        descripcion=f"Procesar partido {partido.cod_partido}",
    )


async def procesar_partidos():
    try:
        partidos = await cargar_partidos_desde_json()
        pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")

        fallidos = []

        tareas_partidos = [procesar_partido(pool, partido) for partido in partidos]

        resultados_partidos = await asyncio.gather(*tareas_partidos)
        jornadas_ligas: list[tuple[int, int]] = []
        jugadores = []

        for idx, resultado in enumerate(resultados_partidos):
            if not resultado:
                fallidos.append(partidos[idx])
            else:
                enlaces, jornada = resultado
                jugadores.extend(enlaces)
                jornadas_ligas.append(jornada)

        jornadas_max = defaultdict(int)
        for id_liga, jornada in jornadas_ligas:
            jornadas_max[id_liga] = max(jornadas_max[id_liga], jornada)

        for id_liga, jornada in jornadas_max.items():
            await actualizar_jornada_liga(pool, int(id_liga), int(jornada))

        pool.close()
        await pool.wait_closed()
        if fallidos:
            logging.info(f"Se produjeron {len(fallidos)} fallos en la inserción.")
        else:
            logging.info("Todas las inserciones finalizaron correctamente.")
        if jugadores:
            logging.info(
                print_cabecera(
                    f"Jugadores encontrados que no estaban en la Base: {len(jugadores)}"
                )
            )
            logging.info(f"Scrapeandolos y insertandolos en la base de datos...")
            jugadores_bd, links_represenantes, _ = await scrapear_jugadores(jugadores)
            representantes, _ = await scrapear_agentes(links_represenantes)
            enlaces = [enlace for enlace, _, _ in jugadores]
            historicos, _ = await scrapear_historico_jugadores(enlaces)
            valores_mercado, _ = await scrapear_valores_mercado_jugadores(enlaces)

            if representantes:
                await procesar_representantes(agentes=representantes)
            if jugadores_bd and historicos:
                await procesar_jugadores(jugadores=jugadores_bd, historicos=historicos)
            if valores_mercado:
                pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")
                fallidos = []
                tareas = [
                    actualizar_valores_mercado(pool, jvm) for jvm in valores_mercado
                ]
                resultados = await asyncio.gather(*tareas)

                for idx, resultado in enumerate(resultados):
                    if not resultado:
                        fallidos.append(valores_mercado[idx])
                if fallidos:
                    logging.info(
                        f"Se produjeron {len(fallidos)} fallos en la actualización de valores de mercado."
                    )

                pool.close()
                await pool.wait_closed()
            logging.info(
                print_cabecera(
                    f"Jugadores scrapeados e insertados: {len(jugadores_bd)}"
                )
            )
            logging.info(
                print_cabecera(
                    f"Representantes scrapeados e insertados: {len(representantes)}"
                )
            )
    except Exception as e:
        logging.error(f"Error en la ejecucion: {e}")


async def main():
    start_time = time.time()
    try:
        logging.info(print_cabecera("Iniciando inserción de jornada"))
        await procesar_partidos()
        logging.info(print_cabecera("Inserción de jornada finalizada"))
    except Exception as e:
        logging.error(f"Error en la ejecución: {e}")
    finally:
        end_time = time.time()
        minutes, seconds = divmod(end_time - start_time, 60)
        logging.info(
            f"\nTiempo total de ejecución: {int(minutes)} minutos y {seconds:.2f} segundos"
        )


if __name__ == "__main__":
    asyncio.run(main())
