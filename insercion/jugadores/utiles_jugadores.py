from dataclasses import dataclass
import json
from typing import Optional
import logging
from typing import List
from extraccion.utiles.utiles_modelos import Jugador
from insercion.utiles.utiles_db import (
    ejecutar_con_reintentos,
    normalizar_fecha,
    obtener_id_representante,
    obtener_id_equipo,
    obtener_id_liga,
)
from insercion.utiles.utiles_localidad import (
    Localidad_BD,
    limpiar_localidad_jugador,
)
from insercion.utiles.utiles_paises import obtener_o_insertar_pais


@dataclass
class Jugador_BD:
    cod_jugador: int
    nombre: str
    ano: Optional[int] = None
    fecha_nacimiento: Optional[str] = None
    altura: Optional[float] = None
    lateralidad: Optional[str] = None
    id_localidad: Optional[int] = None
    telefono: Optional[str] = None
    apodo: Optional[str] = None
    id_representante: Optional[int] = None
    gallego: Optional[int] = None
    valor_mercado: Optional[str] = None
    valor_mercado_max: Optional[str] = None


@dataclass
class Posicion_BD:
    id_posicion: Optional[int] = None
    categoria: Optional[str] = None
    nombre: Optional[str] = None


@dataclass
class JugadorLiga_BD:
    id_jugador: int = None
    id_equipo: int = None
    id_liga: int = None
    capitan: int = None
    dorsal: int = None


@dataclass
class Jugador_ContratoActual:
    id_jugador: int = None
    id_equipo: int = None
    fichado: str = None
    fin_contrato: str = None
    ultima_renovacion: str = None
    cesion: int = None
    opcion_compra: int = None
    fin_cesion: str = None
    contrato_cedente_hasta: str = None


async def cargar_jugadores_desde_json(
    ruta="extraccion/jugadores/resultados/datos_jugadores.json",
) -> List[Jugador]:
    try:
        with open(ruta, "r", encoding="utf-8") as f:
            datos = json.load(f)
            jugadores = [Jugador(**j) for j in datos]
            logging.info(f"Cargados {len(jugadores)} jugadores desde JSON.")
            return jugadores
    except Exception as e:
        logging.error(f"Error cargando jugadores JSON: {e}")
        return []


def preprocesar_posicion(posicion_str: str) -> Posicion_BD:
    if not posicion_str:
        return Posicion_BD(categoria=None, nombre="-")
    if "-" in posicion_str:
        categoria, nombre = [p.strip() for p in posicion_str.split("-", 1)]
    else:
        categoria, nombre = None, posicion_str.strip()

        if any(palabra in nombre for palabra in ["Defensa", "Lateral", "Carrilero"]):
            categoria = "Defensa"
        elif any(
            palabra in nombre
            for palabra in ["Medio", "Pivote", "Interior", "Centrocampista"]
        ):
            categoria = "Centrocampista"
        elif any(
            palabra in nombre for palabra in ["Extremo", "Delantero", "Mediapunta"]
        ):
            categoria = "Delantero"
        elif nombre.lower() == "portero":
            categoria = "Portero"
    if nombre == "Centrocampista":
        nombre = "Mediocentro"
    return Posicion_BD(categoria=categoria, nombre=nombre)


def normalizar_altura(altura_str: Optional[str]) -> Optional[float]:
    if not altura_str or "-" in altura_str:
        return None
    try:
        altura_str = altura_str.replace("m", "").strip().replace(",", ".")
        return float(altura_str.strip())
    except Exception:
        return None


def es_gallego(localidad: Localidad_BD) -> int:
    provincias_gallegas = {
        "ourense",
        "orense",
        "pontevedra",
        "a coruña",
        "la coruña",
        "lugo",
    }

    if localidad.provincia and localidad.provincia.lower() in provincias_gallegas:
        return 1
    if (
        localidad.comunidad_autonoma
        and localidad.comunidad_autonoma.lower() == "galicia"
    ):
        return 1
    return 0


async def insertar_o_actualizar_jugador(cur, jugador_bd: Jugador_BD):
    fecha = normalizar_fecha(jugador_bd.fecha_nacimiento)

    await cur.execute(
        "SELECT id_jugador FROM jugador WHERE cod_jugador = %s",
        (int(jugador_bd.cod_jugador),),
    )
    existe = await cur.fetchone()
    if existe:
        await cur.execute(
            """
                        UPDATE jugador SET cod_jugador=%s, nombre=%s, ano=%s, fecha_nacimiento=%s, altura=%s, lateralidad=%s,
                        id_localidad=%s, apodo=%s, id_representante=%s, gallego=%s
                        WHERE id_jugador=%s
                        """,
            (
                int(jugador_bd.cod_jugador),
                jugador_bd.nombre,
                int(jugador_bd.ano) if jugador_bd.ano is not None else None,
                fecha,
                float(jugador_bd.altura) if jugador_bd.altura is not None else None,
                jugador_bd.lateralidad,
                int(jugador_bd.id_localidad) if jugador_bd.id_localidad else None,
                jugador_bd.apodo,
                (
                    int(jugador_bd.id_representante)
                    if jugador_bd.id_representante
                    else None
                ),
                int(jugador_bd.gallego),
                existe[0],
            ),
        )
        return existe[0]
    else:
        await cur.execute(
            """
                    INSERT INTO jugador (cod_jugador, nombre, ano, fecha_nacimiento, altura, lateralidad,
                    id_localidad, apodo, gallego, id_representante)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
            (
                int(jugador_bd.cod_jugador),
                jugador_bd.nombre,
                int(jugador_bd.ano) if jugador_bd.ano is not None else None,
                fecha,
                float(jugador_bd.altura) if jugador_bd.altura is not None else None,
                jugador_bd.lateralidad,
                int(jugador_bd.id_localidad) if jugador_bd.id_localidad else None,
                jugador_bd.apodo,
                int(jugador_bd.gallego),
                (
                    int(jugador_bd.id_representante)
                    if jugador_bd.id_representante
                    else None
                ),
            ),
        )

        return cur.lastrowid


async def obtener_o_insertar_posicion(cur, posicion_bd: Posicion_BD) -> int:

    await cur.execute(
        "SELECT id_posicion FROM posicion WHERE nombre = %s",
        (posicion_bd.nombre,),
    )
    res = await cur.fetchone()
    if res:
        return res[0]
    await cur.execute(
        "INSERT INTO posicion (categoria, nombre) VALUES (%s, %s)",
        (posicion_bd.categoria, posicion_bd.nombre),
    )
    return cur.lastrowid


async def insertar_posiciones(cur, posiciones):
    posiciones_ids = {}
    for p in posiciones:
        id_posicion = await obtener_o_insertar_posicion(cur, p)
        posiciones_ids[p.nombre] = id_posicion
    return posiciones_ids


async def insertar_representantes(cur, representantes):
    representantes_ids = {}
    for r in representantes:
        id_representante = await obtener_id_representante(cur, r)
        representantes_ids[r] = id_representante
    return representantes_ids


async def insertar_jugador_posicion(
    cur, id_jugador: int, id_posicion: int, principal: int
):
    await cur.execute(
        "SELECT 1 FROM jugador_posicion WHERE id_jugador=%s AND id_posicion=%s",
        (int(id_jugador), int(id_posicion)),
    )
    existe = await cur.fetchone()
    if existe:
        await cur.execute(
            "UPDATE jugador_posicion SET principal=%s WHERE id_jugador=%s AND id_posicion=%s",
            (int(principal), int(id_jugador), int(id_posicion)),
        )
        return True
    else:
        await cur.execute(
            "INSERT INTO jugador_posicion (id_jugador, id_posicion, principal) VALUES (%s, %s, %s)",
            (int(id_jugador), int(id_posicion), int(principal)),
        )
        return True


async def insertar_jugador_pais(cur, id_jugador: int, id_pais: int):

    await cur.execute(
        "SELECT 1 FROM jugador_pais WHERE id_jugador=%s AND id_pais=%s",
        (int(id_jugador), int(id_pais)),
    )
    existe = await cur.fetchone()
    if existe:
        return True
    await cur.execute(
        "INSERT INTO jugador_pais (id_jugador, id_pais) VALUES (%s, %s)",
        (int(id_jugador), int(id_pais)),
    )
    return True


async def insertar_actualizar_jugador_liga(cur, jugador_liga: JugadorLiga_BD):

    await cur.execute(
        "SELECT 1 FROM jugador_liga WHERE id_jugador=%s AND id_liga=%s AND id_equipo=%s",
        (
            int(jugador_liga.id_jugador),
            int(jugador_liga.id_liga),
            int(jugador_liga.id_equipo),
        ),
    )
    existe = await cur.fetchone()
    if existe:
        await cur.execute(
            """
                    UPDATE jugador_liga
                    SET capitan=%s, dorsal=%s
                    WHERE id_jugador=%s AND id_liga=%s AND id_equipo=%s
                    """,
            (
                int(jugador_liga.capitan),
                int(jugador_liga.dorsal),
                int(jugador_liga.id_jugador),
                int(jugador_liga.id_liga),
                int(jugador_liga.id_equipo),
            ),
        )
    else:
        await cur.execute(
            """
                    INSERT INTO jugador_liga (id_jugador, id_liga, id_equipo, capitan, dorsal)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
            (
                int(jugador_liga.id_jugador),
                int(jugador_liga.id_liga),
                int(jugador_liga.id_equipo),
                int(jugador_liga.capitan),
                int(jugador_liga.dorsal),
            ),
        )
    return True


async def insertar_actualizar_contrato_actual(cur, contrato: Jugador_ContratoActual):
    await cur.execute(
        "SELECT 1 FROM contrato WHERE id_jugador=%s",
        (int(contrato.id_jugador),),
    )
    existe = await cur.fetchone()
    if existe:
        await cur.execute(
            """
                    UPDATE contrato
                    SET id_equipo=%s, fichado=%s, fin_contrato=%s, ultima_renovacion=%s,
                        cesion=%s, opcion_compra=%s, fin_cesion=%s, contrato_cedente_hasta=%s
                    WHERE id_jugador=%s
                    """,
            (
                int(contrato.id_equipo),
                normalizar_fecha(contrato.fichado),
                normalizar_fecha(contrato.fin_contrato),
                normalizar_fecha(contrato.ultima_renovacion),
                int(contrato.cesion),
                int(contrato.opcion_compra),
                normalizar_fecha(contrato.fin_cesion),
                normalizar_fecha(contrato.contrato_cedente_hasta),
                int(contrato.id_jugador),
            ),
        )
    else:
        await cur.execute(
            """
                    INSERT INTO contrato
                    (id_jugador, id_equipo, fichado, fin_contrato, ultima_renovacion,
                     cesion, opcion_compra, fin_cesion, contrato_cedente_hasta)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
            (
                int(contrato.id_jugador),
                int(contrato.id_equipo),
                normalizar_fecha(contrato.fichado),
                normalizar_fecha(contrato.fin_contrato),
                normalizar_fecha(contrato.ultima_renovacion),
                int(contrato.cesion),
                int(contrato.opcion_compra),
                normalizar_fecha(contrato.fin_cesion),
                normalizar_fecha(contrato.contrato_cedente_hasta),
            ),
        )
    return True


async def _procesar_jugador(
    pool, jugador, localidades_ids, paises_ids, posiciones_ids, representantes_ids
):
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                localidad_obj = await limpiar_localidad_jugador(
                    cur, jugador.lugar_nacimiento
                )
                id_localidad = localidades_ids.get(localidad_obj.nombre)
                gallego = 0
                if id_localidad:
                    gallego = es_gallego(localidad_obj)

                altura = normalizar_altura(jugador.altura)
                id_representante = representantes_ids.get(jugador.agente)
                jugador_bd = Jugador_BD(
                    cod_jugador=int(jugador.cod_jugador),
                    nombre=jugador.nombre,
                    ano=(
                        int(jugador.anho_nacimiento)
                        if jugador.anho_nacimiento != "-"
                        else 0
                    ),
                    fecha_nacimiento=jugador.fecha_nacimiento,
                    altura=altura,
                    lateralidad=jugador.pie,
                    id_localidad=int(id_localidad),
                    apodo=jugador.apodo,
                    id_representante=int(id_representante),
                    gallego=gallego,
                )

                id_jugador = await insertar_o_actualizar_jugador(cur, jugador_bd)

                posiciones = []
                if jugador.posicion:
                    posiciones.append(jugador.posicion)
                if jugador.posiciones_secundarias:
                    posiciones.extend(jugador.posiciones_secundarias)
                for idx, pos_str in enumerate(posiciones):
                    pos_bd = preprocesar_posicion(pos_str)
                    id_posicion = posiciones_ids.get(pos_bd.nombre)
                    principal = 0
                    if idx == 0:
                        principal = 1
                    await insertar_jugador_posicion(
                        cur, int(id_jugador), int(id_posicion), principal
                    )

                for pais in jugador.nacionalidad:
                    id_pais = paises_ids.get(pais)
                    if not id_pais:
                        id_pais = await obtener_o_insertar_pais(cur, pais)
                        paises_ids[pais] = id_pais
                    await insertar_jugador_pais(cur, int(id_jugador), int(id_pais))

                id_equipo = await obtener_id_equipo(cur, jugador.cod_club_actual)
                id_liga = await obtener_id_liga(cur, jugador.liga_club_actual)
                jugador_liga = JugadorLiga_BD(
                    id_jugador=int(id_jugador),
                    id_equipo=int(id_equipo),
                    id_liga=int(id_liga),
                    capitan=int(jugador.capitan),
                    dorsal=int(jugador.dorsal) if jugador.dorsal != "-" else 0,
                )
                await insertar_actualizar_jugador_liga(cur, jugador_liga)

                cesion = 1 if jugador.club_cedente != "-" else 0
                opcion_compra = (
                    1
                    if cesion and "compra" in jugador.opcion_cedente.lower() or ""
                    else 0
                )

                contrato = Jugador_ContratoActual(
                    id_jugador=int(id_jugador),
                    id_equipo=int(id_equipo),
                    fichado=jugador.fecha_fichado,
                    fin_contrato=jugador.contrato_hasta,
                    ultima_renovacion=jugador.ultima_renovacion,
                    cesion=cesion,
                    opcion_compra=opcion_compra,
                    fin_cesion=jugador.contrato_hasta_cedente,
                    contrato_cedente_hasta=jugador.contrato_hasta_cedente,
                )
                await insertar_actualizar_contrato_actual(cur, contrato)
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e
    return True


async def procesar_jugador(
    pool, jugador, localidades_ids, paises_ids, posiciones_ids, representantes_ids
):
    return await ejecutar_con_reintentos(
        _procesar_jugador,
        pool,
        jugador,
        localidades_ids,
        paises_ids,
        posiciones_ids,
        representantes_ids,
        descripcion=f"Introducir {jugador.nombre}",
    )
