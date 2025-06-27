from dataclasses import dataclass
import json
import logging
from typing import Optional
from extraccion.utiles.utiles_modelos import Campo
from insercion.utiles.utiles_db import ejecutar_con_reintentos
from insercion.utiles.utiles_localidad import (
    limpiar_localidad,
    obtener_o_insertar_localidad,
)


@dataclass
class Campo_BD:
    nombre: str
    id_localidad: int
    direccion: Optional[str] = None
    superficie: Optional[str] = None
    dimensiones: Optional[str] = None
    aforo: Optional[int] = None


def cargar_campos_json(
    ruta="extraccion/equipos/resultados/datos_campos.json",
) -> list[Campo]:
    try:
        with open(ruta, "r", encoding="utf-8") as file:
            campos = [Campo(**c) for c in json.load(file)]
            logging.info(f"Se han cargado {len(campos)} campos.")
            return campos
    except FileNotFoundError:
        logging.error("El archivo datos_campos.json no se encuentra.")
        return []


async def _insertar_campo(
    pool, campo_raw: Campo, localidades_ids: dict = None, paises_ids: dict = None
):
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                localidad = await limpiar_localidad(cur, campo_raw.localidad)
                if localidades_ids and localidad.nombre in localidades_ids:
                    id_localidad = localidades_ids[localidad.nombre]
                else:
                    id_localidad = await obtener_o_insertar_localidad(
                        cur, localidad, paises_ids
                    )
                campo = Campo_BD(
                    nombre=campo_raw.nombre,
                    id_localidad=int(id_localidad),
                    direccion=campo_raw.direccion,
                    superficie=campo_raw.superficie,
                    dimensiones=campo_raw.dimensiones,
                    aforo=int(campo_raw.aforo),
                )

                await cur.execute(
                    "SELECT id_campo FROM campo WHERE nombre = %s", (campo.nombre,)
                )
                existente = await cur.fetchone()
                if existente:
                    await cur.execute(
                        """
                        UPDATE campo SET nombre=%s, id_localidad=%s, direccion=%s, superficie=%s, dimensiones=%s, aforo=%s
                        WHERE id_campo=%s
                        """,
                        (
                            campo.nombre,
                            int(campo.id_localidad),
                            campo.direccion,
                            campo.superficie,
                            campo.dimensiones,
                            int(campo.aforo),
                            int(existente[0]),
                        ),
                    )
                    await conn.commit()
                    return existente[0]
                await cur.execute(
                    """
                    INSERT INTO campo (nombre, id_localidad, direccion, superficie, dimensiones, aforo)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        campo.nombre,
                        int(campo.id_localidad),
                        campo.direccion,
                        campo.superficie,
                        campo.dimensiones,
                        int(campo.aforo),
                    ),
                )
            await conn.commit()
            return cur.lastrowid
        except Exception as e:
            await conn.rollback()
            raise e


async def insertar_campo(
    pool, campo_raw: Campo, localidades_ids: dict = None, paises_ids: dict = None
) -> int:
    return await ejecutar_con_reintentos(
        _insertar_campo,
        pool,
        campo_raw,
        localidades_ids,
        paises_ids,
        descripcion=f"campo {campo_raw.nombre}",
    )


async def _actualizar_id_campo_en_equipos(pool, campos_raw, id_campo_por_nombre):
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                for campo in campos_raw:
                    id_campo = id_campo_por_nombre.get(campo.nombre)
                    if id_campo is None:
                        continue
                    cod_equipo = campo.cod_equipo
                    if not cod_equipo:
                        continue
                    await cur.execute(
                        "UPDATE equipo SET id_campo=%s WHERE cod_equipo=%s",
                        (int(id_campo), int(cod_equipo)),
                    )
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e
    return True


async def actualizar_campos(pool, campos_raw, id_campo_por_nombre):
    return await ejecutar_con_reintentos(
        _actualizar_id_campo_en_equipos,
        pool,
        campos_raw,
        id_campo_por_nombre,
        descripcion=f"{len(campos_raw)} campos asociados a equipos",
    )
