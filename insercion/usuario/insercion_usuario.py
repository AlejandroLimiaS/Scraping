import asyncio
import logging
import os
from pathlib import Path
import sys


sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from insercion.usuario.utiles_usuario import (
    Usuario,
    hash_password,
    insertar_usuario,
    pedir_contrasena,
    usuario_existe,
)

from insercion.utiles.utiles_db import (
    crear_pool_bd_async,
)

USER = os.getenv("MYSQL_USER")
if USER is None:
    raise ValueError("MYSQL_USER environment variable is not set.")
PASSWORD = os.getenv("MYSQL_PASSWORD")
if PASSWORD is None:
    raise ValueError("MYSQL_PASSWORD environment variable is not set.")


async def introducir_usuario():
    pool = await crear_pool_bd_async(USER, PASSWORD, "federacion")
    try:
        while True:
            print("\n--- Introducir nuevo usuario (o 's' para salir) ---")
            usuario = input("Nombre de usuario: ").strip()
            if usuario.lower() == "s":
                print("Saliendo.")
                break
            if " " in usuario:
                usuario = usuario.replace(" ", "_")

            if await usuario_existe(pool, usuario):
                print("El usuario ya existe. Intenta con otro.")
                continue

            nombre = input("Nombre: ").strip()
            if nombre.lower() == "s":
                print("Saliendo.")
                break
            nombre = " ".join(n.capitalize() for n in nombre.split())
            apellidos = input("Apellidos: ").strip()
            if apellidos.lower() == "s":
                print("Saliendo.")
                break
            apellidos = " ".join(a.capitalize() for a in apellidos.split())

            email = input("Email: ").strip()
            if email.lower() == "s":
                print("Saliendo.")
                break

            pw = pedir_contrasena()
            if pw is None:
                print("Saliendo.")
                break

            contrasenha_hash = hash_password(pw)

            prioridad_str = input(
                "Prioridad (1: Director Deportivo, 2: Scout): "
            ).strip()
            if prioridad_str.lower() == "s":
                print("Saliendo.")
                break
            prioridad = 2
            if prioridad_str in {"1", "2"}:
                prioridad = int(prioridad_str)
            else:
                print("Prioridad no válida, asignada por defecto a 2 (Scout).")

            usuario_bd = Usuario(
                usuario=usuario,
                nombre=nombre.capitalize(),
                apellidos=apellidos,
                email=email,
                contrasenha_hash=contrasenha_hash,
                prioridad=prioridad,
            )

            ok = await insertar_usuario(pool, usuario_bd)
            if ok:
                print(f"Usuario '{usuario}' insertado correctamente.")
                break
            else:
                print(
                    "Error al insertar el usuario. Intenta de nuevo o escribe 's' para salir."
                )
                if input("¿Quieres salir? (s/n): ").strip().lower() == "s":
                    break
    except Exception as e:
        print(f"Error inesperado: {e}")
    finally:
        pool.close()
        await pool.wait_closed()


async def main():
    try:
        await introducir_usuario()
    except Exception as e:
        logging.error(f"Error en la ejecución: {e}")


if __name__ == "__main__":
    asyncio.run(main())
