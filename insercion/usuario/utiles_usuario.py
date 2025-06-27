from dataclasses import dataclass
import getpass
import bcrypt

from insercion.utiles.utiles_db import ejecutar_con_reintentos


@dataclass
class Usuario:
    usuario: str = None
    nombre: str = None
    apellidos: str = None
    email: str = None
    contrasenha_hash: str = None
    prioridad: int = 2


async def _usuario_existe(pool, nombre_usuario):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM usuario WHERE nombre_usuario = %s", (nombre_usuario,)
            )
            res = await cur.fetchone()
            return res is not None


async def usuario_existe(pool, nombre_usuario):
    return await ejecutar_con_reintentos(
        _usuario_existe,
        pool,
        nombre_usuario,
        descripcion=f"Comprobar si existe {nombre_usuario}",
    )


async def _insertar_usuario(pool, usuario_bd: Usuario):
    async with pool.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO usuario (nombre_usuario, nombre, apellidos, email, contrasena_hash, cuenta_activa, prioridad)
                    VALUES (%s, %s, %s, %s, %s, 1, %s)
                    """,
                    (
                        usuario_bd.usuario,
                        usuario_bd.nombre,
                        usuario_bd.apellidos,
                        usuario_bd.email,
                        usuario_bd.contrasenha_hash,
                        int(usuario_bd.prioridad),
                    ),
                )
                id_usuario = cur.lastrowid
                await cur.execute(
                    """
                    INSERT INTO lista (nombre, descripcion, id_usuario, visibilidad)
                    VALUES (%s, %s, %s, 1)
                    """,
                    (
                        "Seguir",
                        "Jugadores que han sido observados varias veces y comienzan a destacar. Pueden ser candidatos futuros",
                        int(id_usuario),
                    ),
                )
                await cur.execute(
                    """
                    INSERT INTO lista (nombre, descripcion, id_usuario, visibilidad)
                    VALUES (%s, %s, %s, 1)
                    """,
                    (
                        "Observar",
                        "Seguimiento preliminar: jugadores con potencial pero que necesitan más partidos o informes",
                        int(id_usuario),
                    ),
                )
                if usuario_bd.prioridad == 1:
                    await cur.execute(
                        """
                        INSERT INTO lista (nombre, descripcion, id_usuario, visibilidad)
                        VALUES (%s, %s, %s, 0)
                        """,
                        (
                            "Fichar",
                            "Jugadores que se consideran para fichar tras un seguimiento exhaustivo",
                            int(id_usuario),
                        ),
                    )
                await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e
    return True


async def insertar_usuario(pool, usuario_bd: Usuario):
    return await ejecutar_con_reintentos(
        _insertar_usuario,
        pool,
        usuario_bd,
        descripcion=f"Introducir usuario {usuario_bd.usuario}",
    )


def pedir_contrasena():
    while True:
        pw1 = getpass.getpass("Introduce la contraseña: ")
        pw2 = getpass.getpass("Confirma la contraseña: ")
        if pw1 != pw2:
            print("Las contraseñas no coinciden. Inténtalo de nuevo.")
        elif len(pw1) < 4:
            print("La contraseña debe tener al menos 4 caracteres.")
        else:
            return pw1


def hash_password(pw: str) -> str:
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(pw.encode(), salt)
    return hashed.decode("utf-8")
