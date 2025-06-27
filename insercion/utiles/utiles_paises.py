async def _insertar_pais(cur, nombre: str):

    await cur.execute("SELECT id_pais FROM pais WHERE nombre = %s", (nombre,))
    existe = await cur.fetchone()
    if existe:
        return existe[0]
    await cur.execute(
        "INSERT INTO pais (nombre) VALUES (%s)",
        (nombre),
    )
    return cur.lastrowid


async def obtener_o_insertar_pais(cur, nombre_pais: str) -> int:
    return await _insertar_pais(cur, nombre_pais)
