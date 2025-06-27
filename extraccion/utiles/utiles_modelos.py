from dataclasses import dataclass
from typing import List
from enum import Enum


@dataclass
class URLFailReason(Enum):
    REDIRECTED = "REDIRECTED"
    NOT_FOUND = "NOT_FOUND"
    EMPTY = "EMPTY"
    NO_HTML = "NO_HTML"
    CLICK_FAILED = "CLICK_FAILED"


@dataclass
class URLClickDetails:
    selector: str = None
    wait_for_selector: str = None
    timeout: int = 5000
    reject_cookies: bool = False


@dataclass
class TargetURL:
    url: str
    clicks: List[URLClickDetails] = None
    scroll: bool = False
    selector: str = None
    sleep: int = 0
    json: bool = False


@dataclass
class URLFail:
    attempt: int
    url: str
    reason: URLFailReason
    message: str


@dataclass
class ScrapedURL:
    url: str
    content: str
    paginas_fallidas: List[URLFail]


@dataclass
class ValorMercado:
    valor: str = None
    fecha: str = None


@dataclass
class JugadorValorMercado:
    cod_jugador: str = None
    valor_mercado_actual: ValorMercado = None
    valor_mercado_maximo: ValorMercado = None


@dataclass
class Jugador:
    cod_jugador: str = None
    dorsal: str = None
    apodo: str = None
    capitan: int = 0
    nombre: str = None
    fecha_nacimiento: str = None
    anho_nacimiento: int = None
    lugar_nacimiento: str = None
    nacionalidad: List[str] = None
    altura: str = None
    pie: str = None
    posicion: str = None
    posiciones_secundarias: List[str] = None
    cod_club_actual: str = None
    liga_club_actual: str = None
    agente: str = None
    fecha_fichado: str = None
    contrato_hasta: str = None
    ultima_renovacion: str = None
    club_cedente: str = None
    contrato_hasta_cedente: str = None
    opcion_cedente: str = None


@dataclass
class Liga:
    cod_grupo: str = None
    temporada: str = None
    division: str = None
    grupo: str = None


@dataclass
class Campo:
    nombre: str = None
    localidad: str = None
    direccion: str = None
    superficie: str = None
    dimensiones: str = None
    aforo: int = None
    cod_equipo: str = None


@dataclass
class Club_Equipo:
    nombre: str = None
    cod_club: str = None
    cod_equipo: str = None
    localidad: str = None
    primer_equipo: str = None
    liga: str = None


@dataclass
class Agente:
    nombre: str = None
    telefono: str = None
    email: str = None
    web: str = None
    direccion: str = None


@dataclass
class PartidoCalendario:
    cod_local: str = None
    cod_visitante: str = None
    campo: str = None
    cod_partido: str = None
    jornada: str = None
    enlace: str = None
    liga: str = None
    temporada: str = None


@dataclass
class PartidoPrevia:
    cod_partido: str = None
    horario: str = None


@dataclass
class Cambio:
    cod_entra: str = None
    cod_fuera: str = None
    minuto: int = None
    desc: str = None


@dataclass
class Amonestacion:
    cod_amonestado: str = None
    amarilla: bool = None
    roja: bool = None
    minuto: int = None


@dataclass
class Gol:
    cod_goleador: str = None
    resultado_gol: str = None
    desc: str = None
    minuto: int = None
    cod_asistente: str = None
    desc_asist: str = None


@dataclass
class PenaltiFallado:
    cod_portero: str = None
    minuto: int = None
    parado: int = None


@dataclass
class PartidoJugado:
    cod_partido: str = None
    goles_local: int = None
    goles_visitante: int = None
    cod_titulares_local: list[str] = None
    cod_titulares_visitante: list[str] = None
    cod_suplentes_local: list[str] = None
    cod_suplentes_visitante: list[str] = None
    cambios_local: list[Cambio] = None
    cambios_visitante: list[Cambio] = None
    amonestaciones_local: list[Amonestacion] = None
    amonestaciones_visitante: list[Amonestacion] = None
    goles_local_desc: list[Gol] = None
    goles_visitante_desc: List[Gol] = None
    fecha: str = None
    penaltis_fallados: List[PenaltiFallado] = None


@dataclass
class Fichaje:
    temporada: str = None
    fecha: str = None
    club_anterior: tuple[str, str] = None
    club_nuevo: tuple[str, str] = None
    valor: str = None
    coste: str = None


@dataclass
class HistoricoFichajes:
    cod_jugador: str = None
    fichajes: List[Fichaje] = None


@dataclass
class Incidencia:
    cod_jugador: str = None
    incidencia: str = None
