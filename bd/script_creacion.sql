CREATE TABLE federacion.usuario (
    id_usuario int auto_increment NOT NULL,
    nombre_usuario varchar(255) NOT NULL,
    nombre varchar(255) NOT NULL,
    apellidos varchar(255) NULL,
    email varchar(255) NULL,
    contrasena_hash varchar(255) NOT NULL,
    cuenta_activa tinyint(1) NOT NULL,
    prioridad tinyint DEFAULT 2 NOT NULL,
    CONSTRAINT usuario_pk PRIMARY KEY (id_usuario),
    CONSTRAINT usuario_unique UNIQUE KEY (nombre_usuario)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE federacion.liga (
    id_liga int auto_increment NOT NULL,
    cod_grupo varchar(100) NOT NULL,
    temporada varchar(100) NOT NULL,
    division varchar(100) NOT NULL,
    grupo varchar(100) NOT NULL,
    ultima_jornada int DEFAULT 0 NOT NULL,
    CONSTRAINT liga_pk PRIMARY KEY (id_liga),
    CONSTRAINT liga_unique_1 UNIQUE KEY (temporada, division, grupo)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE federacion.representante (
    id_representante int auto_increment NOT NULL,
    nombre varchar(100) NOT NULL,
    telefono varchar(100) NULL,
    email varchar(100) NULL,
    pag_web varchar(100) NULL,
    direccion varchar(100) NULL,
    CONSTRAINT representante_pk PRIMARY KEY (id_representante),
    CONSTRAINT representante_unique UNIQUE KEY (nombre)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE federacion.representante_fav (
    id_usuario int NOT NULL,
    id_representante int NOT NULL,
    CONSTRAINT representante_fav_pk PRIMARY KEY (id_usuario, id_representante),
    CONSTRAINT representante_fav_usuario_FK FOREIGN KEY (id_usuario) REFERENCES federacion.usuario (id_usuario) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT representante_fav_representante_FK FOREIGN KEY (id_representante) REFERENCES federacion.representante (id_representante) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE federacion.pais (
    id_pais int auto_increment NOT NULL,
    nombre varchar(100) NOT NULL,
    CONSTRAINT pais_pk PRIMARY KEY (id_pais),
    CONSTRAINT pais_unique UNIQUE KEY (nombre)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;


CREATE TABLE federacion.localidad (
	id_localidad int auto_increment NOT NULL,
	nombre varchar(100) NOT NULL,
	provincia varchar(100) NULL,
	comunidad_autonoma varchar(100) NULL,
	id_pais int NOT NULL,
	CONSTRAINT localidad_pk PRIMARY KEY (id_localidad),
	CONSTRAINT localidad_unique UNIQUE KEY (nombre),
	CONSTRAINT localidad_pais_FK FOREIGN KEY (id_pais) REFERENCES federacion.pais(id_pais) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.campo (
	id_campo int auto_increment NOT NULL,
	nombre varchar(100) NOT NULL,
	id_localidad int NOT NULL,
	direccion varchar(255) NULL,
	superficie varchar(100) NULL,
	aforo int NULL,
	dimensiones varchar(100) NULL,
	CONSTRAINT campo_pk PRIMARY KEY (id_campo),
	CONSTRAINT campo_localidad_FK FOREIGN KEY (id_localidad) REFERENCES federacion.localidad(id_localidad) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE federacion.club (
	id_club int auto_increment NOT NULL,
	nombre varchar(100) NULL,
	cod_club int NOT NULL,
	id_localidad int NOT NULL,
	CONSTRAINT club_pk PRIMARY KEY (id_club),
	CONSTRAINT club_unique UNIQUE KEY (cod_club),
	CONSTRAINT club_localidad_FK FOREIGN KEY (id_localidad) REFERENCES federacion.localidad(id_localidad) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE federacion.equipo (
	id_equipo int auto_increment NOT NULL,
	id_club int NOT NULL,
	nombre varchar(100) NOT NULL,
	cod_equipo int NOT NULL,
	id_campo int NULL,
	CONSTRAINT equipo_pk PRIMARY KEY (id_equipo),
	CONSTRAINT equipo_unique UNIQUE KEY (cod_equipo),
	CONSTRAINT equipo_club_FK FOREIGN KEY (id_club) REFERENCES federacion.club(id_club) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT equipo_campo_FK FOREIGN KEY (id_campo) REFERENCES federacion.campo(id_campo) ON DELETE SET NULL ON UPDATE CASCADE
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE federacion.equipo_liga (
	id_equipo int NOT NULL,
	id_liga int NOT NULL,
	puntos int DEFAULT 0 NOT NULL,
	jugados int DEFAULT 0 NOT NULL,
	victorias int DEFAULT 0 NOT NULL,
	empates int DEFAULT 0 NOT NULL,
	derrotas int DEFAULT 0 NOT NULL,
	goles_favor int DEFAULT 0 NOT NULL,
	goles_contra int DEFAULT 0 NOT NULL,
	CONSTRAINT equipo_liga_pk PRIMARY KEY (id_equipo,id_liga),
	CONSTRAINT equipo_liga_equipo_FK FOREIGN KEY (id_equipo) REFERENCES federacion.equipo(id_equipo) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT equipo_liga_liga_FK FOREIGN KEY (id_liga) REFERENCES federacion.liga(id_liga) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.jugador (
	id_jugador int auto_increment NOT NULL,
	cod_jugador int NOT NULL,
	nombre varchar(255) NOT NULL,
	ano int NULL,
	fecha_nacimiento date NULL,
	altura float NULL,
	lateralidad varchar(100) NULL,
	id_localidad int NOT NULL,
	telefono varchar(100) NULL,
	apodo varchar(255) NULL,
	id_representante int NULL,
	gallego tinyint(1) DEFAULT 0 NULL,
	valor_mercado_actual float NULL,
	fecha_valor_mercado_actual date NULL,
	valor_mercado_maximo float NULL,
	fecha_valor_mercado_maximo date NULL,
	CONSTRAINT jugador_pk PRIMARY KEY (id_jugador),
	CONSTRAINT jugador_unique UNIQUE KEY (cod_jugador),
	CONSTRAINT jugador_localidad_FK FOREIGN KEY (id_localidad) REFERENCES federacion.localidad(id_localidad) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT jugador_representante_FK FOREIGN KEY (id_representante) REFERENCES federacion.representante(id_representante) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.contrato (
	id_jugador int NOT NULL,
	id_equipo int NOT NULL,
	fichado date NULL,
	fin_contrato date NULL,
	ultima_renovacion date NULL,
	cesion tinyint(1) NULL,
	opcion_compra tinyint(1) NULL,
	fin_cesion date NULL,
	contrato_cedente_hasta date NULL,
	CONSTRAINT contrato_pk PRIMARY KEY (id_jugador),
	CONSTRAINT contrato_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT contrato_equipo_FK FOREIGN KEY (id_equipo) REFERENCES federacion.equipo(id_equipo) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.posicion (
	id_posicion int auto_increment NOT NULL,
	categoria varchar(100) NULL,
	nombre varchar(100) NOT NULL,
	CONSTRAINT posicion_pk PRIMARY KEY (id_posicion),
	CONSTRAINT posicion_unique UNIQUE KEY (nombre)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.jugador_posicion (
	id_jugador int NOT NULL,
	id_posicion int NOT NULL,
	principal tinyint(1) DEFAULT 0 NOT NULL,
	CONSTRAINT jugador_posicion_pk PRIMARY KEY (id_jugador,id_posicion),
	CONSTRAINT jugador_posicion_posicion_FK FOREIGN KEY (id_posicion) REFERENCES federacion.posicion(id_posicion) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT jugador_posicion_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.jugador_pais (
	id_jugador int NOT NULL,
	id_pais int NOT NULL,
	CONSTRAINT jugador_pais_pk PRIMARY KEY (id_jugador,id_pais),
	CONSTRAINT jugador_pais_pais_FK FOREIGN KEY (id_pais) REFERENCES federacion.pais(id_pais) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT jugador_pais_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE CASCADE ON UPDATE CASCADE
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.partido (
	id_partido int auto_increment NOT NULL,
	id_liga int NOT NULL,
	id_local int NOT NULL,
	id_visitante int NOT NULL,
	id_campo int NULL,
	cod_partido int NOT NULL,
	jornada int NOT NULL,
	fecha datetime NULL,
	goles_local int NULL,
	goles_visitante int NULL,
	jugado tinyint(1) DEFAULT 0 NOT NULL,
	enlace varchar(255) NOT NULL,
	CONSTRAINT partido_pk PRIMARY KEY (id_partido),
	CONSTRAINT partido_unique UNIQUE KEY (cod_partido),
	CONSTRAINT partido_liga_FK FOREIGN KEY (id_liga) REFERENCES federacion.liga(id_liga) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT partido_equipo_FK FOREIGN KEY (id_local) REFERENCES federacion.equipo(id_equipo) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT partido_equipo_FK_1 FOREIGN KEY (id_visitante) REFERENCES federacion.equipo(id_equipo) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT partido_campo_FK FOREIGN KEY (id_campo) REFERENCES federacion.campo(id_campo) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.jugador_partido (
	id_jugador int NOT NULL,
	id_partido int NOT NULL,
	id_equipo int NOT NULL,
	titular tinyint(1) DEFAULT 0 NOT NULL,
	minutos int DEFAULT 0 NOT NULL,
	sustituido int DEFAULT 0 NOT NULL,
	razon_sustituido varchar(100) NULL,
	goles int DEFAULT 0 NOT NULL,
	goles_penalti int DEFAULT 0 NOT NULL,
	amarillas int DEFAULT 0 NOT NULL,
	rojas tinyint(1) DEFAULT 0 NOT NULL,
	goles_falta int DEFAULT 0 NOT NULL,
	goles_cabeza int DEFAULT 0 NOT NULL,
	goles_propia int DEFAULT 0 NOT NULL,
	goles_abajo int DEFAULT 0 NOT NULL,
	penaltis_parados int DEFAULT 0 NOT NULL,
	asistencias int DEFAULT 0 NOT NULL,
	desc_goles text NULL,
	desc_asist text NULL,
	CONSTRAINT jugador_partido_pk PRIMARY KEY (id_jugador,id_partido),
	CONSTRAINT jugador_partido_equipo_FK FOREIGN KEY (id_equipo) REFERENCES federacion.equipo(id_equipo) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT jugador_partido_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT jugador_partido_partido_FK FOREIGN KEY (id_partido) REFERENCES federacion.partido(id_partido) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.jugador_liga (
	id_jugador int NOT NULL,
	id_liga int NOT NULL,
	id_equipo int NOT NULL,
	convocatorias int DEFAULT 0 NOT NULL,
	titularidades int DEFAULT 0 NOT NULL,
	minutos int DEFAULT 0 NOT NULL,
	goles int DEFAULT 0 NOT NULL,
	goles_penalti int DEFAULT 0 NOT NULL,
	amarillas int DEFAULT 0 NOT NULL,
	rojas int DEFAULT 0 NOT NULL,
	goles_falta int DEFAULT 0 NOT NULL,
	goles_propia int DEFAULT 0 NOT NULL,
	goles_abajo int DEFAULT 0 NOT NULL,
	goles_cabeza int DEFAULT 0 NOT NULL,
	porterias_imbatidas int DEFAULT 0 NOT NULL,
	penaltis_parados int DEFAULT 0 NOT NULL,
	asistencias int DEFAULT 0 NOT NULL,
	capitan tinyint(1) DEFAULT 0 NOT NULL,
	dorsal int DEFAULT 0 NOT NULL,
	desc_goles text NULL,
	desc_asist text NULL,
	goles_sesenta int DEFAULT 0 NOT NULL,
	goles_setentaycinco int DEFAULT 0 NOT NULL,
	racha int DEFAULT 0 NOT NULL,
	jugados int DEFAULT 0 NOT NULL,
	CONSTRAINT jugador_liga_pk PRIMARY KEY (id_jugador,id_liga,id_equipo),
	CONSTRAINT jugador_liga_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT jugador_liga_equipo_FK FOREIGN KEY (id_equipo) REFERENCES federacion.equipo(id_equipo) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT jugador_liga_liga_FK FOREIGN KEY (id_liga) REFERENCES federacion.liga(id_liga) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE federacion.lista (
	id_lista int auto_increment NOT NULL,
	nombre varchar(100) NOT NULL,
	descripcion text NULL,
	id_usuario int NOT NULL,
	visibilidad int DEFAULT 2 NOT NULL,
	CONSTRAINT lista_pk PRIMARY KEY (id_lista),
	CONSTRAINT lista_unique UNIQUE KEY (id_usuario,nombre),
	CONSTRAINT lista_usuario_FK FOREIGN KEY (id_usuario) REFERENCES federacion.usuario(id_usuario) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.jugador_lista (
	id_jugador int NOT NULL,
	id_lista int NOT NULL,
	fecha_entrada timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT jugador_lista_pk PRIMARY KEY (id_jugador,id_lista),
	CONSTRAINT jugador_lista_lista_FK FOREIGN KEY (id_lista) REFERENCES federacion.lista(id_lista) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT jugador_lista_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.incidencia (
	id_incidencia int auto_increment NOT NULL,
	id_jugador int NOT NULL,
	id_usuario int NULL,
	fecha timestamp DEFAULT current_timestamp on update current_timestamp NOT NULL,
	observacion text NOT NULL,
	CONSTRAINT incidencia_pk PRIMARY KEY (id_incidencia),
	CONSTRAINT incidencia_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT incidencia_usuario_FK FOREIGN KEY (id_usuario) REFERENCES federacion.usuario(id_usuario) ON DELETE CASCADE ON UPDATE CASCADE
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.historico (
	id_historico int auto_increment NOT NULL,
	id_jugador int NOT NULL,
	nombre_equipo_antiguo varchar(100) NOT NULL,
	nombre_equipo_nuevo varchar(100) NOT NULL,
	temporada varchar(100) NOT NULL,
	fecha date NULL,
	coste varchar(100) NULL,
	valor varchar(100) NULL,
	CONSTRAINT historico_unique UNIQUE KEY (id_jugador,nombre_equipo_antiguo,nombre_equipo_viejo,temporada,fecha),
	CONSTRAINT historico_pk PRIMARY KEY (id_historico),
	CONSTRAINT historico_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4


CREATE TABLE federacion.informe (
	id_informe int auto_increment NOT NULL,
	id_jugador int NOT NULL,
	id_usuario int NULL,
	fecha timestamp DEFAULT current_timestamp on update current_timestamp NOT NULL,
	visibilidad int DEFAULT 2 NOT NULL,
	valoracion int DEFAULT 0 NOT NULL,
	conclusion text NULL,
	tecnico int DEFAULT 0 NOT NULL,
	notas_tecnico text NULL,
	tactico int DEFAULT 0 NOT NULL,
	notas_tactico text NULL,
	fisico int DEFAULT 0 NOT NULL,
	notas_fisico text NULL,
	psicologico int DEFAULT 0 NOT NULL,
	notas_psicologico text NULL,
	media int DEFAULT 0 NOT NULL,
	CONSTRAINT informe_pk PRIMARY KEY (id_informe),
	CONSTRAINT informe_usuario_FK FOREIGN KEY (id_usuario) REFERENCES federacion.usuario(id_usuario) ON DELETE SET NULL ON UPDATE SET NULL,
	CONSTRAINT informe_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.contacto (
	id_contacto int auto_increment NOT NULL,
	id_jugador int NOT NULL,
	id_usuario int NULL,
	fecha date NOT NULL,
	interlocutor varchar(100) NULL,
	medio varchar(100) NULL,
	visibilidad int DEFAULT 2 NOT NULL,
	resumen text NOT NULL,
	CONSTRAINT contacto_pk PRIMARY KEY (id_contacto),
	CONSTRAINT contacto_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT contacto_usuario_FK FOREIGN KEY (id_usuario) REFERENCES federacion.usuario(id_usuario) ON DELETE SET NULL ON UPDATE SET NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.liga_favorita (
	id_usuario int NOT NULL,
	id_liga int NOT NULL,
	CONSTRAINT liga_favorita_pk PRIMARY KEY (id_liga,id_usuario),
	CONSTRAINT liga_favorita_usuario_FK FOREIGN KEY (id_usuario) REFERENCES federacion.usuario(id_usuario) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT liga_favorita_liga_FK FOREIGN KEY (id_liga) REFERENCES federacion.liga(id_liga) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.equipo_favorito (
	id_usuario int NOT NULL,
	id_equipo int NOT NULL,
	CONSTRAINT equipo_favorito_pk PRIMARY KEY (id_usuario,id_equipo),
	CONSTRAINT equipo_favorito_usuario_FK FOREIGN KEY (id_usuario) REFERENCES federacion.usuario(id_usuario) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT equipo_favorito_equipo_FK FOREIGN KEY (id_equipo) REFERENCES federacion.equipo(id_equipo) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.tarea (
	id_tarea int auto_increment NOT NULL,
	nombre varchar(100) NOT NULL,
	descripcion text NOT NULL,
	id_usuario_destino int NOT NULL,
	fecha_creacion timestamp DEFAULT current_timestamp on update current_timestamp NOT NULL,
	fecha_limite timestamp DEFAULT current_timestamp on update current_timestamp NOT NULL,
	estado tinyint(1) DEFAULT 0 NOT NULL,
	visibilidad int DEFAULT 2 NOT NULL,
	id_usuario_creador int NULL,
	id_jugador int NULL,
	id_equipo int NULL,
	CONSTRAINT tarea_pk PRIMARY KEY (id_tarea),
	CONSTRAINT tarea_usuario_FK FOREIGN KEY (id_usuario_destino) REFERENCES federacion.usuario(id_usuario) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT tarea_usuario_FK_1 FOREIGN KEY (id_usuario_creador) REFERENCES federacion.usuario(id_usuario) ON DELETE SET NULL ON UPDATE SET NULL,
	CONSTRAINT tarea_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT tarea_equipo_FK FOREIGN KEY (id_equipo) REFERENCES federacion.equipo(id_equipo) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE federacion.jugador_racha (
	id_racha int auto_increment NOT NULL,
	id_jugador int NOT NULL,
	racha int DEFAULT 0 NOT NULL,
	id_liga int NOT NULL,
	CONSTRAINT jugador_racha_pk PRIMARY KEY (id_racha),
	CONSTRAINT jugador_racha_jugador_FK FOREIGN KEY (id_jugador) REFERENCES federacion.jugador(id_jugador) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT jugador_racha_liga_FK FOREIGN KEY (id_liga) REFERENCES federacion.liga(id_liga) ON DELETE RESTRICT ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;









USE federacion;
TRUNCATE TABLE `campo`;
TRUNCATE TABLE `club`;
TRUNCATE TABLE `contacto`;
TRUNCATE TABLE `contrato`;
TRUNCATE TABLE `equipo`;
TRUNCATE TABLE `equipo_favorito`;
TRUNCATE TABLE `equipo_liga`;
TRUNCATE TABLE `historico`;
TRUNCATE TABLE `incidencia`;
TRUNCATE TABLE `informe`;
TRUNCATE TABLE `jugador`;
TRUNCATE TABLE `jugador_liga`;
TRUNCATE TABLE `jugador_lista`;
TRUNCATE TABLE `jugador_pais`;
TRUNCATE TABLE `jugador_partido`;
TRUNCATE TABLE `jugador_posicion`;
TRUNCATE TABLE `liga`;
TRUNCATE TABLE `liga_favorita`;
TRUNCATE TABLE `lista`;
TRUNCATE TABLE `localidad`;
TRUNCATE TABLE `pais`;
TRUNCATE TABLE `partido`;
TRUNCATE TABLE `posicion`;
TRUNCATE TABLE `representante`;
TRUNCATE TABLE `representante_fav`;
TRUNCATE TABLE `tarea`;
TRUNCATE TABLE `usuario`;