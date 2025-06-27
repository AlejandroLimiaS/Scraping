import pickle
import os


posicion_a_minuto = {}


ancho = 360
alto = 472
num_cols = 10
num_filas = 13
anchura_icono = ancho // num_cols
altura_icono = alto // num_filas


for minuto in range(1, 121):
    x_pos = ((minuto - 1) % num_cols) * anchura_icono
    y_pos = ((minuto - 1) // num_cols) * altura_icono
    posicion_a_minuto[f"-{x_pos}px -{y_pos}px"] = minuto


ultima_y = (num_filas - 1) * altura_icono
posicion_a_minuto[f"0px {-ultima_y}px"] = "-"

directorio_script = os.path.dirname(os.path.abspath(__file__))
ruta_archivo = os.path.join(directorio_script, "pos_a_minuto.pkl")

with open(ruta_archivo, "wb") as fichero:
    pickle.dump(posicion_a_minuto, fichero)


print(posicion_a_minuto[f"-180px -144px"])
