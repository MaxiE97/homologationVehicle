# Para usar este script, necesitas instalar odfpy:
# pip install odfpy

import sys
import os
from odf.opendocument import load
from odf import text, teletype

def buscar_y_reemplazar_en_odt(ruta_archivo, diccionario_reemplazos, ruta_salida=None):
    """
    Busca y reemplaza marcadores en un archivo ODT.
    
    Args:
        ruta_archivo: Ruta al archivo ODT original
        diccionario_reemplazos: Diccionario con {marcador: reemplazo}
        ruta_salida: Ruta donde guardar el archivo modificado (opcional)
    """
    if not os.path.exists(ruta_archivo):
        print(f"Error: No se encontró el archivo {ruta_archivo}")
        return False
    
    # Si no se especifica ruta de salida, usamos un nombre predeterminado
    if ruta_salida is None:
        nombre_base, extension = os.path.splitext(ruta_archivo)
        ruta_salida = f"{nombre_base}_modificado{extension}"
    
    # Cargar el documento
    print(f"Cargando documento: {ruta_archivo}")
    doc = load(ruta_archivo)
    
    # Obtener todos los párrafos del documento
    parrafos = doc.getElementsByType(text.P)
    
    # Contador para seguir el progreso
    reemplazos_realizados = {marcador: 0 for marcador in diccionario_reemplazos}
    total_reemplazos = 0
    
    # Procesar cada párrafo
    for parrafo in parrafos:
        # Obtener el texto del párrafo
        contenido = teletype.extractText(parrafo)
        modificado = False
        
        # Verificar si algún marcador está en el contenido
        for marcador, reemplazo in diccionario_reemplazos.items():
            if marcador in contenido:
                # Aquí solo estamos detectando, no reemplazando todavía
                print(f"Encontrado '{marcador}' en un párrafo")
                reemplazos_realizados[marcador] += 1
                total_reemplazos += 1
                modificado = True
    
    print(f"\nResumen de marcadores encontrados:")
    for marcador, cantidad in reemplazos_realizados.items():
        if cantidad > 0:
            print(f"  - '{marcador}': {cantidad} ocurrencias")
    
    print(f"\nTotal de marcadores encontrados: {total_reemplazos}")
    print("\nNota: Este script solo detecta los marcadores, no los reemplaza aún.")
    print("Para implementar el reemplazo, necesitaríamos desarrollar lógica adicional.")
    
    return True

# Ejemplo de uso
if __name__ == "__main__":
    # Ruta al archivo ODT
    ruta_archivo = "utils/planillaIngles.odt"
    
    # Ejemplo de diccionario de reemplazos (marcadores a buscar y sus reemplazos)
    # Aquí solo estamos buscando, no reemplazando todavía
    diccionario_reemplazos = {
        "{{B1}}": "Valor1",
        "{{B2}}": "Valor2",
        "{{B30}}": "Valor30",  # Uno de los que faltaban
        "{{B36}}": "Valor36",  # Uno de los que faltaban
        "{{B37}}": "Valor37",  # Uno de los que faltaban
        "{{B39}}": "Valor39",  # Uno de los que faltaban
        "{{B40}}": "Valor40",  # Uno de los que faltaban
    }
    
    buscar_y_reemplazar_en_odt(ruta_archivo, diccionario_reemplazos)