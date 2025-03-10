import os
import pandas as pd
from odf.opendocument import load
from odf import text, teletype
from odf.style import Style, TextProperties
from odf.text import Span
import io
import re

class ODTExporter:
    def __init__(self, template_path):
        self.template_path = template_path
        
    def prepare_data_for_export(self, df):
        """
        Convierte los datos del DataFrame en un diccionario de reemplazo.
        """
        print("\n=== DATAFRAME ===")
        print(df)
        print("=== FIN DATAFRAME ===\n")
        
        valores_finales = df['Valor Final'].tolist()
        replacement_dict = {f'{{{{B{i+1}}}}}': str(valor) if pd.notna(valor) else '' 
                            for i, valor in enumerate(valores_finales)}
        
        print("\n=== DICCIONARIO DE REEMPLAZO ===")
        for key, value in replacement_dict.items():
            print(f"{key} -> '{value}'")
        print("=== FIN DICCIONARIO DE REEMPLAZO ===\n")
        
        return replacement_dict
                
    def find_markers_in_odt(self):
        """
        Busca y lista todos los marcadores {{B..}} en el documento ODT.
        """
        try:
            if not os.path.exists(self.template_path):
                raise FileNotFoundError(f"La plantilla {self.template_path} no existe.")
            
            doc = load(self.template_path)
            parrafos = doc.getElementsByType(text.P)
            unique_markers = set()
            for parrafo in parrafos:
                contenido = teletype.extractText(parrafo)
                markers = re.findall(r'\{\{B\d+\}\}', contenido)
                unique_markers.update(markers)
            
            print("\n=== MARCADORES ENCONTRADOS ===")
            for marker in sorted(unique_markers, key=lambda x: int(re.search(r'B(\d+)', x).group(1))):
                print(marker)
            print("=== FIN MARCADORES ENCONTRADOS ===\n")
                
            return unique_markers
            
        except Exception as e:
            print(f"Error al buscar marcadores en ODT: {repr(e)}")
            return None
            
    def export_to_odt(self, df):
        """
        Procesa el DataFrame y genera un ODT con los valores reemplazados.
        """
        replacement_dict = self.prepare_data_for_export(df)
        return self.replace_text_in_odt(replacement_dict)
        
    def replace_text_in_odt(self, replacement_dict):
        """
        Reemplaza los marcadores en el documento ODT y devuelve el documento modificado como bytes.
        Se procesa el texto completo de cada elemento (párrafos y spans) y, en caso de reemplazo,
        se inserta el texto envuelto en un Span con el estilo deseado (Liberation Serif, 8pt).
        """
        try:
            if not os.path.exists(self.template_path):
                raise FileNotFoundError(f"La plantilla {self.template_path} no existe.")
                
            doc = load(self.template_path)
            
            # Crear (o asegurarse de tener) un estilo para el texto reemplazado
            replacement_style_name = "replacementStyle"
            # Creamos un estilo de tipo "text" con la fuente y tamaño requeridos.
            new_style = Style(name=replacement_style_name, family="text")
            new_style.addElement(TextProperties(fontfamily="Liberation Serif", fontsize="8pt"))
            # Agregar el estilo a la sección de estilos del documento
            doc.styles.addElement(new_style)
            
            # Recolectar elementos de texto: párrafos y spans.
            elementos_texto = []
            for parrafo in doc.getElementsByType(text.P):
                elementos_texto.append(parrafo)
            for span in doc.getElementsByType(text.Span):
                elementos_texto.append(span)
                
            reemplazos_realizados = {marcador: 0 for marcador in replacement_dict}
            total_reemplazos = 0
            
            for elemento in elementos_texto:
                # Extraer el texto completo (fusiona nodos) del elemento.
                texto_completo = teletype.extractText(elemento)
                texto_original = texto_completo
                
                if any(marcador in texto_completo for marcador in replacement_dict):
                    print(f"Procesando elemento: '{texto_completo}'")
                    for marcador, reemplazo in replacement_dict.items():
                        ocurrencias = texto_completo.count(marcador)
                        if ocurrencias > 0:
                            texto_completo = texto_completo.replace(marcador, reemplazo)
                            reemplazos_realizados[marcador] += ocurrencias
                            total_reemplazos += ocurrencias
                            print(f"Reemplazando '{marcador}' por '{reemplazo}' - ocurrencias: {ocurrencias}")
                    
                    if texto_original != texto_completo:
                        # Eliminar todos los nodos hijos iterando sobre una copia de la lista
                        for nodo in list(elemento.childNodes):
                            try:
                                elemento.removeChild(nodo)
                            except Exception as e:
                                print(f"Error removiendo un nodo: {repr(e)}")
                        # En lugar de agregar texto plano, creamos un Span con el estilo deseado
                        nuevo_span = Span(stylename=replacement_style_name)
                        nuevo_span.addText(texto_completo)
                        elemento.addElement(nuevo_span)
            
            output_stream = io.BytesIO()
            doc.save(output_stream)
            output_stream.seek(0)
            
            print("\n=== RESUMEN DE REEMPLAZOS ===")
            for marcador, cantidad in sorted(reemplazos_realizados.items(), 
                                             key=lambda x: int(re.search(r'B(\d+)', x[0]).group(1))):
                print(f"  - '{marcador}': {cantidad} ocurrencias")
            print(f"\nTotal de reemplazos realizados: {total_reemplazos}")
            print("=== FIN RESUMEN DE REEMPLAZOS ===\n")
            
            marcadores_sin_reemplazar = [m for m, c in reemplazos_realizados.items() if c == 0]
            if marcadores_sin_reemplazar:
                print("\n=== ADVERTENCIA: MARCADORES NO REEMPLAZADOS ===")
                for marcador in marcadores_sin_reemplazar:
                    if marcador in replacement_dict and replacement_dict[marcador]:
                        print(f"El marcador '{marcador}' no fue reemplazado aunque tiene valor: '{replacement_dict[marcador]}'")
                print("=== FIN ADVERTENCIA ===\n")
            
            return output_stream.getvalue()
            
        except Exception as e:
            print(f"Error al reemplazar texto en ODT: {repr(e)}")
            import traceback
            traceback.print_exc()
            return None
