# scraping/scraping_site_3.py

import pandas as pd
from bs4 import BeautifulSoup
# Importa la clase base desde el mismo directorio
from .base_scraper import BaseScraper

class Site3Scraper(BaseScraper):
    """
    Scraper específico para extraer datos de especificaciones de vehículos
    del sitio auto-data.net.
    """
    def scrape(self, url: str) -> pd.DataFrame:
        """
        Realiza el scraping de la URL dada y devuelve un DataFrame con los datos extraídos.
        """
        print(f"Iniciando scraping para el Sitio 3: {url}") # Mensaje informativo
        try:
            # Usa el método fetch_page de la clase base para obtener el soup
            soup = self.fetch_page(url)
        except Exception as e:
            # Si fetch_page falla, retorna un DataFrame vacío o maneja el error como prefieras
            print(f"Error al obtener la página para el Sitio 3 ({url}): {e}")
            return pd.DataFrame(columns=["Key", "Value"])

        # Mapeo de los textos de encabezado (th) en el HTML a los nombres de clave deseados
        key_mapping = {
            "Power steering": "Steering, method of assistance",
            "Body type": "Type of body",
            "Doors": "Number and configuration of doors",
            "Seats": "Number and position of seats",
            "Front suspension": "Front suspension",
            "Rear suspension": "Rear suspension",
            "Front brakes": "Front brakes",
            "Rear brakes": "Rear brakes",
            "Assisting systems": "Assisting systems",
        }

        extracted_data = []

        # Encontrar la tabla principal de detalles
        details_table = soup.find('table', class_='cardetailsout car2')

        if details_table:
            # Iterar sobre todas las filas (tr) de la tabla
            rows = details_table.find_all('tr')
            for row in rows:
                header = row.find('th')
                data_cell = row.find('td')

                if header and data_cell:
                    header_text = header.get_text(strip=True)

                    # Verificar si el texto del encabezado está en nuestro mapeo
                    if header_text in key_mapping:
                        target_key = key_mapping[header_text]

                        # Extraer el texto del td, limpiando posibles <br> y espacios extra
                        if header_text == "Assisting systems":
                            value_text = data_cell.find(string=True, recursive=False)
                            value_text = value_text.strip() if value_text else data_cell.get_text(strip=True)
                        else:
                            value_text = data_cell.get_text(strip=True)

                        extracted_data.append((target_key, value_text))
                        # print(f"S3 - Encontrado: '{target_key}' -> '{value_text}'") # Descomentar para depurar más
        else:
            print("Advertencia: No se encontró la tabla 'cardetailsout car2' en el Sitio 3.")

        # Convertir la lista de tuplas a un DataFrame de Pandas
        if not extracted_data:
             print("Advertencia: No se extrajeron datos del Sitio 3.")

        return pd.DataFrame(extracted_data, columns=["Key", "Value"])