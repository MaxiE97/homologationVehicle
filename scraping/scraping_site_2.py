from .base_scraper import BaseScraper
import pandas as pd
from typing import List, Dict, Tuple
from bs4 import BeautifulSoup
import re


class Site2Scraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.search_configs = [
            {
                'container_class': 'col-sm-7 cocInfo',
                'value_class': 'col-sm-5',
                'identifiers': [
                    '40 Length', '41 Width', '42 Height', '44 Distance axis 1-2',
                    '43 Überhange f/b', '52 Netweight', '55 Roof load',
                    '57 braked', '58 unbraked', '67 Support load',
                    '47 Track Axis 1', '48 Track Axis 2'
                ],
                'next_sibling': True
            },
            {
                'container_class': 'col-sm-5 cocInfo',
                'value_class': 'col-sm-7',
                'identifiers': [
                    '14 Axles/Wheels', '25 Brand / Type', '27 Capacity:','26 Design type',
                    '28 Power / n',  '16 Final drive'
                ],
                'next_sibling': True
            },
            {
                'container_class': 'col-sm-2 cocInfo',
                'value_class': 'col-sm-2',
                'identifiers': ['Wet Weigh Kg'],
                'next_sibling': True,
                'element_type': 'label'
            }
        ]
#            <div class="col-sm-5 cocInfo">16 Final drive</div><div class="col-sm-7">Front wheel </div>

    def extract_data_by_config(self, soup: BeautifulSoup, config: Dict) -> List[Tuple[str, str]]:
        """Extrae datos según la configuración proporcionada."""
        data = []
        element_type = config.get('element_type', 'div')
        elements = soup.find_all(element_type, class_=config['container_class'])

        for element in elements:
            text = element.get_text(strip=True)
            if any(identifier in text for identifier in config['identifiers']):
                value_element = element.find_next(element_type, class_=config['value_class'])
                if value_element:
                    value = value_element.get_text(strip=True)
                    data.append((text, value))
        return data

    def extract_axle_guarantees(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
        """Extrae específicamente las garantías de ejes."""
        data = []
        main_element = soup.find('div', class_='col-sm-6 cocInfo', string='54 Axle guarantees')

        if main_element:
            # Extraer garantía delantera (v.)
            v_element = main_element.find_next('div', class_='col-sm-1 cocInfo', string='v.')
            if v_element:
                value_v = v_element.find_next('div', class_='col-sm-5')
                if value_v:
                    data.append((f"54 Axle guarantees v.", value_v.get_text(strip=True)))

            # Extraer garantía trasera (b.)
            b_element = main_element.find_next('div', class_='offset-sm-6 col-sm-1 cocInfo', string='b.')
            if b_element:
                value_b = b_element.find_next('div', class_='col-sm-5')
                if value_b:
                    data.append((f"54 Axle guarantees b.", value_b.get_text(strip=True)))

        return data

    def extract_tow_hitch_info(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
      """Extrae la información del enganche de remolque (56) de la sección de Remarks."""
      data = []

      # Buscar la sección de Remarks
      remarks_header = soup.find('div', string='Remarks')

      if remarks_header:
          # Buscar el elemento pre que contiene las observaciones
          pre_element = remarks_header.find_next('pre')

          if pre_element:
              pre_html = str(pre_element)
              # Buscar el texto que comienza con "56)" hasta el primer <br>
              match = re.search(r'tszeichen:(.*?)(?:<br\s*\/?>|$)', pre_html, re.DOTALL)

              if match:
                hitch_info = match.group(1).strip()
                data.append(("Tow hitch", hitch_info))

      return data

    def extract_vmax_info(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
      """Extrae información de VMax mecánica y automática y las combina en un solo valor."""
      data = []

      # Buscar el elemento que contiene "19 Vehicle VMax mech."
      vmax_element = soup.find('div', class_='col-sm-6 cocInfo', string=lambda s: '19 Vehicle VMax mech.' in s if s else False)

      if vmax_element:
          # Extraer valor mecánico
          mech_value_element = vmax_element.find_next('div', class_='col-sm-1 no-gutters')
          mech_value = mech_value_element.get_text(strip=True) if mech_value_element else ""

          # Extraer valor automático
          autom_label_element = vmax_element.find_next('div', class_='col-sm-2 cocInfo', string=lambda s: 'autom.' in s if s else False)
          autom_value = ""
          if autom_label_element:
              autom_value_element = autom_label_element.find_next('div', class_='col-sm-3')
              autom_value = autom_value_element.get_text(strip=True) if autom_value_element else ""

          # Crear un valor combinado simple
          combined_value = f"mech {mech_value} - autom {autom_value}"
          data.append(("19 Vehicle VMax", combined_value))

      return data




    def extract_emissions_data(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
        """Extrae información de emisiones usando hermanos directos para separar encabezados y datos.

        Se asume que dentro de un bloque (div.row.cocRow):
          - El primer elemento es el título "72 Emissions".
          - Los 8 siguientes son los encabezados.
          - Los elementos restantes se agrupan en bloques de 8 datos cada uno.
          Si existen dos grupos, se asigna un sufijo según la primera celda de cada grupo.
        """
        data = []
        # Buscar el elemento con el título "72 Emissions"
        emissions_header = soup.find('div', string='72 Emissions')
        if emissions_header:
            # Obtener el contenedor (la fila completa)
            block = emissions_header.find_parent("div", class_="row cocRow")
            if block:
                # Obtener todos los hijos directos de la fila
                siblings = block.find_all("div", recursive=False)
                try:
                    idx = siblings.index(emissions_header)
                except ValueError:
                    idx = 0
                # Los 8 siguientes elementos son los encabezados
                headers = [sib.get_text(strip=True) for sib in siblings[idx+1: idx+9]]
                # Los elementos restantes son las celdas de datos
                data_cells = siblings[idx+9:]
                if headers and len(data_cells) % len(headers) == 0:
                    num_groups = len(data_cells) // len(headers)
                    for i in range(num_groups):
                        group_cells = data_cells[i*len(headers):(i+1)*len(headers)]
                        group_values = [cell.get_text(strip=True) for cell in group_cells]
                        suffix = ""
                        if num_groups == 2:
                            first_value = group_values[0].lower()
                            if first_value.startswith("m"):
                                suffix = " (mec)"
                            elif first_value.startswith("a"):
                                suffix = " (autom)"
                        for header, value in zip(headers, group_values):
                            key = f"72 Emissions - {header}{suffix}"
                            data.append((key, value))
        return data



    def extract_transmission_info(self, soup: BeautifulSoup, transmissionManual: bool = None) -> List[Tuple[str, str]]:
      """Extrae la información de '18 Transmission/IA' según la opción indicada por transmissionManual.

      Si transmissionManual es True, se extrae el primer bloque (por ejemplo, la opción manual).
      Si es False, se extrae el segundo bloque (por ejemplo, la opción automática).
      Si no se indica, se toma por defecto el primer bloque.
      """
      data = []
      # Buscar el encabezado "18 Transmission/IA"
      transmission_header = soup.find('div', class_='col-sm-5 cocInfo', string=lambda s: s and '18 Transmission/IA' in s)

      if transmission_header:
          # Extraer el primer bloque de datos
          first_data_div = transmission_header.find_next('div', class_='col-sm-7')
          first_value = first_data_div.get_text(strip=True) if first_data_div else ""

          # Buscar el siguiente bloque de datos asociado (por ejemplo, en la sección "Assignment")
          assignment_header = first_data_div.find_next('div', class_='col-sm-5 cocInfo', string=lambda s: s and 'Assignment' in s)
          second_value = ""
          if assignment_header:
              second_data_div = assignment_header.find_next('div', class_='col-sm-7')
              second_value = second_data_div.get_text(strip=True) if second_data_div else ""

          # Decidir cuál valor extraer según transmissionManual
          if transmissionManual is not None:
              value = first_value if transmissionManual else second_value
          else:
              # Valor por defecto si no se especifica la variable: tomar el primero
              value = first_value

          data.append(("18 Transmission/IA", value))

      return data





    def scrape(self, url: str, transmissionManual: bool = None) -> pd.DataFrame:
      """Método principal para realizar el scraping, con opción de especificar la transmisión."""
      soup = self.fetch_page(url)
      all_data = []

      # Extraer datos según las configuraciones existentes
      for config in self.search_configs:
          all_data.extend(self.extract_data_by_config(soup, config))

      all_data.extend(self.extract_axle_guarantees(soup))
      all_data.extend(self.extract_tow_hitch_info(soup))
      all_data.extend(self.extract_vmax_info(soup))
      all_data.extend(self.extract_emissions_data(soup))

      # Extraer información de Transmission/IA con la opción indicada
      all_data.extend(self.extract_transmission_info(soup, transmissionManual))

      return pd.DataFrame(all_data, columns=['Key', 'Value'])
