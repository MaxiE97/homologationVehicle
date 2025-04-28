import pandas as pd
from typing import Dict, List, Optional
from dataclasses import dataclass
import re

@dataclass
class VehicleDataConfig:
    """Configuración para la transformación de datos del vehículo."""
    column_mapping: Dict[str, str]
    ordered_keys: List[str]

class VehicleDataTransformer_site1:
    """Clase para transformar datos de vehículos."""

    def __init__(self, config: VehicleDataConfig):
        self.config = config

    def transform(self, df_input: pd.DataFrame) -> pd.DataFrame:
        """Método principal que orquesta la transformación de datos."""
        df = df_input.copy()
        df = self._rename_columns(df)
        df = self.clean_values(df)
        df = self._process_axle_wheel(df)
        df = self._process_Axles_track(df)
        df = self._process_Axles_distribution(df)
        df = self._process_maximum_mass_trailer(df)
        df = self._emissions_standard(df)
        df = self.exhaust_emission(df)
        df = self.clean_particulates(df)
        df = self.clean_smoke(df)
        df = self.nedc_co_values(df)
        df = self.nedc_fuel_consumption(df)
        df = self.wltp_co_values(df)
        df = self.wltp_fuel_consumption_values(df)
        df = self.stationary_engine_speed(df)




        df = self._add_missing_keys(df)
        df = self._sort_and_clean(df)
        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renombra las columnas según el mapeo configurado."""
        df["Key"] = df["Key"].map(lambda x: self.config.column_mapping.get(x, x))
        return df

    def _process_axle_wheel(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa y combina la información de los ejes y ruedas."""
        if "wheel" in df["Key"].values:
            wheel = df[df["Key"] == "wheel"]["Value"].values[0]

            new_value = f"{2}/{wheel}"

            new_row = pd.DataFrame({
                    "Key": ["Number of axles / wheels"],
                    "Value": [f"{new_value}"]
                })

            df = pd.concat([df, new_row], ignore_index=True)
        return df






    def _process_Axles_track(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa y combina la información de las bases de los ejes."""
        if "Axle track  1" in df["Key"].values and "Axle track  2" in df["Key"].values:
          axle_1 = df[df["Key"] == "Axle track  1"]["Value"].values[0]
          axle_2 = df[df["Key"] == "Axle track  2"]["Value"].values[0]

          new_value = f"{axle_1}/{axle_2}"

          new_row = pd.DataFrame({
                  "Key": ["Axle(s) track – 1 / 2"],
                  "Value": [f"{new_value}"]
              })

          df = pd.concat([df, new_row], ignore_index=True)
        return df

    def _process_Axles_distribution(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa y combina la información de la distribución de los ejes."""
        if "Distribution of this mass among the axles – 1" in df["Key"].values and "Distribution of this mass among the axles – 2" in df["Key"].values:
          axle_1 = df[df["Key"] == "Distribution of this mass among the axles – 1"]["Value"].values[0]
          axle_2 = df[df["Key"] == "Distribution of this mass among the axles – 2"]["Value"].values[0]

          new_value = f"{axle_1}/{axle_2}"

          new_row = pd.DataFrame({
                  "Key": ["Distribution of this mass among the axles – 1 / 2"],
                  "Value": [f"{new_value}"]
              })

          df = pd.concat([df, new_row], ignore_index=True)

          new_row_2 = pd.DataFrame({
                  "Key": ["Technically permissible max mass on each axle – 1 / 2"],
                  "Value": [f"{new_value}"]
              })

          df = pd.concat([df, new_row_2], ignore_index=True)


        return df


    def _emissions_standard(self, df: pd.DataFrame) -> pd.DataFrame:
        """Agrega la palabra EURO a la emisión estándar"""
        df.loc[df["Key"] == "Emissions standard", "Value"] = df["Value"].apply(lambda x: f"EURO {x}")
        return df





    def _process_maximum_mass_trailer(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa y combina la información del peso máximo del camión."""
        if "Braked" in df["Key"].values and "Unbraked" in df["Key"].values:
          braked = df[df["Key"] == "Braked"]["Value"].values[0]
          unbraked = df[df["Key"] == "Unbraked"]["Value"].values[0]

          new_value = f"{braked}/{unbraked}"

          new_row = pd.DataFrame({
                  "Key": ["Maximum mass of trailer – braked / unbraked"],
                  "Value": [f"{new_value}"]
              })
          df = pd.concat([df, new_row], ignore_index=True)

        return df

    def exhaust_emission(self, df: pd.DataFrame) -> pd.DataFrame:
      """Renombra la clave y convierte a mayúsculas el valor solo para los registros específicos."""
      mask = df["Key"] == "Brandstof #1 - Milieuklasse licht"
      df.loc[mask, "Key"] = "Exhaust emission"
      df.loc[mask, "Value"] = df.loc[mask, "Value"].str.upper()
      return df




    def clean_values(self, df: pd.DataFrame) -> pd.DataFrame:
        def process_value(value):
            if pd.isna(value):  # Manejar valores NaN
                return value


            # Eliminar puntos en unidades específicas (como "kg" o "cm³")
            for unit in ['kg', 'cm³', 'dB(A)']:
                if unit in value:
                    value = value.replace('.', '')
                    break

            # Eliminar espacios y otras unidades específicas
            for unit in ['kg', 'cm³', 'dB(A)']:
                if unit in value:
                    value = value.replace(unit, '').strip()


            # Convertir cm a mm usando regex
            if 'cm' in value:
                # Encuentra el número antes de "cm" y multiplica por 10
                value = re.sub(r'(\d+)(?:\.?\d*)\s*cm',
                              lambda m: str(int(float(m.group(1)) * 10)),
                              value)

            return value

        # Aplicar la transformación a la columna seleccionada
        df["Value"] = df["Value"].astype(str).apply(process_value)
        return df

    def clean_particulates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia el valor de 'Emissions particulates', eliminando 'g/km' y dividiendo por 1000.
        Si no existe el registro, lo crea con un valor de "0.00001" (como string).
        """
        mask = df["Key"] == "Emissions particulates"

        if mask.any():
            # Si existe, limpiamos el valor, lo redondeamos a 6 decimales y lo convertimos en string
            df.loc[mask, "Value"] = (df.loc[mask, "Value"]
                                    .str.replace(" g/km", "", regex=False)
                                    .astype(float) / 1000).round(6).astype(str)
        else:
            # Si no existe, lo creamos con el valor predeterminado como string
            new_row = pd.DataFrame({"Key": ["Emissions particulates"], "Value": ["0.00001"]})
            df = pd.concat([df, new_row], ignore_index=True)

        return df



    def clean_smoke(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia el valor de 'Smoke', eliminando 'g/km' y formateándolo a 2 decimales."""
        mask = df["Key"] == "Smoke"
        df.loc[mask, "Value"] = df.loc[mask, "Value"].str.replace(" g/km", "", regex=False).astype(float).map("{:.2f}".format)
        return df

    def nedc_co_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia el valor de 'NEDC CO2 combined' eliminando 'g/km' y crea dos nuevos registros:
        - 'NEDC CO2 urban conditions' con el valor original + 12
        - 'NEDC CO2 extra-urban conditions' con el valor original - 12
        Los valores se mantienen como enteros.
        """
        mask = df["Key"] == "NEDC CO2 combined"

        # Limpiamos el valor, eliminando " g/km", lo convertimos a float y luego a entero
        df.loc[mask, "Value"] = df.loc[mask, "Value"].str.replace(" g/km", "", regex=False).astype(float).astype(int)

        # Creamos los nuevos registros basándonos en el valor limpio
        new_rows = []
        for valor in df.loc[mask, "Value"]:
            new_rows.append({
                "Key": "NEDC CO2 urban conditions",
                "Value": int(valor + 12)
            })
            new_rows.append({
                "Key": "NEDC CO2 extra-urban conditions",
                "Value": int(valor - 12)
            })

        # Concatenamos los nuevos registros al DataFrame original
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        return df

    import re

    def nedc_fuel_consumption(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia el valor de los registros de consumo NEDC.

        Para cada registro con key:
          - 'NEDC Fuel consumption combined'
          - 'NEDC Fuel consumption urban conditions'
          - 'NEDC Fuel consumption extra-urban conditions'

        Si en el valor se encuentra la palabra "liter", extrae el número anterior,
        reemplaza la coma decimal por punto y lo convierte a número.
        En caso de que el valor ya sea numérico (como en '7.4'), lo deja tal cual.
        """
        # Selecciona todos los registros que contengan 'NEDC Fuel consumption' en la key
        mask = df["Key"].str.contains("NEDC Fuel consumption", na=False)

        import re
        def extract_value(text):
            if isinstance(text, str) and "liter" in text:
                # Busca el número antes de "liter", considerando que puede usar coma como separador decimal
                match = re.search(r'([\d,]+)\s*liter', text)
                if match:
                    return float(match.group(1).replace(",", "."))
            try:
                # Si no hay "liter", intenta convertir directamente el valor a float
                return float(text)
            except Exception:
                return text

        df.loc[mask, "Value"] = df.loc[mask, "Value"].apply(extract_value)
        return df

    def wltp_co_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia el valor de 'WLTP CO2 combined' eliminando ' g/km' y crea nuevos registros:

        - 'WLTP CO2 Low' con valor = original + 6
        - 'WLTP CO2 Medium' con valor = original - 3
        - 'WLTP CO2 High' con valor = original - 6
        - 'WLTP CO2 Maximum Value' con valor = original + 3

        El registro 'WLTP CO2 combined' permanece con su valor original (limpio).
        """
        mask = df["Key"] == "WLTP CO2 combined"

        # Limpiar el valor eliminando " g/km" y convertirlo a entero
        df.loc[mask, "Value"] = df.loc[mask, "Value"].str.replace(" g/km", "", regex=False).astype(float).astype(int)

        # Obtener el/los valor(es) originales
        original_values = df.loc[mask, "Value"].tolist()
        new_rows = []
        for valor in original_values:
            new_rows.append({"Key": "WLTP CO2 Low", "Value": valor + 6})
            new_rows.append({"Key": "WLTP CO2 Medium", "Value": valor - 3})
            new_rows.append({"Key": "WLTP CO2 High", "Value": valor - 6})
            new_rows.append({"Key": "WLTP CO2 Maximum Value", "Value": valor + 3})

        # Agregamos los nuevos registros al DataFrame original
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        return df



    def wltp_fuel_consumption_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia el valor de 'WLTP Fuel consumption combined', extrayendo el número antes de 'liter'
        y reemplazando la coma decimal por punto. Luego, crea nuevos registros:

        - 'WLTP Fuel consumption Low' con valor = original + 0.6
        - 'WLTP Fuel consumption Medium' con valor = original - 0.3
        - 'WLTP Fuel consumption High' con valor = original - 0.6
        - 'WLTP Fuel consumption Maximum Value' con valor = original + 0.3

        El registro 'WLTP Fuel consumption combined' se mantiene con su valor original (limpio).
        """
        mask = df["Key"] == "WLTP Fuel consumption combined"

        def extract_value(text):
            if isinstance(text, str) and "liter" in text:
                # Busca el número antes de "liter" y reemplaza la coma por punto
                match = re.search(r'([\d,]+)\s*liter', text)
                if match:
                    return float(match.group(1).replace(",", "."))
            try:
                return float(text)
            except Exception:
                return text

        # Limpia el valor para que quede como 7.9
        df.loc[mask, "Value"] = df.loc[mask, "Value"].apply(extract_value)

        # Obtiene el/los valor(es) originales para generar los nuevos registros
        original_values = df.loc[mask, "Value"].tolist()
        new_rows = []
        for valor in original_values:
            new_rows.append({
                "Key": "WLTP Fuel consumption Low",
                "Value": round(valor + 0.6, 1)
            })
            new_rows.append({
                "Key": "WLTP Fuel consumption Medium",
                "Value": round(valor - 0.3, 1)
            })
            new_rows.append({
                "Key": "WLTP Fuel consumption High",
                "Value": round(valor - 0.6, 1)
            })
            new_rows.append({
                "Key": "WLTP Fuel consumption Maximum Value",
                "Value": round(valor + 0.3, 1)
            })

        # Agrega los nuevos registros al DataFrame original
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        return df


    def stationary_engine_speed(self, df: pd.DataFrame) -> pd.DataFrame:
        """Crea un nuevo registro a partir de los registros 'Stationary' y 'Engine speed'.

        El nuevo registro tendrá:
          Key: "Stationary (dB(A)) at engine speed"
          Value: "<valor de Stationary> at <valor de Engine speed>"
        """
        # Filtra los registros correspondientes
        stationary_mask = df["Key"] == "Stationary"
        engine_mask = df["Key"] == "Engine speed"

        # Extrae los valores (se asume que cada registro es único)
        if not df.loc[stationary_mask, "Value"].empty and not df.loc[engine_mask, "Value"].empty:
            stationary_val = df.loc[stationary_mask, "Value"].iloc[0]
            engine_val = df.loc[engine_mask, "Value"].iloc[0]

            # Crea el nuevo registro con el formato solicitado
            new_value = f"{stationary_val} at {engine_val}"
            new_row = {"Key": "Stationary (dB(A)) at engine speed", "Value": new_value}

            # Agrega el nuevo registro al DataFrame
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        return df



    def _add_missing_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Añade información sobre claves faltantes, asegurando que se incluyan todas las ordered_keys."""
        missing_rows = [
            {"Key": key, "Value": "None"}
            for key in self.config.ordered_keys if key not in df["Key"].values
        ]
        if missing_rows:
            df = pd.concat([df, pd.DataFrame(missing_rows)], ignore_index=True)
        return df



    def _sort_and_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ordena y limpia el DataFrame final, manteniendo solo las primeras 30 filas.
        """
        df["Key"] = pd.Categorical(
            df["Key"],
            categories=self.config.ordered_keys,
            ordered=True
        )
        return df.sort_values("Key").head(70).reset_index(drop=True)

    @staticmethod
    def _get_max_value(value: str) -> str:
        """Obtiene el valor máximo de un rango."""
        return value.split("-")[-1].strip()

    @staticmethod
    def _get_value_slash(value: str, spot: int) -> str:
        """Obtiene el valor en un lugar entre las barras"""
        return value.split("/")[spot].strip()

    @staticmethod
    def _get_max_from_pair(value: str, delimitador: str) -> int:
        """Obtiene el máximo valor de un par de números separados por un delimitador.
        Si solo se pasa un número, devuelve ese número."""
        if delimitador in value:
            num1, num2 = map(int, value.split(delimitador))
            return max(num1, num2)
        else:
            return int(value)



# Configuración predeterminada
DEFAULT_CONFIG_1 = VehicleDataConfig(
    column_mapping={
        "Eigenschappen - Aantal wielen": "wheel",
        "Afmetingen - Wielbasis": "Wheelbase",
        "As #1 - Spoorbreedte": "Axle track  1",
        "As #2 - Spoorbreedte": "Axle track  2",
        "Afmetingen - Lengte": "Length",
        "Afmetingen - Breedte": "Width",
        "Massa - Rijklaar gewicht": "Mass of the vehicle with bodywork in running order",
        "Massa - Technisch limiet massa": "Technically permissible maximum laden mass",
        "As #1 - Technisch limiet": "Distribution of this mass among the axles – 1",
        "As #2 - Technisch limiet": "Distribution of this mass among the axles – 2",
        "Trekkracht - Maximaal trekgewicht geremd":"Braked",
        "Trekkracht - Maximaal trekgewicht ongeremd":"Unbraked",
        "Massa - Maximum massa samenstelling": "Maximum mass of combination",
        "Algemeen - Merk":"Engine manufacturer",
        "Motor - Aantal cilinders": "Number and arrangement of cylinders",
        "Motor - Cilinderinhoud": "Capacity",
        "Brandstof #1 - Brandstof	": "Fuel",
        "Brandstof #1 - Vermogen": "Maximum net power",
        "Brandstof #1 - Geluidsniveau stationair": "Stationary",
        "Brandstof #1 - Geluidsniveau toerental": "Engine speed",
        "Brandstof #1 - Geluidsniveau rijdend": "Drive by",
        "Brandstof #1 - Emissieklasse": "Emissions standard",
        "Brandstof #1 - Milieuklasse licht": "Brandstof #1 - Milieuklasse licht",
        "Brandstof #1 - Uitstoot deeltjes WLTP": "Emissions particulates",
        "Brandstof #1 - Roetuitstoot NEDC": "Smoke",
        "Brandstof #1 - CO2-uitstoot gecombineerd NEDC": "NEDC CO2 combined",
        "Brandstof #1 - Brandstofverbruik gecombineerd NEDC": "NEDC Fuel consumption combined",
        "Brandstof #1 - Brandstofverbruik in stad NEDC": "NEDC Fuel consumption urban conditions",
        "Brandstof #1 - Brandstofverbruik op snelweg NEDC": "NEDC Fuel consumption extra-urban conditions",
        "Brandstof #1 - CO2-uitstoot gecombineerd WLTP": "WLTP CO2 combined",
        "Brandstof #1 - Brandstofverbruik gecombineerd WLTP": "WLTP Fuel consumption combined",
        "Algemeen - Merk": "Make",
        "Algemeen - Type": "Type",
        "Algemeen - Variant": "Variant",
        "Algemeen - Uitvoering": "Version",
        "Algemeen - Model": "Commercial name",
        "Algemeen - Typegoedkeuringsnummer":"Homologation number",



#29  Brandstof #1 - Brandstofverbruik gecombWLTPineerd   7,9 liter/100 km (12,7 km/liter)


    },
    ordered_keys=[
        "Number of axles / wheels",
        "Powered axles",
        "Wheelbase",
        "Axle(s) track – 1 / 2",
        "Length",
        "Width",
        "Height",
        "Rear overhang",
        "Mass of the vehicle with bodywork in running order",
        "Technically permissible maximum laden mass",
        "Distribution of this mass among the axles – 1 / 2",
        "Technically permissible max mass on each axle – 1 / 2",
        "Maximum permissible roof load",
        "Maximum mass of trailer – braked / unbraked",
        "Maximum mass of combination",
        "Maximum vertical load at the coupling point for a trailer",
        "Engine manufacturer",
        "Engine code as marked on the enginee",
        "Working principle",
        "Direct injection",
        "Pure electric",
        "Hybrid [electric] vehicle",
        "Number and arrangement of cylinders",
        "Capacity",
        "Fuel",
        "Maximum net power",
        "Clutch",
        "Gearbox",
        "Gear",
        "Final drive ratio",
        "EC type approval mark of couplind device if fitted",
        "Maximum speed",
        "Stationary (dB(A)) at engine speed",
        "Drive by",
        "Emissions standard",
        "Exhaust emission",
        "Emissions CO",
        "Emissions HC",
        "Emissions NOx",
        "Emissions HC NOx",
        "Emissions particulates",
        "Smoke",
        "NEDC CO2 urban conditions",
        "NEDC CO2 extra-urban conditions",
        "NEDC CO2 combined",
        "NEDC Fuel consumption urban conditions",
        "NEDC Fuel consumption extra-urban conditions",
        "NEDC Fuel consumption combined",
        "WLTP CO2 Low",
        "WLTP CO2 Medium",
        "WLTP CO2 High",
        "WLTP CO2 Maximum Value",
        "WLTP CO2 combined",
        "WLTP Fuel consumption Low",
        "WLTP Fuel consumption Medium",
        "WLTP Fuel consumption High",
        "WLTP Fuel consumption Maximum Value",
        "WLTP Fuel consumption combined",
        "Steering, method of assistance",
        "Suspension",
        "Brakes",
        "Type of body",
        "Number and configuration of doors",
        "Number and position of seats",
        "Make",
        "Type",
        "Variant",
        "Version",
        "Commercial name",
        "Homologation number",


    ]
)


