import pandas as pd
from typing import Dict, List, Optional
from dataclasses import dataclass
import re

@dataclass
class VehicleDataConfig:
    """Configuración para la transformación de datos del vehículo."""
    column_mapping: Dict[str, str]
    ordered_keys: List[str]

class VehicleDataTransformer_site3:
    """Clase para transformar datos de vehículos."""

    def __init__(self, config: VehicleDataConfig):
        self.config = config

    def transform(self, df_input: pd.DataFrame) -> pd.DataFrame:
        """Método principal que orquesta la transformación de datos."""
        df = df_input.copy()
        df = self._rename_columns(df)
        df = self._create_suspension(df)
        df = self._create_brakes(df)





        df = self._add_missing_keys(df)
        df = self._sort_and_clean(df)
        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renombra las columnas según el mapeo configurado."""
        df["Key"] = df["Key"].map(lambda x: self.config.column_mapping.get(x, x))
        return df
    

    def _create_suspension(self, df: pd.DataFrame) -> pd.DataFrame:
        
            if "Front suspension" in df["Key"].values and "Rear suspension" in df["Key"].values:
                front_val = df[df["Key"] == "Front suspension"]["Value"].values[0].split("-")[0].strip()
                rear_val = df[df["Key"] == "Rear suspension"]["Value"].values[0].split("-")[0].strip()

                
                new_row = pd.DataFrame({
                    "Key": ["Suspension"],
                    "Value": [f"{front_val}/{rear_val}"]
                })

            else:
            
                new_row = pd.DataFrame({
                        "Key": ["Suspension"],
                        "Value": [f"Independent type McPherson/Semi independent multilink"]
                    })
                    

            df = pd.concat([df, new_row], ignore_index=True)  

            

            return df


    def _create_brakes(self, df: pd.DataFrame) -> pd.DataFrame:
            
                if "Front brakes" in df["Key"].values and "Rear brakes" in df["Key"].values:
                    front_val = df[df["Key"] == "Front brakes"]["Value"].values[0]
                    rear_val = df[df["Key"] == "Rear brakes"]["Value"].values[0]

                    
                    new_row = pd.DataFrame({
                        "Key": ["Brakes"],
                        "Value": [f"{front_val}/{rear_val}"]
                    })

                else:
                
                    new_row = pd.DataFrame({
                            "Key": ["Brakes"],
                            "Value": [f"Ventilated discs/Ventilated discs"]
                        })
                        

                df = pd.concat([df, new_row], ignore_index=True)  

                

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
        Ordena y limpia el DataFrame final, manteniendo solo las primeras 59 filas.
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
DEFAULT_CONFIG_3 = VehicleDataConfig(
    column_mapping={
        "Type of body": "Type of body",
        "Steering, method of assistance": "Steering, method of assistance",
        "Number and configuration of doors": "Number and configuration of doors",
        "Number and position of seats": "Number and position of seats",
        
        



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


