import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridUpdateMode, GridOptionsBuilder

# Importaciones de los sitios existentes
from scraping.scraping_site_1 import Site1Scraper
from scraping.scraping_site_2 import Site2Scraper
from data_transformation.transform_site1 import VehicleDataTransformer_site1, DEFAULT_CONFIG_1
from data_transformation.transform_site2 import VehicleDataTransformer_site2, DEFAULT_CONFIG_2

# --- NUEVO: Importaciones para el Sitio 3 ---
from scraping.scraping_site_3 import Site3Scraper
from data_transformation.transform_site3 import VehicleDataTransformer_site3, DEFAULT_CONFIG_3
# --- FIN NUEVO ---

from exportToFile import ODTExporter

class DataProcessor:
    """
    Clase para manejar el procesamiento y transformación de datos de vehículos
    """
    def __init__(self):
        self.site1_scraper = Site1Scraper()
        self.site2_scraper = Site2Scraper()
        # --- NUEVO: Inicializar scraper y transformer para Sitio 3 ---
        self.site3_scraper = Site3Scraper()
        # --- FIN NUEVO ---

        self.transformer_site1 = VehicleDataTransformer_site1(DEFAULT_CONFIG_1)
        self.transformer_site2 = VehicleDataTransformer_site2(DEFAULT_CONFIG_2)
        # --- NUEVO: ---
        self.transformer_site3 = VehicleDataTransformer_site3(DEFAULT_CONFIG_3)
        # --- FIN NUEVO ---

    def process_url(self, url, site_number, transmission_manual=None):
        """Procesa una URL y retorna los datos transformados."""
        try:
            if site_number == 1:
                data = self.site1_scraper.scrape(url)
                return self.transformer_site1.transform(data)
            elif site_number == 2: # <--- Cambiado de else a elif
                # Solo para el site 2 se utiliza el parámetro transmission_manual.
                data = self.site2_scraper.scrape(url, transmission_manual)
                return self.transformer_site2.transform(data)
            # --- NUEVO: Condición para Sitio 3 ---
            elif site_number == 3:
                data = self.site3_scraper.scrape(url)
                return self.transformer_site3.transform(data)
            # --- FIN NUEVO ---
            else:
                st.error(f"Número de sitio desconocido: {site_number}")
                return None
        except Exception as e:
            st.error(f"Error al procesar el Sitio {site_number} ({url}): {e}")
            return None

    @staticmethod
    def merge_dataframes(df1: pd.DataFrame | None, df2: pd.DataFrame | None, df3: pd.DataFrame | None) -> pd.DataFrame | None:
        """
        Combina hasta tres DataFrames manteniendo el orden original de df1
        y priorizando los valores (Sitio 2 > Sitio 1 > Sitio 3).
        """
        # Crear una lista de dataframes no nulos
        dfs = [df for df in [df1, df2, df3] if df is not None]

        if not dfs:
            return None # No hay dataframes para combinar

        # Tomar el primer dataframe como base para el orden y la fusión inicial
        merged_df = dfs[0].copy()
        merged_df['original_index'] = range(len(merged_df))
        # Asignar sufijo basado en qué dataframe es la base
        base_suffix_map = {id(df1): '_site1', id(df2): '_site2', id(df3): '_site3'}
        base_suffix = base_suffix_map.get(id(dfs[0]), '_base')
        merged_df = merged_df.rename(columns={'Value': f'Value{base_suffix}'})

        # Fusionar los dataframes restantes
        suffixes = ['_site1', '_site2', '_site3']
        processed_indices = {suffixes.index(base_suffix)} # Marcar el índice del df base como procesado

        for i, df_to_merge in enumerate([df1, df2, df3]):
             if df_to_merge is not None and id(df_to_merge) != id(dfs[0]): # Si no es el df base
                suffix_index = i
                if suffix_index not in processed_indices:
                    current_suffix = suffixes[suffix_index]
                    # Renombrar la columna 'Value' antes de fusionar para evitar conflictos
                    df_renamed = df_to_merge.rename(columns={'Value': f'Value{current_suffix}'})
                    merged_df = pd.merge(merged_df, df_renamed[['Key', f'Value{current_suffix}']], on='Key', how='outer')
                    processed_indices.add(suffix_index)


        # Rellenar el índice original para filas que solo existían en df2 o df3
        # Se usa un valor grande para ponerlos al final antes de ordenar
        merged_df['original_index'] = merged_df['original_index'].fillna(len(merged_df) + merged_df['original_index'].max())

        # Asegurar que todas las columnas de valor existan, rellenando con None si faltan
        for suffix in suffixes:
            col_name = f'Value{suffix}'
            if col_name not in merged_df.columns:
                merged_df[col_name] = None

        # --- Lógica ACTUALIZADA para 'Valor Final' con prioridad S2 > S1 > S3 ---
        def get_final_value(row):
            if pd.notna(row.get('Value_site2')) and row.get('Value_site2') != 'None':
                return row['Value_site2']
            elif pd.notna(row.get('Value_site1')) and row.get('Value_site1') != 'None':
                return row['Value_site1']
            elif pd.notna(row.get('Value_site3')) and row.get('Value_site3') != 'None':
                return row['Value_site3']
            # Fallback a la primera columna no nula si S2/S1/S3 son None/NaN
            elif pd.notna(row.get('Value_site2')): return row['Value_site2']
            elif pd.notna(row.get('Value_site1')): return row['Value_site1']
            elif pd.notna(row.get('Value_site3')): return row['Value_site3']
            else: return None # O '' si prefieres string vacío

        merged_df['Value_editable'] = merged_df.apply(get_final_value, axis=1)
        # --- FIN Lógica ACTUALIZADA ---

        # Renombrar columnas para la visualización final
        merged_df = merged_df.rename(columns={
            'Value_site1': 'Valor Sitio 1',
            'Value_site2': 'Valor Sitio 2',
            'Value_site3': 'Valor Sitio 3', # <-- Nueva columna
            'Value_editable': 'Valor Final'
        })

        # Ordenar y seleccionar columnas finales
        final_columns = ['Key', 'Valor Sitio 1', 'Valor Sitio 2', 'Valor Sitio 3', 'Valor Final']
        # Asegurarse de que las columnas existan antes de seleccionarlas
        final_columns = [col for col in final_columns if col in merged_df.columns]

        return merged_df.sort_values('original_index').drop('original_index', axis=1)[final_columns]


def init_session_state():
    """Inicializa las variables de estado de la sesión"""
    if 'df_site1' not in st.session_state:
        st.session_state.df_site1 = None
    if 'df_site2' not in st.session_state:
        st.session_state.df_site2 = None
    # --- NUEVO: Inicializar df_site3 ---
    if 'df_site3' not in st.session_state:
        st.session_state.df_site3 = None
    # --- FIN NUEVO ---

    if 'merged_df' not in st.session_state:
        st.session_state.merged_df = None
    if 'search_term' not in st.session_state:
        st.session_state.search_term = ""
    if 'grid_has_changes' not in st.session_state:
        st.session_state.grid_has_changes = False
    if 'previous_data' not in st.session_state:
        st.session_state.previous_data = None

    # Opciones de idioma (sin cambios)
    if 'language_options' not in st.session_state:
        st.session_state.language_options = {
            'Inglés': "utils/planillaIngles.odt",
            'Alemán': "utils/planillaAleman.odt",
            'Italiano': "utils/planillaItaliano.odt",
            'Francés': "utils/planillaFrances.odt",
            'Holandés': "utils/planillaHolandes.odt",
            'Portugués': "utils/planillaPortugues.odt"
        }
    if 'selected_language' not in st.session_state:
        st.session_state.selected_language = list(st.session_state.language_options.keys())[0]

    if 'language_key' not in st.session_state:
        st.session_state.language_key = 0

def change_language():
    """Función de callback para cambiar el idioma seleccionado"""
    st.session_state.selected_language = st.session_state.temp_language
    st.session_state.language_key += 1

def setup_page():
    """Configura la página y el diseño inicial"""
    st.set_page_config(layout="wide", page_title="Extracción de datos para homologación")
    st.markdown("<h1 style='text-align: center;'>Extracción de datos para homologación</h1>", unsafe_allow_html=True)

def render_url_inputs():
    """Renderiza los campos de entrada de URL"""
    st.subheader("Ingreso de URLs")
    # --- NUEVO: Usar 3 columnas para las URLs ---
    col1, col2, col3 = st.columns(3)
    with col1:
        url_site1 = st.text_input("URL Sitio 1 (Voertuig):", key="url1")
    with col2:
        url_site2 = st.text_input("URL Sitio 2 (Typenscheine):", key="url2")
    with col3:
        url_site3 = st.text_input("URL Sitio 3 (Auto-Data):", key="url3") # <--- Input para Sitio 3
    # --- FIN NUEVO ---

    # Opción de transmisión para site 2 (sin cambios)
    st.markdown("**Opción para Sitio 2 (Typenscheine):** Si ofrece dos tipos de transmisiones, seleccione la deseada.")
    transmission_option = st.selectbox(
        "Selecciona la opción de transmisión:",
        ("Por defecto", "Manual", "Automático"),
        key="transmission_option",
        index=0 # Asegurar que 'Por defecto' es la opción inicial
    )
    transmission_manual = None
    if transmission_option == "Manual":
        transmission_manual = True
    elif transmission_option == "Automático":
        transmission_manual = False

    # --- NUEVO: Retornar url_site3 ---
    return url_site1, url_site2, url_site3, transmission_manual
    # --- FIN NUEVO ---


def filter_dataframe(df, search_term):
    """Filtra el dataframe según el término de búsqueda"""
    if not search_term or df is None: # Añadir chequeo por si df es None
        return df
    # Asegurar que 'Key' existe y es de tipo string antes de filtrar
    if 'Key' in df.columns:
      mask = df['Key'].astype(str).str.contains(search_term, case=False, na=False)
      return df[mask].copy()
    else:
      return df # Retornar sin filtrar si no hay columna 'Key'

def render_aggrid(df):
    """Renderiza la tabla usando AgGrid"""
    if df is None:
        st.info("No hay datos combinados para mostrar.")
        return None

    df_display = df.reset_index(drop=False)
    gb = GridOptionsBuilder.from_dataframe(df_display)

    # Configurar columnas comunes
    gb.configure_column('index', header_name="Índice", hide=True)
    gb.configure_column('Key', header_name="Característica", editable=False, sortable=True, filter=True, minWidth=250, wrapText=True, autoHeight=True)

    # Configurar columnas de valores de sitios (solo si existen en el df)
    if 'Valor Sitio 1' in df_display.columns:
      gb.configure_column('Valor Sitio 1', header_name="Valor Sitio 1", editable=False, minWidth=150, wrapText=True, autoHeight=True)
    if 'Valor Sitio 2' in df_display.columns:
      gb.configure_column('Valor Sitio 2', header_name="Valor Sitio 2", editable=False, minWidth=150, wrapText=True, autoHeight=True)
    # --- NUEVO: Configurar columna Valor Sitio 3 ---
    if 'Valor Sitio 3' in df_display.columns:
      gb.configure_column('Valor Sitio 3', header_name="Valor Sitio 3", editable=False, minWidth=150, wrapText=True, autoHeight=True)
    # --- FIN NUEVO ---
    if 'Valor Final' in df_display.columns:
      gb.configure_column('Valor Final', header_name="Valor Final (Editable)", editable=True, minWidth=200, wrapText=True, autoHeight=True)

    # Configuraciones adicionales
    gb.configure_default_column(resizable=True) # Permitir redimensionar columnas
    gb.configure_grid_options(domLayout='normal') # Evitar altura excesiva
    gb.configure_selection(selection_mode='single', use_checkbox=False)
    gb.configure_grid_options(
        suppressFieldDotNotation=True,
        enableCellTextSelection=True,
        ensureDomOrder=True,
        # rowBuffer=100 # Puedes ajustar esto si tienes problemas de rendimiento
    )

    grid_options = gb.build()

    grid_response = AgGrid(
        df_display,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        fit_columns_on_grid_load=True, # Puede ser problemático con wrapText
        theme='alpine', # Otras opciones: 'streamlit', 'balham'
        height=600, # Aumentar altura si es necesario
        width='100%',
        allow_unsafe_jscode=True,
        key="data_grid",
        reload_data=True # Forzar recarga para reflejar cambios de estado
    )
    return grid_response


def update_dataframe_values(original_df, updated_data):
    """Actualiza el DataFrame original con los valores modificados"""
    if updated_data is None or updated_data.empty: # Comprobar si el DataFrame está vacío
        return original_df
    if original_df is None:
        return None # No se puede actualizar un df None

    updated_df = pd.DataFrame(updated_data)

    # Reindexar original_df temporalmente si 'index' no es el índice real
    original_indexed = original_df.set_index('index', drop=False) if 'index' in original_df.columns else original_df

    changes_made = False
    for _, row in updated_df.iterrows():
        try:
            # Usar el índice de la tabla AgGrid si existe, sino asumir que es el índice del df
            original_idx = row.get('index', row.name)
            new_value = row.get('Valor Final')

            # Asegurar que el índice existe en el DF original
            if original_idx in original_indexed.index:
                 # Comparar cuidadosamente, considerando tipos y NaN/None
                current_value = original_indexed.loc[original_idx, 'Valor Final']
                if (pd.isna(current_value) and pd.isna(new_value)) or (str(current_value) == str(new_value)):
                    continue # Sin cambios reales
                else:
                    # Aplicar cambio usando el índice correcto
                    original_indexed.loc[original_idx, 'Valor Final'] = new_value
                    changes_made = True
            # else: # El índice de AgGrid no corresponde a uno en el DF (puede pasar con filtrado)
                 # print(f"Advertencia: Índice {original_idx} no encontrado en el DF original durante la actualización.")

        except KeyError as e:
            st.warning(f"Error de clave al actualizar el registro con índice {original_idx}: {e}")
        except Exception as e: # Captura más genérica por si acaso
             st.error(f"Error inesperado al actualizar índice {original_idx}: {e}")

    if changes_made:
        st.session_state.grid_has_changes = True

    # Devolver el dataframe con el índice reseteado si lo modificamos
    return original_indexed.reset_index(drop=True) if 'index' in original_df.columns else original_indexed


# --- NUEVO: Actualizar firma de process_urls ---
def process_urls(url_site1, url_site2, url_site3, transmission_manual):
# --- FIN NUEVO ---
    """Procesa las URLs y actualiza los dataframes en session_state"""
    processor = DataProcessor()

    with st.spinner('Procesando datos...'):
        # Inicializar a None para asegurar un estado limpio
        st.session_state.df_site1 = None
        st.session_state.df_site2 = None
        # --- NUEVO: Inicializar df_site3 ---
        st.session_state.df_site3 = None
        # --- FIN NUEVO ---

        if url_site1:
            st.session_state.df_site1 = processor.process_url(url_site1, 1)
        if url_site2:
            st.session_state.df_site2 = processor.process_url(url_site2, 2, transmission_manual)
        # --- NUEVO: Procesar url_site3 ---
        if url_site3:
            st.session_state.df_site3 = processor.process_url(url_site3, 3)
        # --- FIN NUEVO ---

        # Actualizar el DataFrame combinado pasando los tres dataframes
        st.session_state.merged_df = processor.merge_dataframes(
            st.session_state.df_site1,
            st.session_state.df_site2,
            st.session_state.df_site3 # <-- Pasar el tercer df
        )

        # Reiniciar el estado de los cambios
        st.session_state.grid_has_changes = False
        st.session_state.previous_data = st.session_state.merged_df.to_dict('records') if st.session_state.merged_df is not None else None

    st.success('¡Procesamiento completado!')


def main():
    setup_page()
    init_session_state()

    # --- NUEVO: Recibir url_site3 ---
    url_site1, url_site2, url_site3, transmission_manual = render_url_inputs()
    # --- FIN NUEVO ---

    if st.button("Procesar URLs", type="primary"):
        # --- NUEVO: Comprobar las tres URLs ---
        if not url_site1 and not url_site2 and not url_site3:
        # --- FIN NUEVO ---
            st.warning("Por favor, ingrese al menos una URL para procesar los datos.")
        else:
            # --- NUEVO: Pasar url_site3 ---
            process_urls(url_site1, url_site2, url_site3, transmission_manual)
            # --- FIN NUEVO ---
            # Forzar un rerun para asegurar que AgGrid se renderice con los nuevos datos
            st.rerun()


    # Mostrar resultados (AgGrid)
    if st.session_state.merged_df is not None:
        st.subheader("Resultados Combinados")

        search_term = st.text_input(
            "🔍 Buscar por característica:",
            value=st.session_state.search_term,
            key="search_input",
            placeholder="Escriba para filtrar..."
        )
        st.session_state.search_term = search_term # Actualizar estado

        # Filtrar DataFrame basado en la búsqueda
        filtered_df = filter_dataframe(st.session_state.merged_df, search_term)

        # Renderizar AgGrid con el DataFrame filtrado
        grid_response = render_aggrid(filtered_df)

        # Lógica para manejar actualizaciones de AgGrid
        if grid_response and grid_response['data'] is not None:
            current_grid_data = pd.DataFrame(grid_response['data'])
            previous_grid_data_df = pd.DataFrame(st.session_state.previous_data) if st.session_state.previous_data is not None else pd.DataFrame()

            # Comprobar si los datos de la tabla han cambiado realmente
            if not current_grid_data.equals(previous_grid_data_df):
                # Actualizar el DataFrame principal en session_state con los cambios de la tabla
                # Importante: pasar filtered_df para mapear correctamente los índices si está filtrado
                st.session_state.merged_df = update_dataframe_values(
                     st.session_state.merged_df, # El DF completo
                     grid_response['data'] # Los datos actuales de la tabla (pueden estar filtrados)
                )
                # Actualizar previous_data para la próxima comparación
                st.session_state.previous_data = current_grid_data.to_dict('records')
                # Puede ser útil un rerun aquí si la actualización no se refleja inmediatamente
                # st.rerun()


        # Selección de idioma y exportación (sin cambios funcionales)
        st.subheader("Selecciona el idioma para la plantilla:")
        st.selectbox(
            "Idioma",
            options=list(st.session_state.language_options.keys()),
            index=list(st.session_state.language_options.keys()).index(st.session_state.selected_language),
            key="temp_language",
            on_change=change_language
        )

        if st.button("Transformar a ODT", type="primary"):
            if st.session_state.merged_df is not None and not st.session_state.merged_df.empty:
                with st.spinner('Preparando documento ODT...'):
                    planilla_path = st.session_state.language_options[st.session_state.selected_language]
                    exporter = ODTExporter(planilla_path)
                    # Asegurarse de pasar el DF completo y actualizado para exportar
                    doc_bytes = exporter.export_to_odt(st.session_state.merged_df)

                    if doc_bytes:
                        st.download_button(
                            label="📥 Descargar documento ODT",
                            data=doc_bytes,
                            file_name=f"datos_exportados_{st.session_state.selected_language}.odt",
                            mime="application/vnd.oasis.opendocument.text"
                        )
                        st.success('¡Documento preparado! Haz clic en el botón de descarga.')
                    else:
                        st.error('Error al generar el documento ODT.')
            else:
                st.warning("No hay datos para exportar.")
    # else: # Comentado para evitar mensaje si aún no se ha procesado nada
        # st.info("Ingrese URLs y presione 'Procesar URLs' para ver los resultados.")


if __name__ == "__main__":
    main()