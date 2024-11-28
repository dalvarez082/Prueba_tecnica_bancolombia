import os
import pandas as pd
import sqlite3
import unidecode  


folder_path = "C:/Users/Diego/OneDrive/Escritorio/Prueba_tecnica_banbolombia/descargas_excel"


def normalize_columns(columns):
    return [unidecode.unidecode(col).strip().lower().replace(" ", "_") for col in columns]


def clean_month_name(month):
    return unidecode.unidecode(month.replace("$", "").strip().lower())


capitacion_columns = None
evento_columns = None


for file_name in os.listdir(folder_path):
    if file_name.startswith("~$") or not file_name.endswith(".xlsx"):  
        continue

    file_path = os.path.join(folder_path, file_name)
    print(f"Inspeccionando archivo: {file_name}")
    excel_file = pd.ExcelFile(file_path)

  
    if "GIRO DIRECTO CAPITACION" in excel_file.sheet_names:
        capitacion = pd.read_excel(file_path, sheet_name="GIRO DIRECTO CAPITACION", header=7)  
        normalized_columns = normalize_columns(capitacion.columns)
        if capitacion_columns is None:
            capitacion_columns = set(normalized_columns)
        else:
            capitacion_columns &= set(normalized_columns)

   
    if "GIRO DIRECTO EVENTO" in excel_file.sheet_names:
        evento = pd.read_excel(file_path, sheet_name="GIRO DIRECTO EVENTO", header=7)
        normalized_columns = normalize_columns(evento.columns)
        if evento_columns is None:
            evento_columns = set(normalized_columns)
        else:
            evento_columns &= set(normalized_columns)


capitacion_columns = list(capitacion_columns)
evento_columns = list(evento_columns)
print(f"Columnas comunes detectadas para 'GIRO DIRECTO CAPITACION': {capitacion_columns}")
print(f"Columnas comunes detectadas para 'GIRO DIRECTO EVENTO': {evento_columns}")


capitacion_data = pd.DataFrame()
evento_data = pd.DataFrame()


for file_name in os.listdir(folder_path):
    if file_name.startswith("~$") or not file_name.endswith(".xlsx"):
        continue

    file_path = os.path.join(folder_path, file_name)
    print(f"Procesando archivo: {file_name}")
    excel_file = pd.ExcelFile(file_path)

   
    if "GIRO DIRECTO CAPITACION" in excel_file.sheet_names:
        capitacion = pd.read_excel(file_path, sheet_name="GIRO DIRECTO CAPITACION", header=7)
        capitacion.columns = normalize_columns(capitacion.columns)

        # Detectar columna dinámica "total_giro" y extraer el periodo
        total_giro_col = [col for col in capitacion.columns if "total_giro" in col]
        if total_giro_col:
            periodo = total_giro_col[0].replace("total_giro_", "").replace("_", " ").strip()
            periodo = clean_month_name(periodo)  
            if " " in periodo:
                capitacion["mes"], capitacion["año"] = periodo.split(" ", 1)
            else:
                capitacion["mes"], capitacion["año"] = periodo, None
            capitacion.rename(columns={total_giro_col[0]: "total_giro"}, inplace=True)

        capitacion = capitacion[capitacion_columns + ["total_giro", "mes", "año"]]
        capitacion_data = pd.concat([capitacion_data, capitacion], ignore_index=True)

    # Procesar hoja "GIRO DIRECTO EVENTO"
    if "GIRO DIRECTO EVENTO" in excel_file.sheet_names:
        evento = pd.read_excel(file_path, sheet_name="GIRO DIRECTO EVENTO", header=7)
        evento.columns = normalize_columns(evento.columns)

       
        total_giro_col = [col for col in evento.columns if "total_giro" in col]
        if total_giro_col:
            periodo = total_giro_col[0].replace("total_giro_", "").replace("_", " ").strip()
            periodo = clean_month_name(periodo)  
            if " " in periodo:
                evento["mes"], evento["año"] = periodo.split(" ", 1)
            else:
                evento["mes"], evento["año"] = periodo, None
            evento.rename(columns={total_giro_col[0]: "total_giro"}, inplace=True)

        evento = evento[evento_columns + ["total_giro", "mes", "año"]]
        evento_data = pd.concat([evento_data, evento], ignore_index=True)


capitacion_data = capitacion_data.loc[:, ~capitacion_data.columns.duplicated()]
evento_data = evento_data.loc[:, ~evento_data.columns.duplicated()]


capitacion_data.columns = capitacion_data.columns.str.replace(" ", "_").str.replace("[^a-zA-Z0-9_]", "", regex=True)
evento_data.columns = evento_data.columns.str.replace(" ", "_").str.replace("[^a-zA-Z0-9_]", "", regex=True)


db_path = "giros_salud.sqlite"
connection = sqlite3.connect(db_path)


cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS giro_directo_capitacion;")
cursor.execute("DROP TABLE IF EXISTS giro_directo_evento;")
connection.commit()


capitacion_data.to_sql("giro_directo_capitacion", connection, if_exists="replace", index=False)
evento_data.to_sql("giro_directo_evento", connection, if_exists="replace", index=False)

print("Datos cargados exitosamente en las tablas SQLite.")
connection.close()
