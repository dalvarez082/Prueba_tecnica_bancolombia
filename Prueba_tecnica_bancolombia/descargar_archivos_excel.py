import os
import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.adres.gov.co/eps/giro-directo/Paginas/girosDiscriminados/giro-por-tipo-de-contratacion.aspx"


DOWNLOAD_FOLDER = "descargas_excel"


os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

def descargar_archivos():
   
    response = requests.get(BASE_URL)
    if response.status_code != 200:
        print(f"No se pudo acceder a la página. Código de estado: {response.status_code}")
        return

    
    soup = BeautifulSoup(response.content, "html.parser")
    
   
    enlaces = soup.find_all("a", href=True)
    archivos_excel = [link["href"] for link in enlaces if link["href"].endswith(".xlsx") or link["href"].endswith(".xls")]

    if not archivos_excel:
        print("No se encontraron archivos Excel en la página.")
        return

   
    for i, archivo in enumerate(archivos_excel, start=1):
        archivo_url = archivo if archivo.startswith("http") else f"https://www.adres.gov.co{archivo}"
        nombre_archivo = os.path.join(DOWNLOAD_FOLDER, archivo.split("/")[-1])
        
        print(f"Descargando archivo {i}: {archivo_url}")
        try:
            archivo_response = requests.get(archivo_url, stream=True)
            if archivo_response.status_code == 200:
                with open(nombre_archivo, "wb") as f:
                    for chunk in archivo_response.iter_content(chunk_size=1024):
                        f.write(chunk)
                print(f"Archivo guardado: {nombre_archivo}")
            else:
                print(f"Error al descargar el archivo {archivo_url}. Código: {archivo_response.status_code}")
        except Exception as e:
            print(f"Error descargando el archivo {archivo_url}: {e}")

if __name__ == "__main__":
    descargar_archivos()
