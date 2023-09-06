import os
import requests
from zipfile import ZipFile
from tqdm import tqdm

def descargar_y_descomprimir(url, ruta_src, ruta_archivo_zip):
    """
    Descarga un archivo desde una URL, lo guarda en una ruta local, lo descomprime
    y elimina el archivo ZIP descargado.

    Args:
        url (str): La URL desde la cual se descargará el archivo.
        ruta_src (str): La ruta donde se guardarán los archivos descomprimidos.
        ruta_archivo_zip (str): La ruta donde se guardará el archivo ZIP descargado.

    Returns:
        str: "Success" si la operación fue exitosa, "Error" si ocurrió un error.
    """
    try:
        session = requests.Session()
        # Incrementar el tamaño del chunk para una descarga mucho más rápida
        chunk_size = 32768    # 32 KB
        os.makedirs(ruta_src, exist_ok=True)
        
        # Verificar si el archivo ZIP ya existe en la ruta
        if not os.path.exists(ruta_archivo_zip):
            # Realizar una solicitud HEAD para obtener el tamaño total del archivo
            response = session.head(url)
            total_size = int(response.headers.get("content-length", 0))
            
            # Mostrar una barra de progreso mientras se descarga el archivo
            with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
                with open(ruta_archivo_zip, "wb") as zip_file:
                    response = session.get(url, stream=True)
                    for data in response.iter_content(chunk_size=chunk_size):
                        zip_file.write(data)
                        pbar.update(len(data))
        
        # Descomprimir el archivo ZIP
        with ZipFile(ruta_archivo_zip, "r") as zip_ref:
            zip_ref.extractall(ruta_src)
        
        # Eliminar el archivo ZIP descargado
        os.remove(ruta_archivo_zip)
        
        return "Success"
    except (requests.exceptions.RequestException, IOError) as e:
        print(f"Error occurred: {e}")
        return "Error"

