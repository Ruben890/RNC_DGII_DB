import csv
import os

def convertir_txt_a_csv(archivo_texto):
    """
    Convierte un archivo de texto en formato CSV y elimina el archivo de texto original.

    Args:
        archivo_texto (str): El nombre del archivo de texto que se va a convertir.

    Returns:
        None
    """

    # Nombre del archivo CSV (obtenido reemplazando la extensión del archivo de texto)
    archivo_csv = archivo_texto.replace('.TXT', '.CSV')

    # Tamaño del buffer
    buffer_size = 4096

    # Abrir el archivo de texto en modo lectura con un buffer
    with open(archivo_texto, 'r', encoding='latin-1', buffering=buffer_size) as entrada:
        # Abrir el archivo CSV de salida en modo escritura
        with open(archivo_csv, 'w', newline='', encoding='utf-8') as salida:
            csv_writer = csv.writer(salida, delimiter=',')
            
            # Leer y procesar cada línea del archivo de texto
            while True:
                chunk = entrada.read(buffer_size)
                if not chunk:
                    break
                lines = chunk.split('\n')
                for linea in lines:
                    campos = linea.strip().split('|')
                    csv_writer.writerow(campos)

    print(f"Se ha creado automáticamente el archivo '{archivo_csv}' con los datos del archivo '{archivo_texto}'.")

    # Eliminar el archivo de texto
    os.remove(archivo_texto)


