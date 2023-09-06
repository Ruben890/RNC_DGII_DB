import os
from descargar_descomprimir import descargar_y_descomprimir
from agregar_datos import agregar_datos_desde_csv
from convertir_TXT_CVS import convertir_txt_a_csv
    


def main():
    file_path = 'TMP/DGII_RNC.CSV'
    archivo_texto = 'TMP/DGII_RNC.TXT'
    url = "https://dgii.gov.do/app/WebApps/Consultas/RNC/DGII_RNC.zip"
    # Obtener el directorio actual
    directorio_actual = os.getcwd()
    # Definir la ruta al subdirectorio "src" dentro del directorio actual
    ruta_src = os.path.join(directorio_actual)
    # Concatenar el directorio "src" con el nombre del archivo ZIP
    ruta_archivo_zip = os.path.join(ruta_src, "DGII_RNC.zip")

    #Descargar y descomprimir el archivo ZIP
    descargar_y_descomprimir(url, ruta_src, ruta_archivo_zip)
    # Llama a la función con el nombre del archivo de texto
    convertir_txt_a_csv(archivo_texto)
    # Agregar datos desde el archivo de  a lacvs base de datos
    agregar_datos_desde_csv(file_path)

if __name__ == "__main__":
    main()
