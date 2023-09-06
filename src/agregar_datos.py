import csv
import logging
from datetime import datetime
from tqdm import tqdm
from database_connection import Conexion

class RNCRecord:
    def __init__(self, rnc, nombre_apellido, actividad_economica, fecha, estado, tipo_contribuyente):
        self.rnc = rnc.strip()
        self.nombre_apellido = " ".join(nombre_apellido.split())  # Eliminar espacios adicionales
        self.actividad_economica = actividad_economica.strip()
        self.fecha = fecha
        self.estado = estado.strip()
        self.tipo_contribuyente = tipo_contribuyente.strip()

   
class DataBaseManager:
    def __init__(self):
        self.conexion = Conexion()
        self.conexion.test_connection()

    def existe_registro(self, numero_registro):
        """
        Verifica si un registro ya existe en la base de datos.

        Args:
            numero_registro (str): El número de registro a verificar.

        Returns:
            bool: True si el registro existe, False si no.
        """
        try:
            sql = 'SELECT EXISTS (SELECT 1 FROM RNC WHERE rnc = %s)'
            self.conexion.cursor.execute(sql, (numero_registro,))
            return self.conexion.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f'Error al verificar si el registro existe: {e}')
    
    def agregar_registros(self, registros):
        try:
            sql = 'INSERT INTO RNC (rnc, nombre_apellido, actividad_economica, fecha, estado, tipo_contribuyente) VALUES (%s, %s, %s, %s, %s, %s)'
            values = [(registro.rnc, registro.nombre_apellido, registro.actividad_economica, registro.fecha, registro.estado, registro.tipo_contribuyente) for registro in registros]
            self.conexion.cursor.executemany(sql, values)
            self.conexion.conexion.commit()
            registros_agregados = self.conexion.cursor.rowcount
            return registros_agregados
        except Exception as e:
            self.conexion.conexion.rollback()
            raise Exception(f'Ha ocurrido un error al agregar los registros: {e}')

def agregar_datos_desde_csv(file_path, batch_size=1000):
    """
    Agrega los datos desde un archivo CSV a la base de datos en lotes.

    Args:
        file_path (str): La ruta del archivo CSV.
        batch_size (int): El tamaño del lote para la inserción en la base de datos.

    Returns:
        int: El número total de registros agregados a la base de datos.
    """
    total_registros = 0

    try:
        with open(file_path, newline='', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Omite la primera línea si contiene encabezados

            registros_lote = []
            pbar = tqdm(total=0)  # Inicializa la barra de progreso

            db_manager = DataBaseManager()  # Crear una única instancia de DataBaseManager

            for row in csv_reader:
                if len(row) == 11:
                    rnc = row[0]
                    if not db_manager.existe_registro(rnc):
                        fecha = row[8].strip()  # Elimina espacios adicionales en la fecha
                        if fecha and fecha != '00/00/0000':
                            try:
                                fecha = datetime.strptime(fecha, "%d/%m/%Y").strftime("%Y-%m-%d")
                            except ValueError:
                                fecha = None  # Asignar None si la fecha no es válida
                        else:
                            fecha = None  # Asignar None si la fecha está en blanco
                        registros_lote.append(RNCRecord(rnc, row[1], row[3], fecha, row[9], row[10]))

                    if len(registros_lote) >= batch_size:
                        try:
                            db_manager.agregar_registros(registros_lote)
                            total_registros += len(registros_lote)
                            registros_lote = []  # Reiniciar el lote
                        except Exception as e:
                            logging.error('Error al agregar registros a la DB', exc_info=True)

                        # Actualiza la barra de progreso con el número total de registros procesados
                        pbar.update(batch_size)

            # Inserta cualquier lote restante
            if registros_lote:
                try:
                    db_manager.agregar_registros(registros_lote)
                    total_registros += len(registros_lote)
                except Exception as e:
                    logging.error('Error al agregar registros a la DB', exc_info=True)

            pbar.close()  # Cierra la barra de progreso al final

        print(f'Total de registros agregados: {total_registros}')
    except FileNotFoundError:
        print('El archivo CSV no existe.')

