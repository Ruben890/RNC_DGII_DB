import logging
import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

class Conexion:
    def __init__(self):
        try:
            self.conexion = mysql.connector.connect(
                host=os.getenv("HOST_DB"),
                user=os.getenv("USER_DB"),
                password=os.getenv("PASSWORD_DB"),
                port=os.getenv("PORT_DB"),
                db=os.getenv("NAME_DB"),
                connect_timeout=int(os.getenv("CONNECT_TIMEOUT", "60"))
            )
            self.cursor = self.conexion.cursor()
            self.test_connection()
        except mysql.connector.Error as e:
            logging.error(f"Ha ocurrido un error de conexión: {e}")

    def test_connection(self):
        try:
            self.cursor.execute("SELECT 1")
            result = self.cursor.fetchone()
            
            if result[0] == 1:
                logging.info("Conexión exitosa a la base de datos.")
            else:
                logging.error("Error en la conexión a la base de datos.")
        except mysql.connector.Error as e:
            logging.error(f"Ha ocurrido un error de conexión: {e}")

    def __del__(self):
        self.cursor.close()
        self.conexion.close()


