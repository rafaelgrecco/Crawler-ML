from pymongo import MongoClient
from loguru import logger
from dotenv import load_dotenv
import os

load_dotenv()
password = os.getenv("PASSWORD")
class Client():
    def __init__(self) -> None:
        self.cluster = f"mongodb+srv://rafael:{password}@mlentryes.pyvl2kb.mongodb.net/?retryWrites=true&w=majority"
    
    def upload_data(self, payload=None):
        client = MongoClient(self.cluster)
        try:
            db = client["MercadoLivre"]
            data = db.casas
            data.insert_one(payload)
            logger.success("Upload concluído")
        except Exception as e:
            logger.error(e)
