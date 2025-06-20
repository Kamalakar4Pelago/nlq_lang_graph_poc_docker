from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    REDSHIFT_CONFIG = {
        "host": os.getenv("REDSHIFT_HOST"),
        "port": int(os.getenv("REDSHIFT_PORT", 5439)),
        "user": os.getenv("REDSHIFT_USER"),
        "password": os.getenv("REDSHIFT_PASSWORD"),
        "dbname": os.getenv("REDSHIFT_DBNAME")
    }