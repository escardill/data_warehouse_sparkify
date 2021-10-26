import os
from dotenv import load_dotenv

load_dotenv()  # Read the environment variables from .env file.

# AWS
KEY = os.environ.get('KEY')
SECRET = os.environ['SECRET']
