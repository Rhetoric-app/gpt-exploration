import os

from dotenv import dotenv_values

env = os.environ | dotenv_values('.env')

OPENAI_API_KEY: str = env.get('OPENAI_API_KEY') or ''
