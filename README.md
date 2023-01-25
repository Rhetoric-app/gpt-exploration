# gpt-exploration

## Quickstart

Make sure you've cloned this repo and installed [pipenv](https://pipenv.pypa.io/en/latest/#install-pipenv-today) globally. Generate a new [OpenAI API key](https://beta.openai.com/account/api-keys). Then run the following commands inside the cloned directory:

```sh
# Add your OpenAI API key to a .env file:
echo 'OPENAI_API_KEY=sk-your-api-key-goes-here' >> .env

# If you're using the SQL generator, you'll also need to add your psql connstring:
echo 'DB_CONN_STRING="postgresql://user:password@host:5432/db"' >> .env

# Install dependencies using pipenv:
make install

# Run the SEC app:
make streamlit-sec

# Run the SQL app:
make streamlit-sql
```
