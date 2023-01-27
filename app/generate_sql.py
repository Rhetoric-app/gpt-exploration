from typing import Any, Dict, List, Union

import pandas as pd
import gpt_index as gpt
import langchain
import sqlalchemy as db
from gpt_index.indices.base import BaseGPTIndex

try:
    from app.env import DB_CONN_STRING, OPENAI_API_KEY
except ModuleNotFoundError as err:
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx

    is_streamlit = bool(get_script_run_ctx())
    if not is_streamlit:
        raise err
    OPENAI_API_KEY = st.secrets['OPENAI_API_KEY']
    DB_CONN_STRING = st.secrets['DB_CONN_STRING']


# Configure SqlAlchemy
engine = db.create_engine(DB_CONN_STRING)


def _build_index() -> BaseGPTIndex:
    llm = langchain.OpenAI(temperature=0, model_name="text-davinci-003", openai_api_key=OPENAI_API_KEY)
    llm_predictor = gpt.LLMPredictor(llm=llm)
    prompt_helper = gpt.PromptHelper.from_llm_predictor(llm_predictor)

    try:
        return gpt.GPTSimpleVectorIndex.load_from_disk(
            'sql-index.json',
            llm_predictor=llm_predictor,
            prompt_helper=prompt_helper,
        )
    except Exception:
        print('Failed to load index from disk: rebuilding...')

    initial_doc = gpt.Document(
        'The following inputs describes the public schema in a Postgres database:\n'
        '----------------------------------------'
    )
    index = gpt.GPTSimpleVectorIndex(documents=[initial_doc], llm_predictor=llm_predictor, prompt_helper=prompt_helper)
    table_tmpl = 'CREATE TABLE {name} (\n{cols}\n);\n'

    # Build a string that describes a column
    def _get_col_str(col: Dict[str, str], fk_map: Dict[str, Dict[str, str]]) -> str:
        col_str = f'  {col["name"]} {col["type"]}'
        if col.get('primary_key'):
            col_str += ' PRIMARY KEY'
        elif col.get('nullable'):
            col_str += ' NULL'
        if col['name'] in fk_map:
            col_str += f' REFERENCES {fk_map[col["name"]]["table"]} ({fk_map[col["name"]]["col"]})'
        return col_str

    # Index each table in the public schema
    table_names = db.inspect(engine).get_table_names(schema='public')
    for table_name in table_names:
        fk_map = {}
        for fk in db.inspect(engine).get_foreign_keys(table_name=table_name, schema='public'):
            fk_map[fk['constrained_columns'][0]] = {'table': fk['referred_table'], 'col': fk['referred_columns'][0]}

        cols = []
        for col in db.inspect(engine).get_columns(table_name, schema='public'):
            cols.append(_get_col_str(col, fk_map))

        cols_str = ',\n'.join(cols)
        table_str = table_tmpl.format(name=table_name, cols=cols_str)
        doc = gpt.Document(table_str)
        index.insert(doc)

    final_doc = gpt.Document(
        (
            '----------------------------------------\n'
            '(END OF SCHEMA)\n'
            'Given an input question, create a syntactically correct Postgres SQL query that follows these rules:\n'
            ' • The generated SQL must be compatible with Postgres databases\n'
            ' • The generated SQL may ONLY reference the following table names: {table_names}\n'
            ' • The generated SQL, column names must be prefixed with table names, in the format "table.column"\n'
            '\n'
            'Use the following format:\n'
            'Question: "Question here"\n'
            'SQLQuery: "SQL Query to run"'
        ).format(table_names=', '.join(f'"{table_name}"' for table_name in table_names))
    )
    index.insert(final_doc)
    try:
        index.save_to_disk('sql-index.json')
    except Exception:
        print('Failed to save index to disk')
    return index


def _execute_nl(index: BaseGPTIndex, nl_query: str) -> str:
    prompt = 'Question: {nl_query}\nSQLQuery: '.format(nl_query=nl_query)
    response = index.query(prompt)
    return response.response or 'ERROR: NO OUTPUT'


def _execute_sql(sql_str) -> Union[Exception, pd.DataFrame]:
    try:
        with engine.connect() as conn:
            return pd.read_sql(sql_str, conn)
    except Exception as err:
        return err


if __name__ == '__main__':
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx

    is_streamlit = bool(get_script_run_ctx())

    index = _build_index()

    if is_streamlit:
        nl_query = st.text_input('Natural language query')
        if st.button('Execute'):
            sql_str = _execute_nl(index, nl_query)
            response = _execute_sql(sql_str)
            if isinstance(response, Exception):
                st.write(response)
            else:
                st.write(sql_str)
                st.write(response)
    else:
        while True:
            nl_query = input('\nEnter a database query in plain english, or enter "q" to exit\n> ')
            if nl_query == 'q':
                break
            sql_str = _execute_nl(index, nl_query)
            response = _execute_sql(sql_str)
            print('\n\n')
            if isinstance(response, Exception):
                print(response)
            else:
                print(sql_str)
                print(response)
