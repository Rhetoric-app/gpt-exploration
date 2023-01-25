from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import gpt_index as gpt
import langchain
import sqlalchemy as db
from gpt_index.prompts.prompts import TextToSQLPrompt

try:
    from app.env import OPENAI_API_KEY
except ModuleNotFoundError as err:
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx

    is_streamlit = bool(get_script_run_ctx())
    if not is_streamlit:
        raise err
    OPENAI_API_KEY = st.secrets['OPENAI_API_KEY']


TEXT_TO_SQL_TMPL = '''
Given an input question, create a syntactically correct Postgres SQL query that follows these rules:
 • The generated SQL must be compatible with Postgres databases.
 • In the generated SQL, column names should always be prefixed with table names, in the format "table.column".

Use the following format:
Question: "Question here"
SQLQuery: "SQL Query to run"

The following describes the public schema in a Postgres database:
--------------------------------------
{schema}
--------------------------------------

Question: {query_str}
SQLQuery:
'''
TEXT_TO_SQL_PROMPT = TextToSQLPrompt(TEXT_TO_SQL_TMPL)


@dataclass
class TableSchemaCol:
    name: str
    type: db.types.TypeEngine
    nullable: bool = False
    primary_key: bool = False
    fk: Optional[str] = None


@dataclass
class TableSchema:
    name: str
    desc: str
    cols: List[TableSchemaCol]


# Define all tables
TABLES: List[TableSchema] = [
    TableSchema(
        name='users',
        desc=(
            'This table gives information regarding each user (AKA person, customer) in the app. '
            'In this table, the column "created_at" refers to when a user joined.'
        ),
        cols=[
            TableSchemaCol('id', db.Integer, primary_key=True),
            TableSchemaCol('full_name', db.String(256)),
            TableSchemaCol('created_at', db.DateTime),
        ],
    ),
    TableSchema(
        name='user_logins',
        desc=(
            'This table gives information regarding the datetime (AKA timestamp, AKA date) of each user login event. '
            'In this table, the column "created_at" refers to the the date and time of a given login.'
        ),
        cols=[
            TableSchemaCol('id', db.Integer, primary_key=True),
            TableSchemaCol('user_id', db.ForeignKey('users.id')),
            TableSchemaCol('created_at', db.DateTime),
        ],
    ),
    TableSchema(
        name='products',
        desc=(
            'This table gives information regarding each product (AKA item, subscription) for sale in the app. '
            'In this table, the column "amount_cents" refers to the price of an item in cents (USD).'
        ),
        cols=[
            TableSchemaCol('id', db.Integer, primary_key=True),
            TableSchemaCol('amount_cents', db.Integer),
            TableSchemaCol('created_at', db.DateTime),
        ],
    ),
    TableSchema(
        name='users_purchases',
        desc=(
            'This table gives information regarding each purchase (AKA sale) for each user and product. '
            'Total revenue may be calculated by joining the "products" table and summing "products.amount_cents".'
        ),
        cols=[
            TableSchemaCol('id', db.Integer, primary_key=True),
            TableSchemaCol('user_id', db.ForeignKey('users.id')),
            TableSchemaCol('product_id', db.ForeignKey('products.id')),
            TableSchemaCol('created_at', db.DateTime),
        ],
    ),
]


class SQLDatabase(gpt.SQLDatabase):
    def get_table_info(self, table_names: Optional[List[str]] = None) -> str:
        table_tmpl = 'CREATE TABLE {name} (\n{cols}\n);'
        all_table_names = self.get_table_names()

        if table_names is not None:
            missing_tables = set(table_names).difference(all_table_names)
            if missing_tables:
                raise ValueError(f"table_names {missing_tables} not found in database")
            all_table_names = table_names

        def _get_col_str(col: Dict[str, str], fk_map: Dict[str, Dict[str, str]]) -> str:
            col_str = f'  {col["name"]} {col["type"]}'
            if col.get('primary_key'):
                col_str += ' PRIMARY KEY'
            elif col.get('nullable'):
                col_str += ' NULL'
            if col['name'] in fk_map:
                col_str += f' REFERENCES {fk_map[col["name"]]["table"]} ({fk_map[col["name"]]["col"]})'
            return col_str

        tables = []
        for table_name in all_table_names:
            fks: List[Dict[str, str]] = db.inspect(engine).get_foreign_keys(table_name=table_name)
            fk_map = {
                f['constrained_columns'][0]: {'table': f['referred_table'], 'col': f['referred_columns'][0]}
                for f in fks
            }
            cols = []
            for col in self._inspector.get_columns(table_name, schema=self._schema):
                cols.append(_get_col_str(col, fk_map))
            cols_str = ',\n'.join(cols)
            table_str = table_tmpl.format(name=table_name, cols=cols_str)
            tables.append(table_str)

        return "\n\n".join(tables)


# Configure SqlAlchemy
engine = db.create_engine("sqlite:///:memory:")
metadata_obj = db.MetaData(bind=engine)

# Configure LLM
llm = langchain.OpenAI(temperature=0, model_name="text-davinci-003", openai_api_key=OPENAI_API_KEY)
llm_predictor = gpt.LLMPredictor(llm=llm)
prompt_helper = gpt.PromptHelper.from_llm_predictor(llm_predictor)

# Create tables
for table in TABLES:
    db.Table(
        table.name,
        metadata_obj,
        *[db.Column(col.name, col.type, nullable=col.nullable, primary_key=col.primary_key) for col in table.cols],
    )
metadata_obj.create_all()

# Create DB and generate schema
sql_database = SQLDatabase(engine, include_tables=[table.name for table in TABLES])
table_schema = sql_database.get_table_info()
print(table_schema)


def _execute(nl_query) -> str:
    sql_str, _ = llm_predictor.predict(TEXT_TO_SQL_PROMPT, query_str=nl_query, schema=table_schema)
    return sql_str


def _check_sql(sql_str) -> Optional[Exception]:
    try:
        sql_database.run_sql(sql_str)
        return None
    except Exception as err:
        return err


if __name__ == '__main__':
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx

    is_streamlit = bool(get_script_run_ctx())
    if is_streamlit:
        st.write(table_schema)
        nl_query = st.text_input('Natural language query')
        if st.button('Execute'):
            sql_str = _execute(nl_query)
            error = _check_sql(sql_str)
            if error:
                st.write(error)
            else:
                st.write(sql_str)
    else:
        while True:
            nl_query = input('\nEnter a database query in plain english, or enter "q" to exit\n> ')
            if nl_query == 'q':
                break
            sql_str = _execute(nl_query)
            error = _check_sql(sql_str)
            print('\n\n')
            if error:
                print(error)
            else:
                print(sql_str)
