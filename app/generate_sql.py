from dataclasses import dataclass
from typing import List, Optional

import gpt_index as gpt
import langchain
import sqlalchemy as db
from gpt_index.prompts.default_prompts import DEFAULT_TEXT_TO_SQL_PROMPT

try:
    from app.env import OPENAI_API_KEY
except ModuleNotFoundError as err:
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx

    is_streamlit = bool(get_script_run_ctx())
    if not is_streamlit:
        raise err
    OPENAI_API_KEY = st.secrets['OPENAI_API_KEY']

# Configure SqlAlchemy
engine = db.create_engine("sqlite:///:memory:")
metadata_obj = db.MetaData(bind=engine)

# Configure LLM
llm = langchain.OpenAI(temperature=0, model_name="text-davinci-003", openai_api_key=OPENAI_API_KEY)
llm_predictor = gpt.LLMPredictor(llm=llm)
prompt_helper = gpt.PromptHelper.from_llm_predictor(llm_predictor)


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

# Create tables
for table in TABLES:
    db.Table(
        table.name,
        metadata_obj,
        *[db.Column(col.name, col.type, nullable=col.nullable, primary_key=col.primary_key) for col in table.cols],
    )

# Create DB
metadata_obj.create_all()
sql_database = gpt.SQLDatabase(engine, include_tables=[table.name for table in TABLES])


def _get_all_tables_desc() -> str:
    """Get tables schema + optional context as a single string."""
    tables_desc = []
    for table_name in sql_database.get_table_names():
        table_desc = sql_database.get_single_table_info(table_name)
        table_text = f"Schema of table {table_name}:\n" f"{table_desc}\n"
        table = next((table for table in TABLES if table.name == table_name), None)
        if table:
            table_text += f"Context of table {table.name}:\n"
            table_text += table.desc
        tables_desc.append(table_text)
    result = "\n\n".join(tables_desc)
    print(result)
    return result


tables_schema = _get_all_tables_desc()


def _execute(nl_query) -> str:
    sql_str, _ = llm_predictor.predict(DEFAULT_TEXT_TO_SQL_PROMPT, query_str=nl_query, schema=tables_schema)
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
