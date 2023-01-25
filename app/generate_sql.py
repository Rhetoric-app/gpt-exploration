"""
https://github.com/jerryjliu/gpt_index/blob/main/examples/struct_indices/SQLIndexDemo.ipynb
https://github.com/jerryjliu/gpt_index/blob/main/examples/struct_indices/SQLIndexDemo-Context.ipynb
https://github.com/jerryjliu/gpt_index/issues/204
"""
import gpt_index as gpt
from gpt_index.indices.query.query_runner import QueryRunner
from gpt_index.indices.query.struct_store.sql import GPTNLStructStoreIndexQuery
import sqlalchemy as db
from gpt_index.indices.query.query_map import get_query_cls
import langchain
from app.env import OPENAI_API_KEY

engine = db.create_engine("sqlite:///:memory:")
metadata_obj = db.MetaData(bind=engine)

llm = langchain.OpenAI(temperature=0, model_name="text-davinci-002", openai_api_key=OPENAI_API_KEY)
llm_predictor = gpt.LLMPredictor(llm=llm)

table_name = "city_stats"
city_stats_table = db.Table(
    table_name,
    metadata_obj,
    db.Column("city_name", db.String(16), primary_key=True),
    db.Column("population", db.Integer),
    db.Column("country", db.String(16), nullable=False),
)
metadata_obj.create_all()

sql_database = gpt.SQLDatabase(engine, include_tables=["city_stats"])
sql_database.table_info

city_stats_text = (
    "This table gives information regarding the population and country of a given city.\n"
    "The user will query with codewords, where 'foo' corresponds to population and 'bar'"
    "corresponds to city."
)

# manual
table_context_dict = {"city_stats": city_stats_text}
index = gpt.GPTSQLStructStoreIndex(
    documents=[],
    sql_database=sql_database,
    table_name="city_stats",
    table_context_dict=table_context_dict,
    llm_predictor=llm_predictor,
)

# BaseGptIndexQuery.query
query_config = gpt.QueryConfig(
    index_struct_type=gpt.IndexStructType.from_index_struct(index.index_struct),
    query_mode=gpt.QueryMode.DEFAULT,
)
query_runner = QueryRunner(
    index._llm_predictor,
    index._prompt_helper,
    index._embed_model,
    index._docstore,
    query_configs=[query_config],
    verbose=True,
    recursive=False,
)

# # QueryRunner.query
index_struct_type = gpt.IndexStructType.from_index_struct(index.index_struct)
if index_struct_type not in query_runner._config_dict:
    raise ValueError(f"IndexStructType {index_struct_type} not in config_dict")
config = query_runner._config_dict[index_struct_type]
mode = config.query_mode
query_cls = get_query_cls(index_struct_type, mode)
query_kwargs = query_runner._get_query_kwargs(config)
query_obj: GPTNLStructStoreIndexQuery = query_cls(
    index.index_struct,
    query_runner=query_runner,
    docstore=query_runner._docstore,
    sql_database=sql_database,
    **query_kwargs,
)

# GPTNLStructStoreIndexQuery._query
table_desc_str = query_obj._get_all_tables_desc()


if __name__ == '__main__':
    while True:
        query_str = input('\nEnter a database query in plain english, or enter "q" to exit\n')
        if query_str == 'q':
            break
        response_str, _ = query_obj._llm_predictor.predict(
            query_obj._text_to_sql_prompt,
            query_str=query_str,
            schema=table_desc_str,
        )
        sql_query_str = query_obj._parse_response_to_sql(response_str)
        print('\n')
        print(sql_query_str)
