import os
from time import sleep
import gpt_index as gpt
import langchain
from app.util import text
from gpt_index.indices.base import BaseGPTIndex
from pathlib import Path

try:
    from app.env import OPENAI_API_KEY
except ModuleNotFoundError as err:
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx

    is_streamlit = bool(get_script_run_ctx())
    if not is_streamlit:
        raise err
    OPENAI_API_KEY = st.secrets['OPENAI_API_KEY']

llm = langchain.OpenAI(temperature=0, model_name="text-davinci-003", openai_api_key=OPENAI_API_KEY)
llm_predictor = gpt.LLMPredictor(llm=llm)
prompt_helper = gpt.PromptHelper.from_llm_predictor(llm_predictor)


def _path_to_filing(prefix: str) -> str:
    return f'{Path.cwd()}/app/sec_filings/{prefix}_SECFiling_10Q_Q3.md'


def _build_index():
    try:
        return gpt.GPTSimpleVectorIndex.load_from_disk(
            'sec-index.json',
            llm_predictor=llm_predictor,
            prompt_helper=prompt_helper,
        )
    except Exception:
        print(f'Failed to load index from disk: rebuilding from scratch...')

    index = gpt.GPTSimpleVectorIndex(
        documents=[],
        llm_predictor=llm_predictor,
        prompt_helper=prompt_helper,
    )

    for prefix in ['Lyft', 'Uber', 'Doordash']:
        initial_doc = gpt.Document(
            'The following series of inputs represent a public SEC filing for {prefix} in Markdown format:\n'
            '----------------------------------------'
        )
        index.insert(initial_doc)

        with open(_path_to_filing(prefix), 'r') as fileobj:
            content = fileobj.read()

        for chunk in text.chunk_paragraphs(content):
            try:
                doc = gpt.Document(chunk)
                index.insert(doc)
                sleep(1)
            except Exception as e:
                print('Failed to index chunk on first attempt: {e}')
                sleep(5)
                try:
                    index.insert(doc)
                except Exception as e:
                    print('Failed to index chunk on final attempt, skipping: {e}')
                    sleep(5)
                    continue

        index.insert(gpt.Document(('---------------------------------------- END OF SEC FILING FOR {prefix})')))

    index.save_to_disk('sec-index.json')
    return index


PROMPT_TEMPLATE = 'Use the following format to answer questions:\nQuestion: "Question here"\nResponse: "Response"'


def _execute(index: BaseGPTIndex, query: str) -> str:
    prompt = 'Question: According to the SEC filings, {query}\nResponse: '.format(query=query)
    response = index.query(prompt, mode="default")
    return response.response or 'ERROR: NO OUTPUT'


if __name__ == '__main__':
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx

    is_streamlit = bool(get_script_run_ctx())
    index = _build_index()

    if is_streamlit:
        query = st.text_input('Ask a question about Lyft, DoorDash, or Uber')
        if st.button('Ask'):
            response = _execute(index, query)
            st.write(response)
    else:
        while True:
            query = input('\n\nEnter a database query in plain english, or enter "q" to exit\n> ')
            if query == 'q':
                break
            response = _execute(index, query)
            print('\n\n')
            print(response)
