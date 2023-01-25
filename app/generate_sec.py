from time import sleep
import gpt_index as gpt
import langchain

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


def _build_index():
    try:
        return gpt.GPTSimpleVectorIndex.load_from_disk(
            'sec-index.json',
            llm_predictor=llm_predictor,
            prompt_helper=prompt_helper,
        )
    except Exception:
        print('Failed to load index from disk: rebuilding from scratch...')
    index = gpt.GPTSimpleVectorIndex(
        documents=[],
        llm_predictor=llm_predictor,
        prompt_helper=prompt_helper,
    )

    initial_doc = gpt.Document(
        'The following inputs are public SEC filings for DoorDash, Lyft, and Uber:\n'
        '----------------------------------------'
    )
    index.insert(initial_doc)

    documents = gpt.SimpleDirectoryReader('app/sec_filings').load_data()
    for doc in documents:
        try:
            index.insert(doc)
        except Exception:
            sleep(1)
            try:
                index.insert(doc)
            except Exception:
                continue

    final_doc = gpt.Document(
        (
            '----------------------------------------\n'
            '(END OF SEC FILINGS)\n\n'
            'Given an input question, respond with relevant information from the SEC filings:\n'
        )
    )

    index.insert(final_doc)
    index.save_to_disk('sec-index.json')
    return index


if __name__ == '__main__':
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx

    is_streamlit = bool(get_script_run_ctx())
    index = _build_index()

    if is_streamlit:
        query = st.text_input('Ask a question about Lyft, DoorDash, or Uber')
        if st.button('Ask'):
            response = index.query(query).response
            st.write(response)
    else:
        while True:
            query = input('\n\nEnter a database query in plain english, or enter "q" to exit\n> ')
            if query == 'q':
                break
            response = index.query(query).response
            print('\n\n')
            print(response)
