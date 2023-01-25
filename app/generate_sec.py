from time import sleep
import gpt_index as gpt


def _build_index():
    try:
        return gpt.GPTSimpleVectorIndex.load_from_disk('sec-index.json')
    except Exception:
        print('Failed to load index from disk: rebuilding from scratch...')
    documents = gpt.SimpleDirectoryReader('app/sec_filings').load_data()
    index = gpt.GPTSimpleVectorIndex(documents=[])
    for doc in documents:
        try:
            index.insert(doc)
        except Exception:
            sleep(1)
            try:
                index.insert(doc)
            except Exception:
                continue
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
