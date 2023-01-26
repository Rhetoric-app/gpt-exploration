import os
import re
import subprocess
from enum import Enum
from pathlib import Path
from time import sleep
from typing import Iterable, Literal, Optional

import gpt_index as gpt
import langchain
from gpt_index.indices.base import BaseGPTIndex
from markdownify import MarkdownConverter as _MarkdownConverter

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

DocType = Literal['txt', 'html', 'rtf', 'rtfd', 'doc', 'docx', 'wordml', 'odt', 'webarchive']


class DocTypeEnum(Enum):
    txt: DocType = 'txt'
    html: DocType = 'html'
    rtf: DocType = 'rtf'
    rtfd: DocType = 'rtfd'
    doc: DocType = 'doc'
    docx: DocType = 'docx'
    wordml: DocType = 'wordml'
    odt: DocType = 'odt'
    webarchive: DocType = 'webarchive'


class MarkdownConverter(_MarkdownConverter):
    def process_tag(self, node, *args, **kwargs) -> str:
        if node and node.name in ['head', 'style']:
            return ''
        return super().process_tag(node, *args, **kwargs)  # type: ignore

    def process_text(self, el):
        text = super().process_text(el)
        return text.strip()


def convert_rft(input_path: str, to_type: DocTypeEnum, output_path: Optional[str] = None) -> None:
    """
    Convert a rich text file to another supported format.
    """
    args = ['textutil', '-convert', to_type.value, input_path]
    if output_path:
        args.extend(['-output', output_path])
    subprocess.run(args, check=True)


def _remove_extra_newlines(s: str) -> str:
    """
    Remove any series of newlines greater than two.
    """
    return re.sub(r'\n+\s*\n', r'\n\n', s)


def _update_ext(path: str, new_ext: str) -> str:
    """
    Non-destructively return a new file path with an updated file extension.
    """
    return os.path.splitext(path)[0] + new_ext


def html_to_md(input_path: str, output_path: Optional[str] = None) -> None:
    """
    Convert an HTML file to markdown.
    """
    output_path = output_path or _update_ext(input_path, '.md')
    with open(input_path, 'r') as fileobj:
        html_str = fileobj.read()
    md_str = MarkdownConverter(strip=['a']).convert(html_str)
    md_str = _remove_extra_newlines(md_str)
    with open(output_path, 'w') as fileobj:
        fileobj.write(md_str)


def rtf_to_md(input_path: str, output_path: Optional[str] = None) -> None:
    """
    Convert a rich text file to markdown, preserving formatting.
    """
    output_path = output_path or _update_ext(input_path, '.html')
    convert_rft(input_path, DocTypeEnum.html, output_path)
    input_path = output_path
    output_path = _update_ext(input_path, '.md')
    html_to_md(input_path, output_path)
    os.remove(input_path)


def iter_paragraphs(s: str, delim='\n\n') -> Iterable[str]:
    """
    Yield each paragraph in a larger block of text.
    """
    paragraphs = s.split(delim)
    for p in paragraphs:
        yield p


def chunk_paragraphs(s: str, max_chars=4000) -> Iterable[str]:
    """
    Iterate chunks of paragraphs, up to `max_chars` long.
    """
    chunk = ''
    for p in iter_paragraphs(s):
        if len(chunk) + len(p) > max_chars:
            yield chunk
            chunk = ''
        chunk += p
    if chunk:
        yield chunk


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
        print('Failed to load index from disk: rebuilding from scratch...')

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
                print(f'Failed to index chunk on first attempt: {e}')
                sleep(5)
                try:
                    index.insert(doc)
                except Exception as e:
                    print(f'Failed to index chunk on final attempt, skipping: {e}')
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
