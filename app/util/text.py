import os
import re
from enum import Enum
import subprocess
from markdownify import MarkdownConverter as _MarkdownConverter
from typing import Iterable, Literal, Optional

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
