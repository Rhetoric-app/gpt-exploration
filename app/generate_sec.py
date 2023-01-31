"""
Based on: https://medium.datadriveninvestor.com/access-companies-sec-filings-using-python-760e6075d3ad
"""

from time import sleep
from datetime import datetime
import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd
import openai
import sqlalchemy as db
import requests

try:
    from app.env import OPENAI_API_KEY
except ModuleNotFoundError as err:
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx

    is_streamlit = bool(get_script_run_ctx())
    if not is_streamlit:
        raise err
    OPENAI_API_KEY = st.secrets['OPENAI_API_KEY']
finally:
    openai.api_key = OPENAI_API_KEY


@dataclass
class Company:
    cik: str
    title: str
    ticker: str


@dataclass
class Asset:
    timestamp: datetime
    company: str
    total_assets_usd: int
    fiscal_year: int
    fiscal_quarter: str
    form_name: str


_SQL_ENGINE: Optional[db.engine.Engine] = None
_TICKER_TO_COMPANY_MAP: Optional[Dict[str, 'Company']] = None
_HTTP_HEADERS: Dict[str, str] = {'User-Agent': 'scripting@rhetoric.app'}


def _get_sql_engine() -> db.engine.Engine:
    """
    Return a cached SqlAlchemy `Engine`.
    """
    global _SQL_ENGINE
    if _SQL_ENGINE:
        return _SQL_ENGINE
    _SQL_ENGINE = db.create_engine("sqlite:///:memory:")
    return _SQL_ENGINE


def _get_ticker_to_company_map() -> Dict[str, 'Company']:
    """
    Cache and normalize SEC-provided data as a mapping of ticker symbols to `Company` objects.
    """
    global _TICKER_TO_COMPANY_MAP
    if _TICKER_TO_COMPANY_MAP:
        return _TICKER_TO_COMPANY_MAP
    _TICKER_TO_COMPANY_MAP = {}

    data = _request('https://www.sec.gov/files/company_tickers.json')
    for index in data:
        ticker = data[index]['ticker']
        company = Company(
            ticker=ticker,
            cik=str(data[index]['cik_str']).zfill(10),
            title=data[index]['title'],
        )
        _TICKER_TO_COMPANY_MAP[ticker] = company

    return _TICKER_TO_COMPANY_MAP


def _get_tag_for_ticker(ticker: str, *, tag: str) -> Dict[str, Any]:
    """
    A complete list of tags:
    https://xbrlview.fasb.org/yeti/resources/yeti-gwt/Yeti.jsp#tax~(id~174*v~8788)!net~(a~3474*l~832)!lang~(code~en-us)!rg~(rg~32*p~12)
    """
    cik = _get_ticker_to_company_map()[ticker].cik
    url = f'https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/{tag}.json'
    return _request(url)


def _get_assets_for_ticker(ticker: str) -> List['Asset']:
    """
    Get all filings for the "Assets" tag for the given company as `Asset` objects.
    """
    assets: List['Asset'] = []
    data = _get_tag_for_ticker(ticker, tag='Assets')
    entries = data['units']['USD']
    for entry in entries:
        asset = Asset(
            timestamp=datetime.strptime(entry['end'], '%Y-%m-%d'),
            company=ticker,
            total_assets_usd=entry['val'],
            fiscal_year=entry['fy'],
            fiscal_quarter=entry['fp'],
            form_name=entry['form'],
        )
        assets.append(asset)
    return assets


def _nl_to_sql(nl_str) -> str:
    """
    Use GPT to translate a natural-language question into SQL.
    """
    response = openai.Completion.create(
        model="code-davinci-002",
        prompt=(
            'Given a Postgres table named "assets_stream" with the following structure:\n'
            '_________________\n'
            'timestamp (DATETIME)\n'
            'company (TEXT)\n'
            'total_assets_usd (FLOAT)\n'
            'fiscal_year (INT)\n'
            'fiscal_quarter (TEXT)\n'
            'form_id (TEXT) \n'
            '_________________\n'
            '\n'
            'Respond according to the following rules:\n'
            ' - The response must be a syntactically correct Postgres SQL query.\n'
            ' - The response may only SELECT from the "assets_stream" table.\n'
            '\n'
            'The value of the "company" column must be a known SEC stock ticker symbol.\n'
            'For example \'LYFT\', \'AAPL\', \'UBER\', \'DASH\'.\n'
            '\n'
            'Respond in the following format:\n'
            '_________________\n'
            'QUESTION: A natural language question in english?\n'
            'POSTGRESQL: Postgres SQL Query to run\n'
            '_________________\n'
            '\n'
            'QUESTION: {}\n'
            'POSTGRESQL:'
        ).format(nl_str),
        temperature=0,
        max_tokens=256,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=[";", "\n\n"],
    )
    return response.choices[0].text.strip()  # type: ignore [no-any-return]


def _extract_companies_from_sql(sql_str) -> List['Company']:
    """
    For a given SQL query, extract any referenced Companies by their ticker symbol.
    """
    ticker_map = _get_ticker_to_company_map()
    maybe_tickers: List[str] = re.findall('\'([A-Z]+)\'', sql_str)
    return [ticker_map[t] for t in maybe_tickers if t in ticker_map]


def _prep_db_for_companies(companies: List['Company']) -> None:
    if not companies:
        raise Exception(
            'No companies matched your query.\n'
            'Please be sure to reference one or more specific companies by name or ticker symbol.'
        )
    engine = _get_sql_engine()
    for company in companies:
        assets = _get_assets_for_ticker(company.ticker)
        df = pd.DataFrame(data=[asdict(dc) for dc in assets])
        sleep(0.11)
        df.to_sql(name='assets_stream', con=engine, if_exists='append', index=False)


def _request(url: str) -> Dict[str, Any]:
    response = requests.get(url, headers=_HTTP_HEADERS)
    print(f'HTTP {response.status_code} {url}')
    try:
        response.raise_for_status()
        return response.json()  # type: ignore [no-any-return]
    except requests.RequestException as error:
        raise error


def _execute_sql(sql_str: str, retries=0) -> Union[Exception, pd.DataFrame]:
    engine = _get_sql_engine()
    try:
        with engine.connect() as conn:
            try:
                return pd.read_sql(sql_str, conn)
            except Exception as e:
                if not retries:
                    raise e
                raise e  # TODO: retry logic goes here
    except Exception as err:
        return err


if __name__ == '__main__':
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx

    is_streamlit = bool(get_script_run_ctx())

    if is_streamlit:
        nl_str = st.text_input('Natural language query')
        if st.button('Execute'):
            sql_str = _nl_to_sql(nl_str)
            companies = _extract_companies_from_sql(sql_str)
            try:
                _prep_db_for_companies(companies)
            except Exception as error:
                st.write(sql_str)
                st.write(error)
            response = _execute_sql(sql_str)
            if isinstance(response, Exception):
                st.write(response)
            else:
                st.write(sql_str)
                st.write(response)

    else:
        while True:
            nl_str = input('\nEnter a database query in plain english, or enter "q" to exit\n> ')
            if nl_str == 'q':
                break
            sql_str = _nl_to_sql(nl_str)
            companies = _extract_companies_from_sql(sql_str)
            try:
                _prep_db_for_companies(companies)
            except Exception as error:
                print(sql_str)
                print(error)
                continue
            response = _execute_sql(sql_str)
            print('\n\n')
            if isinstance(response, Exception):
                print(response)
            else:
                print(sql_str)
                print(response)
