"""
Based on: https://medium.datadriveninvestor.com/access-companies-sec-filings-using-python-760e6075d3ad
"""

import re
from dataclasses import asdict, dataclass
from datetime import datetime
from time import sleep
from typing import Any, Dict, List, Optional, Sequence, Union

import openai
import pandas as pd
import requests
import sqlalchemy as db

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

_TABLE_NAME = 'company_events'
_SQL_ENGINE: Optional[db.engine.Engine] = None
_TICKER_TO_COMPANY_MAP: Optional[Dict[str, 'Company']] = None
_HTTP_HEADERS: Dict[str, str] = {'User-Agent': 'scripting@rhetoric.app'}


@dataclass
class Company:
    cik: str
    title: str
    ticker: str


@dataclass
class _BaseMetric:
    timestamp: datetime
    company: str
    fiscal_year: int
    fiscal_quarter: str

    @staticmethod
    def from_json(ticker: str, obj: Dict[str, Any]) -> '_BaseMetric':
        return _BaseMetric(
            timestamp=datetime.strptime(obj['end'], '%Y-%m-%d'),
            company=ticker,
            fiscal_year=obj['fy'],
            fiscal_quarter=obj['fp'],
        )


@dataclass
class Asset(_BaseMetric):
    """
    Sum of the carrying amounts as of the balance sheet date of all assets that are recognized. Assets are probable
    future economic benefits obtained or controlled by an entity as a result of past transactions or events.
    """

    total_assets_usd: int

    @staticmethod
    def from_json(ticker: str, obj: Dict[str, Any]) -> 'Asset':
        return Asset(total_assets_usd=obj['val'], **asdict(_BaseMetric.from_json(ticker, obj)))


@dataclass
class CashAndCashEquivalents(_BaseMetric):
    """
    Amount of currency on hand as well as demand deposits with banks or financial institutions. Includes other kinds of
    accounts that have the general characteristics of demand deposits. Also includes short-term, highly liquid
    investments that are both readily convertible to known amounts of cash and so near their maturity that they present
    insignificant risk of changes in value because of changes in interest rates. Excludes cash and cash equivalents
    within disposal group and discontinued operation.
    """

    cash_and_cash_equivalents_usd: int

    @staticmethod
    def from_json(ticker: str, obj: Dict[str, Any]) -> 'CashAndCashEquivalents':
        return CashAndCashEquivalents(
            cash_and_cash_equivalents_usd=obj['val'],
            **asdict(_BaseMetric.from_json(ticker, obj)),
        )


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
    data = _get_tag_for_ticker(ticker, tag='Assets')
    return [Asset.from_json(ticker=ticker, obj=obj) for obj in data['units']['USD']]


def _get_cash_and_cash_equivalents_for_ticker(ticker: str) -> List['CashAndCashEquivalents']:
    """
    Get all filings for the "CashAndCashEquivalentsAtCarryingValue" tag for the given company as
    `CashAndCashEquivalents` objects.
    """
    data = _get_tag_for_ticker(ticker, tag='CashAndCashEquivalentsAtCarryingValue')
    return [CashAndCashEquivalents.from_json(ticker=ticker, obj=obj) for obj in data['units']['USD']]


def _nl_to_sql(nl_query: str) -> str:
    """
    Use GPT to translate a natural-language question into SQL.
    """
    response = openai.Completion.create(
        model="code-davinci-002",
        prompt=(
            'Given a sqlite database table named "{table_name}" with the following structure:\n'
            '_________________\n'
            'timestamp (DATETIME)\n'
            'company (TEXT)\n'
            'total_assets_usd (FLOAT)\n'
            'cash_and_cash_equivalents_usd (FLOAT)\n'
            'fiscal_year (INT)\n'
            'fiscal_quarter (TEXT)\n'
            '_________________\n'
            '\n'
            'Respond according to the following rules:\n'
            ' - The response must be a syntactically correct sqlite query.\n'
            ' - The response may only SELECT from the "{table_name}" table.\n'
            '\n'
            'The value of the "company" column must be a known SEC stock ticker symbol.\n'
            'For example \'LYFT\', \'AAPL\', \'UBER\', \'DASH\'.\n'
            '\n'
            'The value of the "company" column must be a known SEC stock ticker symbol.\n'
            'For example \'LYFT\', \'AAPL\', \'UBER\', \'DASH\'.\n'
            '\n'
            'Respond in the following format:\n'
            '_________________\n'
            'QUESTION: A natural language question in English?\n'
            'SQL: Sqlite SQL Query to run\n'
            '_________________\n'
            '\n'
            'QUESTION: {nl_query}\n'
            'SQL:'
        ).format(table_name=_TABLE_NAME, nl_query=nl_query),
        temperature=0,
        max_tokens=256,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=[";", "\n\n"],
    )
    return response.choices[0].text.strip()  # type: ignore [no-any-return]


def _nl_to_metric_names(nl_query: str) -> List[str]:
    """
    Use GPT to extract metric names from a natural-language question.
    """
    response = openai.Completion.create(
        model="text-davinci-003",
        prompt=(
            'The following is a list of corporate accounting definitions in the format "token": "A definition of the token":\n'
            '__________\n'
            '"assets_usd": "Sum of the carrying amounts as of the balance sheet date of all assets that are recognized.'
            ' Assets are probable future economic benefits obtained or controlled by an entity as a result of past'
            ' transactions or events."\n'
            '\n'
            '"cash_and_cash_equivalents_usd": "Amount of currency on hand as well as demand deposits with banks or'
            ' financial institutions. Includes other kinds of accounts that have the general characteristics of demand'
            ' deposits."\n'
            '__________\n'
            '\n'
            'Respond according to the following rules:\n'
            '- The response may only include one of the above tokens\n'
            '- The response must be in JSON array format, e.g. ["assets_usd"]\n'
            '- The response must only include tokens that might be used to answer the question\n'
            '- If none of the tokens might be used to answer the question, return the empty array: []\n'
            '\n'
            'Respond in the following format:\n'
            '__________\n'
            'QUESTION: A financial question in english\n'
            'RESPONSE: ["example_token_1", "example_token_2"]\n'
            '__________\n'
            '\n'
            'QUESTION: {nl_query}\n'
            'RESPONSE:'
        ).format(nl_query=nl_query),
        temperature=0,
        max_tokens=256,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=["]", "\n"],
    )
    return response.choices[0].text.strip()  # type: ignore [no-any-return]


def _extract_companies_from_sql(sql_str) -> List['Company']:
    """
    For a given SQL query, extract any referenced Companies by their ticker symbol.
    """
    ticker_map = _get_ticker_to_company_map()
    maybe_tickers: List[str] = re.findall('\'([A-Z]+)\'', sql_str)
    return [ticker_map[t] for t in maybe_tickers if t in ticker_map]


def _merge_metrics(*metric_lists: Sequence[_BaseMetric]) -> pd.DataFrame:
    df: Optional[pd.DataFrame] = None
    for metric_list in metric_lists:
        new_df = pd.DataFrame(data=[asdict(dc) for dc in metric_list])
        if df is None:
            df = new_df
            continue
        df = pd.merge(df, new_df, on=['company', 'fiscal_year', 'fiscal_quarter'], how='outer')
        df = df.drop_duplicates(['company', 'fiscal_year', 'fiscal_quarter'])
    return df


def _prep_db_for_companies(companies: List['Company']) -> None:
    if not companies:
        raise Exception(
            'No companies matched your query.\n'
            'Please be sure to reference one or more specific companies by name or ticker symbol.'
        )
    engine = _get_sql_engine()
    for company in companies:
        assets = _get_assets_for_ticker(company.ticker)
        cash_and_equivalents = _get_cash_and_cash_equivalents_for_ticker(company.ticker)
        # df = pd.DataFrame(data=[asdict(dc) for dc in assets])
        df = _merge_metrics(assets, cash_and_equivalents)
        sleep(0.11)
        df.to_sql(name=_TABLE_NAME, con=engine, if_exists='append', index=False)


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
