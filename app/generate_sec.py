"""
Based on: https://medium.datadriveninvestor.com/access-companies-sec-filings-using-python-760e6075d3ad
"""

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from time import sleep
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type, Union

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


@dataclass
class Company:
    cik: str
    title: str
    ticker: str


@dataclass
class _BaseMetric:
    tag = ''
    colname = ''
    definition = ''

    timestamp: datetime
    company: str
    fiscal_year: int
    fiscal_quarter: str

    @classmethod
    def from_json(cls, ticker: str, data: Dict[str, Any]) -> '_BaseMetric':
        return cls(
            **{
                'timestamp': datetime.strptime(data['end'], '%Y-%m-%d'),
                'company': ticker,
                'fiscal_year': data['fy'],
                'fiscal_quarter': data['fp'] if data['fp'] != 'FY' else 'Q4',
                cls.colname: data['val'],
            }
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def fetch(cls, ticker: str) -> List['_BaseMetric']:
        try:
            data = _get_tag_for_ticker(ticker, tag=cls.tag)
        except requests.exceptions.RequestException as request_error:
            if request_error.response.status_code == 404:
                return []
            raise request_error
        return [cls.from_json(ticker=ticker, data=item) for item in data['units']['USD']]


@dataclass
class Assets(_BaseMetric):
    tag = 'Assets'
    colname = 'total_assets_usd'
    definition = (
        'Assets: sum of the carrying amounts as of the balance sheet date of all assets that are recognized. Assets '
        'are probable future economic benefits obtained or controlled by an entity as a result of past transactions '
        'or events.'
    )
    total_assets_usd: int


@dataclass
class CashAndCashEquivalents(_BaseMetric):
    tag = 'CashAndCashEquivalentsAtCarryingValue'
    colname = 'cash_and_cash_equivalents_usd'
    definition = (
        'Cash and cash securities: sum of the carrying amounts as of the balance sheet date of all assets that are '
        'recognized. Assets are probable future economic benefits obtained or controlled by an entity as a result of '
        'past transactions or events.'
    )
    cash_and_cash_equivalents_usd: int


@dataclass
class MarketableSecuritiesCurrent(_BaseMetric):
    tag = 'MarketableSecuritiesCurrent'
    colname = 'marketable_securities_current_usd'
    definition = 'Marketable securities, current: amount of investment in marketable security, classified as current.'
    marketable_securities_current_usd: int


@dataclass
class NetInventory(_BaseMetric):
    tag = 'InventoryNet'
    colname = 'net_inventory_usd'
    definition = (
        'Net inventory: amount after valuation and LIFO reserves of inventory expected to be sold, or consumed within '
        'one year or operating cycle, if longer.'
    )
    net_inventory_usd: int


@dataclass
class NetDeferredTaxAssetsCurrent(_BaseMetric):
    tag = 'DeferredTaxAssetsNetCurrent'
    colname = 'deferred_tax_assets_current_usd'
    definition = (
        'Net deferred tax assets, current: Amount after allocation of valuation allowances of deferred tax asset '
        'attributable to deductible temporary differences and carryforwards classified as current.'
    )
    deferred_tax_assets_current_usd: int


@dataclass
class OtherAssetsCurrent(_BaseMetric):
    tag = 'OtherAssetsCurrent'
    colname = 'other_assets_current_usd'
    definition = 'Other assets, current: amount of current assets classified as other.'
    other_assets_current_usd: int


@dataclass
class AssetsCurrent(_BaseMetric):
    tag = 'AssetsCurrent'
    colname = 'assets_current_usd'
    definition = (
        'Assets, current: sum of the carrying amounts as of the balance sheet date of all assets that are expected to '
        'be realized in cash, sold, or consumed within one year (or the normal operating cycle, if longer). Assets are '
        'probable future economic benefits obtained or controlled by an entity as a result of past transactions or '
        'events.'
    )
    assets_current_usd: int


@dataclass
class NetPropertyPlantAndEquipment(_BaseMetric):
    tag = 'PropertyPlantAndEquipmentNet'
    colname = 'net_property_pland_and_equipment_usd'
    definition = (
        'Net property, plant, and equipment: amount after accumulated depreciation, depletion and amortization of '
        'physical assets used in the normal conduct of business to produce goods and services and not intended for '
        'resale. Examples include, but are not limited to, land, buildings, machinery and equipment, office '
        'equipment, and furniture and fixtures.'
    )
    net_property_pland_and_equipment_usd: int


TABLE_NAME = 'company_events'
METRICS: List[Type[_BaseMetric]] = [
    Assets,
    CashAndCashEquivalents,
    MarketableSecuritiesCurrent,
    NetInventory,
    NetDeferredTaxAssetsCurrent,
    OtherAssetsCurrent,
    AssetsCurrent,
    NetPropertyPlantAndEquipment,
]
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


def _nl_to_sql(nl_query: str, metrics: List[Type[_BaseMetric]]) -> str:
    """
    Use GPT to translate a natural-language question into SQL.
    """
    response = openai.Completion.create(
        model="code-davinci-002",
        prompt=(
            'The following is a desciption of a sqlite database table named "{table_name}" in the format:'
            ' column_name (COLUMN TYPE) "Column description":\n'
            '_________________\n'
            'timestamp (DATETIME) "The date that this metric was reported"\n'
            'company (TEXT) "The stock ticker symbol of the corporation"\n'
            '{metric_defs}\n'
            'fiscal_year (INT) "The 4-digit YYYY year for the given metric"\n'
            'fiscal_quarter (TEXT) "The 2-character quarter, i.e. Q1, Q2, Q3, or Q4"\n'
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
        ).format(
            table_name=TABLE_NAME,
            nl_query=nl_query,
            metric_defs='\n'.join([f'{metric.colname} (FLOAT) {metric.definition}' for metric in metrics]),
        ),
        temperature=0,
        max_tokens=256,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=[";", "\n\n"],
    )
    return response.choices[0].text.strip()  # type: ignore [no-any-return]


def _nl_to_metric_colnames(nl_query: str) -> List[str]:
    """
    Use GPT to extract metric column names from a natural-language question.
    """
    prompt = (
        'The following is a list of SEC accounting definitions in the format "token": "A definition of the token":\n'
        '__________\n'
        '{metric_defs}\n'
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
    ).format(
        nl_query=nl_query,
        metric_defs='\n\n'.join([f'"{metric.colname}": "{metric.definition}"' for metric in METRICS]),
    )
    response = openai.Completion.create(
        model="text-davinci-003",
        prompt=prompt,
        temperature=0,
        max_tokens=256,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=["\n"],
    )
    metric_names_str = response.choices[0].text.strip()
    metric_names = json.loads(metric_names_str)
    if not metric_names:
        raise Exception('No metrics matched your search. Please be sure to reference one or more specific metrics.')
    return metric_names  # type: ignore [no-any-return]


def _extract_companies_from_sql(sql_str) -> List['Company']:
    """
    For a given SQL query, extract any referenced Companies by their ticker symbol.
    """
    ticker_map = _get_ticker_to_company_map()
    maybe_tickers: List[str] = re.findall('\'([A-Z]+)\'', sql_str)
    return [ticker_map[t] for t in maybe_tickers if t in ticker_map]


def _merge_metric_data(metrics: List[Type[_BaseMetric]], *metric_data: Sequence[_BaseMetric]) -> pd.DataFrame:
    df: Optional[pd.DataFrame] = None
    join_cols = ['company', 'fiscal_year', 'fiscal_quarter']

    for metric_list in metric_data:
        if not metric_list:
            continue
        new_df = pd.DataFrame(data=[asdict(dc) for dc in metric_list])[[*join_cols, metric_list[0].colname]]
        if df is None:
            df = new_df
            continue
        df = pd.merge(df, new_df, on=join_cols, how='outer')
        df = df.drop_duplicates(join_cols)
    return df


def _prep_db_for_companies(companies: List['Company'], metrics: List[Type[_BaseMetric]]) -> None:
    df: Optional[pd.DataFrame] = None
    if not companies:
        raise Exception(
            'No companies matched your query.\n'
            'Please be sure to reference one or more specific companies by name or ticker symbol.'
        )
    engine = _get_sql_engine()
    for company in companies:
        metric_data = [metric.fetch(company.ticker) for metric in metrics]
        metric_data = [metric for metric in metric_data if metric]
        new_df = _merge_metric_data(metrics, *metric_data)

        if df is None:
            df = new_df
            continue

        df = pd.concat([df, new_df], axis=0, ignore_index=True)
        sleep(0.11)

    if df is None:
        raise Exception('The SEC API returned no data for this query.')
    df.to_sql(name=TABLE_NAME, con=engine, if_exists='append', index=False)


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


def _execute_nl(nl_query: str) -> Tuple[str, Union[pd.DataFrame, Exception]]:
    metric_colnames = _nl_to_metric_colnames(nl_query)
    metrics: List[Type[_BaseMetric]] = []
    for metric_colname in metric_colnames:
        for metric in METRICS:
            if metric.colname == metric_colname:
                metrics.append(metric)

    sql_str = _nl_to_sql(nl_query, metrics)
    companies = _extract_companies_from_sql(sql_str)

    try:
        _prep_db_for_companies(companies, metrics)
    except Exception as error:
        return (sql_str, error)
    response = _execute_sql(sql_str)
    return (sql_str, response)


if __name__ == '__main__':
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx

    is_streamlit = bool(get_script_run_ctx())

    if is_streamlit:
        st.write('You can ask for financial data for any publicly traded company for any of the following metrics:')
        st.write('\n\n'.join([metric.definition for metric in METRICS]))
        nl_query = st.text_input('Natural language query')
        if st.button('Execute'):
            sql_str, response = _execute_nl(nl_query)
            st.write(sql_str)
            st.write(response)

    else:
        while True:
            nl_query = input('\nEnter a database query in plain english, or enter "q" to exit\n> ')
            if nl_query == 'q':
                break
            print('\n\n')
            sql_str, response = _execute_nl(nl_query)
            print(sql_str)
            print(response)
