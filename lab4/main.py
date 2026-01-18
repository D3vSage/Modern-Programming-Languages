import os
import requests
import pandas as pd
from dotenv import load_dotenv

from sqlalchemy import create_engine
from sqlalchemy.types import String, Integer, Float, Date, BigInteger
from urllib.parse import quote_plus


# Скачиваем файл со страницы
url = "https://spimex.com//files/trades/result/upload/reports/oil_xls/oil_xls_20251210162000.xls?r=8982&amp;p=L3VwbG9hZC9yZXBvcnRzL3BkZi9vaWwvb2lsXzIwMjUxMjEwMTYyMDAwLnBkZg.."
response = requests.get(url)
print("HTTP:", response.status_code)

with open('spimex_file_original.xls', 'wb') as f:
    f.write(response.content)
print("✓ Файл сохранен как 'spimex_file_original.xls'")


# Парсим файл
def simple_extract_data(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path, header=None, dtype=str)

    data_rows = []
    trade_date_str = ""
    inside_data_table = False

    i = 0
    while i < len(df):
        cell_value = str(df.iloc[i, 1]) if pd.notna(df.iloc[i, 1]) else ""

        if 'Код\nИнструмента' in cell_value.replace(' ', ''):
            inside_data_table = True
            i += 2
            continue

        if "Дата торгов:" in cell_value:
            parts = cell_value.split(": ")
            if len(parts) > 1:
                trade_date_str = parts[1].strip()
            i += 1
            continue

        if 'Итого:' in cell_value and inside_data_table:
            inside_data_table = False
            i += 1
            continue

        if inside_data_table:
            data_rows.append(df.iloc[i].tolist())

        i += 1

    trade_date_iso = None
    if trade_date_str:
        arr = trade_date_str.split(".")
        if len(arr) == 3:
            trade_date_iso = f"{arr[2]}-{arr[1]}-{arr[0]}"

    result_df = pd.DataFrame(data_rows, columns=df.columns)
    result_df['Дата'] = trade_date_iso
    print(f"Извлечено строк: {len(result_df)}")
    return result_df


data = simple_extract_data("spimex_file_original.xls")

# Преобразуем полученные данные
data = data.iloc[:, 1:].copy()

new_column_names = [
    'КодИнструмента',
    'НаименованиеИнструмента',
    'БазисПоставки',
    'ОбъемДоговоровЕИ',
    'ОбъемДоговоровРуб',
    'ИзмРынРуб',
    'ИзмРынПроц',
    'МинЦена',
    'СреднЦена',
    'МаксЦена',
    'РынЦена',
    'ЛучшПредложение',
    'ЛучшСпрос',
    'КоличествоДоговоров',
    'Дата'
]
data.columns = new_column_names

data['Товар'] = data['НаименованиеИнструмента'].apply(
    lambda x: x.split(',')[0] if isinstance(x, str) and ',' in x else x
)

data = data.replace('-', None)

# Нормализуем типы
def to_int_series(s: pd.Series) -> pd.Series:
    cleaned = s.astype(str).str.replace(r"[^\d\-]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce").astype("Int64")

def to_float_series(s: pd.Series) -> pd.Series:
    cleaned = (
        s.astype(str)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(r"[^0-9\.\-]", "", regex=True)
    )
    return pd.to_numeric(cleaned, errors="coerce")

int_cols = ['ОбъемДоговоровЕИ', 'ЛучшПредложение', 'ЛучшСпрос', 'КоличествоДоговоров']
bigint_cols = ['ОбъемДоговоровРуб']
float_cols = ['ИзмРынРуб', 'ИзмРынПроц', 'МинЦена', 'СреднЦена', 'МаксЦена', 'РынЦена']

for c in int_cols:
    if c in data.columns:
        data[c] = to_int_series(data[c])

for c in bigint_cols:
    if c in data.columns:
        data[c] = to_int_series(data[c])

for c in float_cols:
    if c in data.columns:
        data[c] = to_float_series(data[c])

data['Дата'] = pd.to_datetime(data['Дата'], errors='coerce').dt.date

data.to_csv("Parsed_data.csv", index=False)


# Загружаем в PostgreSQL
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "spimex_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

DB_URL = (
    f"postgresql+psycopg2://{quote_plus(DB_CONFIG['user'])}:{quote_plus(DB_CONFIG['password'])}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

def load_via_sqlalchemy(df: pd.DataFrame, db_url: str, table_name: str = 'trade_data') -> bool:
    engine = create_engine(db_url)

    dtype_mapping = {
        'КодИнструмента': String(50),
        'НаименованиеИнструмента': String(1000),
        'БазисПоставки': String(500),
        'ОбъемДоговоровЕИ': Integer(),
        'ОбъемДоговоровРуб': BigInteger(),
        'ИзмРынРуб': Float(),
        'ИзмРынПроц': Float(),
        'МинЦена': Float(),
        'СреднЦена': Float(),
        'МаксЦена': Float(),
        'РынЦена': Float(),
        'ЛучшПредложение': Integer(),
        'ЛучшСпрос': Integer(),
        'КоличествоДоговоров': Integer(),
        'Дата': Date(),
        'Товар': String(200)
    }

    try:
        df.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False,
            dtype=dtype_mapping,
            method='multi'
        )
        print(f"Успешно загружено {len(df)} записей в {table_name}")
        return True
    except Exception as e:
        print(f"Ошибка при загрузке: {e}")
        return False


load_via_sqlalchemy(data, DB_URL, "trade_data")
