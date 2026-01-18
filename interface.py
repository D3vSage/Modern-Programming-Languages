import os
import io
from datetime import datetime, timedelta, date

import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Данные торгов",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stButton>button {
        background-color: #1E3A8A;
        color: white;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "127.0.0.1").strip(),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "spimex_db").strip(),
            user=os.getenv("DB_USER", "postgres").strip(),
            password=os.getenv("DB_PASSWORD", "").strip(),
        )
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к БД: {e}")
        return None


def load_data_from_db() -> pd.DataFrame:
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()

    try:
        query = 'SELECT * FROM trade_data ORDER BY "Дата" DESC'
        df = pd.read_sql_query(query, conn)

        if "Дата" in df.columns:
            df["Дата"] = pd.to_datetime(df["Дата"], errors="coerce").dt.date

        numeric_cols = [
            "ОбъемДоговоровЕИ", "ОбъемДоговоровРуб",
            "ИзмРынРуб", "ИзмРынПроц",
            "МинЦена", "СреднЦена", "МаксЦена", "РынЦена",
            "ЛучшПредложение", "ЛучшСпрос", "КоличествоДоговоров",
        ]
        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        return df

    except Exception as e:
        st.error(f"Ошибка загрузки данных: {e}")
        return pd.DataFrame()


def export_to_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="СПБ_Данные")
    return output.getvalue()


def export_to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def get_date_bounds(df: pd.DataFrame):
    if "Дата" not in df.columns or df["Дата"].isnull().all():
        today = datetime.now().date()
        return today - timedelta(days=30), today

    dmin = df["Дата"].min()
    dmax = df["Дата"].max()

    if not isinstance(dmin, date):
        dmin = pd.to_datetime(dmin, errors="coerce").date()
    if not isinstance(dmax, date):
        dmax = pd.to_datetime(dmax, errors="coerce").date()

    return dmin, dmax


def apply_filters(
    df: pd.DataFrame,
    start_date: date,
    end_date: date,
    selected_instruments,
    selected_products,
    min_price: float,
    max_price: float
) -> pd.DataFrame:
    filtered = df.copy()

    if "Дата" in filtered.columns and start_date and end_date:
        mask = (filtered["Дата"] >= start_date) & (filtered["Дата"] <= end_date)
        filtered = filtered[mask]

    if selected_instruments and "КодИнструмента" in filtered.columns:
        filtered = filtered[filtered["КодИнструмента"].isin(selected_instruments)]

    if selected_products and "Товар" in filtered.columns:
        filtered = filtered[filtered["Товар"].isin(selected_products)]

    if "СреднЦена" in filtered.columns:
        if min_price is not None and float(min_price) > 0:
            filtered = filtered[filtered["СреднЦена"] >= float(min_price)]
        if max_price is not None and float(max_price) > 0:
            filtered = filtered[filtered["СреднЦена"] <= float(max_price)]

    return filtered


def set_defaults_and_reset(df: pd.DataFrame):
    dmin, dmax = get_date_bounds(df)

    st.session_state["f_start_date"] = dmin
    st.session_state["f_end_date"] = dmax
    st.session_state["f_instruments"] = []
    st.session_state["f_products"] = []
    st.session_state["f_min_price"] = 0.0
    st.session_state["f_max_price"] = 0.0

    st.session_state["filtered_df"] = df.copy()


def main():
    st.markdown('<h1 class="main-header">Анализ данных торгов</h1>', unsafe_allow_html=True)

    with st.spinner("Загрузка данных из базы..."):
        df = load_data_from_db()

    if df.empty:
        st.warning("Нет данных для отображения")
        return

    dmin, dmax = get_date_bounds(df)

    all_instruments = sorted(df["КодИнструмента"].dropna().unique()) if "КодИнструмента" in df.columns else []
    all_products = sorted(df["Товар"].dropna().unique()) if "Товар" in df.columns else []

    if "filtered_df" not in st.session_state:
        st.session_state["filtered_df"] = df.copy()

    if "f_start_date" not in st.session_state:
        st.session_state["f_start_date"] = dmin
    if "f_end_date" not in st.session_state:
        st.session_state["f_end_date"] = dmax
    if "f_instruments" not in st.session_state:
        st.session_state["f_instruments"] = []
    if "f_products" not in st.session_state:
        st.session_state["f_products"] = []
    if "f_min_price" not in st.session_state:
        st.session_state["f_min_price"] = 0.0
    if "f_max_price" not in st.session_state:
        st.session_state["f_max_price"] = 0.0


    with st.sidebar:
        st.header("🔍 Фильтры")

        if st.button("Сбросить фильтры", use_container_width=True):
            set_defaults_and_reset(df)
            st.rerun()

        with st.form("filters_form", border=True):
            st.subheader("Период дат")
            st.date_input("Начало", key="f_start_date")
            st.date_input("Конец", key="f_end_date")

            st.subheader("Инструменты")
            st.multiselect(
                "Выберите инструменты (пусто = все)",
                options=all_instruments,
                key="f_instruments"
            )

            st.subheader("Тип товара")
            st.multiselect(
                "Выберите товары (пусто = все)",
                options=all_products,
                key="f_products"
            )

            st.subheader("Диапазон цен")
            st.number_input(
                "Минимальная цена",
                min_value=0.0,
                step=1000.0,
                key="f_min_price"
            )
            st.number_input(
                "Максимальная цена (0 = без ограничения)",
                min_value=0.0,
                step=1000.0,
                key="f_max_price"
            )

            applied = st.form_submit_button("Применить", use_container_width=True)

        if applied:
            st.session_state["filtered_df"] = apply_filters(
                df=df,
                start_date=st.session_state["f_start_date"],
                end_date=st.session_state["f_end_date"],
                selected_instruments=st.session_state["f_instruments"],
                selected_products=st.session_state["f_products"],
                min_price=st.session_state["f_min_price"],
                max_price=st.session_state["f_max_price"],
            )

    filtered_df = st.session_state["filtered_df"]

    st.info(f"Найдено записей после фильтров: **{len(filtered_df)}**")


    st.subheader("Статистика")
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric("Всего записей", len(filtered_df))
    with c2:
        st.metric(
            "Уникальных инструментов",
            filtered_df["КодИнструмента"].nunique() if "КодИнструмента" in filtered_df.columns else 0
        )
    with c3:
        avg_price = filtered_df["СреднЦена"].mean() if "СреднЦена" in filtered_df.columns else None
        st.metric("Средняя цена", f"{avg_price:,.0f} Руб." if avg_price is not None and not pd.isna(avg_price) else "N/A")
    with c4:
        total_volume = filtered_df["ОбъемДоговоровРуб"].sum() if "ОбъемДоговоровРуб" in filtered_df.columns else None
        st.metric("Общий объем", f"{total_volume:,.0f} Руб." if total_volume is not None and not pd.isna(total_volume) else "N/A")


    st.subheader("Показать колонки")

    all_columns = list(filtered_df.columns)
    default_columns = ["Дата", "КодИнструмента", "Товар", "СреднЦена", "ОбъемДоговоровРуб"]
    visible_columns = st.multiselect(
        "",
        options=all_columns,
        default=[c for c in default_columns if c in all_columns]
    )

    display_df = filtered_df[visible_columns] if visible_columns else filtered_df

    sort_column = st.selectbox("Сортировать по", options=display_df.columns, index=0)
    sort_ascending = st.checkbox("По возрастанию", value=False)
    display_df = display_df.sort_values(by=sort_column, ascending=sort_ascending)

    st.dataframe(display_df, use_container_width=True, height=420)


    st.subheader("Экспорт данных")

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    col_e1, col_e2 = st.columns(2)

    with col_e1:
        st.download_button(
            label="Экспорт в Excel",
            data=export_to_excel(filtered_df),
            file_name=f"spimex_data_{ts}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    with col_e2:
        st.download_button(
            label="Экспорт в CSV",
            data=export_to_csv(filtered_df),
            file_name=f"spimex_data_{ts}.csv",
            mime="text/csv",
            use_container_width=True
        )


if __name__ == "__main__":
    main()
