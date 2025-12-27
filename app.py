import time
import streamlit as st
from utils import (use_pandas,
                   use_polars,
                   get_anomalies_chart,
                   get_season_charts,
                   parse_output)
from api import fetch_data_sync, fetch_data_async, fetch_many_async
import asyncio
import aiohttp


@st.cache_data(show_spinner="Анализ...")
def analize(file):
    df_pandas, time_pandas = use_pandas(file)
    df_polars, time_polars = use_polars(file)
    return df_pandas, df_polars, time_pandas, time_polars



def main():
    st.set_page_config(layout="wide")
    if "sync_result" not in st.session_state:
        st.session_state.sync_result = None
        st.session_state.sync_time = None

    if "async_result" not in st.session_state:
        st.session_state.async_result = None
        st.session_state.async_time = None

    if "sync_result_all_cities" not in st.session_state:
        st.session_state.sync_result_all_cities = None
        st.session_state.sync_time_all_cities = None

    if "async_result_all_cities" not in st.session_state:
        st.session_state.async_result_all_cities = None
        st.session_state.async_time_all_cities = None


    st.title("Анализ данных с использованием Streamlit")
    st.write("Это интерактивное приложение для анализа данных.")

    st.subheader("Загрузка данных")
    uploaded_file = st.file_uploader("Выберите CSV-файл", type=["csv"])


    if uploaded_file is not None:

        st.subheader('Анализ данных с помощью pandas и polars')
        df_pandas, df_polars, time_pandas, time_polars = analize(uploaded_file)


        st.write(f"Использование Pandas: {time_pandas:.5f} сек")
        st.write(f"Использование Polars: {time_polars:.5f} сек")
        st.write(f"Ускорение в: {time_pandas / time_polars:.2f} раз")

        target_city = st.selectbox("Выберите город", df_pandas.city.unique(), index=None, placeholder="Gotham?...")
        if target_city:
            one_city = df_pandas.loc[df_pandas['city'] == target_city].copy()

            st.dataframe(
                one_city[["temperature", "rolling_mean"]].describe(),
                width='stretch'
            )
            fig = get_anomalies_chart(one_city, target_city)
            st.plotly_chart(fig, width='stretch')

            fig2 = get_season_charts(one_city, target_city)
            st.plotly_chart(fig2, width='stretch')


            st.subheader('Проверка текущей погоды')
            api_key = st.text_input("Your api key", placeholder='abc...')

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Sync"):
                    start_sync_one_city = time.perf_counter()
                    data_sync = fetch_data_sync(target_city, api_key)
                    end_sync_one_city = time.perf_counter()
                    st.session_state.sync_time = end_sync_one_city - start_sync_one_city
                    st.session_state.sync_result = parse_output(data_sync, df_pandas)
            with col2:
                if st.button("Async"):
                    async def run():
                        async with aiohttp.ClientSession() as session:
                            return await fetch_data_async(target_city, api_key, session)

                    start_async_one_city = time.perf_counter()
                    data_async = asyncio.run(run())
                    end_async_one_city = time.perf_counter()
                    st.session_state.async_time = end_async_one_city - start_async_one_city
                    st.session_state.async_result = parse_output(data_async, df_pandas)


            col1, col2 = st.columns(2)
            with col1:
                if st.session_state.sync_result:
                    st.write(f"Время (sync): {st.session_state.sync_time:.4f} сек")
                    parsed_answer_sync = st.session_state.sync_result
                    if parsed_answer_sync['code'] == 200:
                        st.metric(f"Температура в {target_city}", f"{parsed_answer_sync['temp']} °C")
                        st.write("Температура аномальна" if parsed_answer_sync['anomalous'] else "Обычная температура")
                    elif parsed_answer_sync['code'] == 401:
                        st.write('ERROR!!!  ' + parsed_answer_sync['message'])
                    else:
                        st.write(parsed_answer_sync['message'])
            with col2:
                if st.session_state.async_result:
                    st.write(f"Время (async): {st.session_state.async_time:.4f} сек")
                    parsed_answer_async = st.session_state.async_result
                    if parsed_answer_async['code'] == 200:
                        st.metric(f"Температура в {target_city}", f"{parsed_answer_async['temp']} °C")
                        st.write("Температура аномальна" if parsed_answer_async['anomalous'] else "Обычная температура")
                    elif parsed_answer_async['code'] == 401:
                        st.write('ERROR!!!  ' + parsed_answer_async['message'])
                    else:
                        st.write(parsed_answer_async['message'])


            st.subheader('Поиск городов, где текущая температура считается аномальной')
            cities = sorted(df_pandas.city.unique())


            col1, col2 = st.columns(2)
            with col1:
                if st.button("Sync all cities"):
                    start_sync_all_cities = time.perf_counter()
                    sync_results = [
                        fetch_data_sync(city, api_key)
                        for city in cities
                    ]
                    end_sync_all_cities = time.perf_counter()
                    st.session_state.sync_time_all_cities = end_sync_all_cities - start_sync_all_cities
                    all_parsed_sync = [parse_output(i, df_pandas) for i in sync_results]
                    st.session_state.sync_result_all_cities = all_parsed_sync
            with col2:
                if st.button("Async all cities"):
                    start_async_all_cities = time.perf_counter()
                    async_results = asyncio.run(
                        fetch_many_async(cities, api_key)
                    )
                    end_async_all_cities = time.perf_counter()
                    st.session_state.async_time_all_cities = end_async_all_cities - start_async_all_cities
                    all_parsed_async = [parse_output(i, df_pandas) for i in async_results]
                    st.session_state.async_result_all_cities = all_parsed_async


            col1, col2 = st.columns(2)
            with col1:
                if st.session_state.sync_result_all_cities:
                    st.write(f"Время (sync): {st.session_state.sync_time_all_cities:.4f} сек")
                    parsed_answer_sync = st.session_state.sync_result_all_cities
                    if all([_city['code'] == 200 for _city in parsed_answer_sync]):
                        if any([_city['anomalous'] for _city in parsed_answer_sync]):
                            st.write("Температура аномальна в ", [_city['city'] for _city in parsed_answer_sync if _city['anomalous']])
                        else:
                            st.write("Температура везде обычная")

                    else:
                        st.write('Проверьте API и доступность сервиса OpenWeatherMap')

            with col2:
                if st.session_state.async_result_all_cities:
                    st.write(f"Время (async): {st.session_state.async_time_all_cities:.4f} сек")
                    parsed_answer_async = st.session_state.async_result_all_cities
                    if all([_city['code'] == 200 for _city in parsed_answer_async]):
                        if any([_city['anomalous'] for _city in parsed_answer_async]):
                            st.write("Температура аномальна в ", [_city['city'] for _city in parsed_answer_async if _city['anomalous']])
                        else:
                            st.write("Температура везде обычная")

                    else:
                        st.write('Проверьте API и доступность сервиса OpenWeatherMap')



if __name__ == "__main__":
    main()
