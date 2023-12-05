import pyarrow.parquet as pq
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
import calendar


from estrategia import teste_unitario, teste_multiplos_W_N, teste_por_periodo




primaryColor="#F63366"
backgroundColor="#FFFFFF"
secondaryBackgroundColor="#F0F2F6"
textColor="#262730"
font="sans serif"


st.set_page_config(page_title="BTC", layout="wide")


# LEITURA


@st.cache_data
def leitura(filtrar=True, filtro1=None, filtro2=None):
    dataframes = []

    for arquivo in Path().parent.glob("*.parquet"):
        df = (
            pq.ParquetFile(arquivo)
            .read(columns=["Close", "Volume", "Timestamp"])
            .to_pandas()
        )
        df.rename(
            columns={"Close": "close", "Volume": "volume", "Timestamp": "time"},
            inplace=True,
        )
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        df.set_index("time", inplace=True)

        dataframes.append(df)

    df_juntos = pd.concat(dataframes)
    df_juntos = df_juntos[~df_juntos.index.duplicated(keep="first")]

    df_juntos = df_juntos[df_juntos["close"] > 0]

    if filtrar:
        dt_i = datetime(filtro1.year, filtro1.month, filtro1.day)
        dt_f = datetime(filtro2.year, filtro2.month, filtro2.day) + timedelta(days=1)

        df_filtro = df_juntos[(df_juntos.index >= dt_i) & (df_juntos.index <= dt_f)]

        return df_filtro
    else:
        return df_juntos


col01, col02, col03, col04 = st.sidebar.columns([0.3, 0.2, 0.3, 0.2])


valor_aplicacao_real = float(col01.text_input("Aplicado:(R$)", value="500"))
contacaoUSDT = float(col02.text_input("R$/USDT", value="5"))

vct_checkbox = col03.checkbox("Venda > (Compra - Taxa)", value=True)

taxa = float(col04.text_input("TAXA", value="0.10"))


if valor_aplicacao_real or contacaoUSDT:
    valor_investimento = valor_aplicacao_real / contacaoUSDT
    st.sidebar.markdown(f"USDT: {valor_investimento:.2f}")


cola1, cola2, cola3 = st.sidebar.columns([0.4, 0.4, 0.2])


dt_inicial = cola1.date_input(
    "Data inicial:",
    format="DD/MM/YYYY",
    value=datetime(2023, 1, 1),
    min_value=datetime(2023, 1, 1),
    max_value=datetime(2023, 12, 31),
)

if dt_inicial:
    ultimo_dia_do_mes = calendar.monthrange(dt_inicial.year, dt_inicial.month)[1]
    data_final = datetime(dt_inicial.year, dt_inicial.month, ultimo_dia_do_mes)

    st.session_state["datainicila"] = data_final


dt_final = cola2.date_input(
    "Data final:",
    format="DD/MM/YYYY",
    value=st.session_state["datainicila"],
    min_value=datetime(2023, 1, 1),
    max_value=datetime(2023, 12, 31),
)


checkbox_rsi = cola3.checkbox("RSI", value=True)


st.sidebar.divider()

colb1, colb2 = st.sidebar.columns([0.5, 0.5])

w = colb1.text_input("Config W:", value=20)
n = colb2.text_input("Config N:", value=2)


# FUNÇAO


def padrao(df_trades):
    df_trades["ret-aplicado"] = df_trades["aplicado"] - df_trades["aplicado"].shift(1)

    filtro = df_trades[df_trades["side"] == "V"].copy()

    filtro["cumulative_ret"] = filtro["ret-aplicado"].cumsum()

    df_trades["cumulative_ret"] = filtro["cumulative_ret"]

    st.markdown(f"Lucro : {filtro['cumulative_ret'].iloc[-1]:.2f}")
    st.markdown(
        f"Lucro %: {filtro['cumulative_ret'].iloc[-1] / valor_investimento * 100:.2f}"
    )

    st.markdown(f"Operacões : [ {len(df_trades)} ] {df_trades['taxa'].sum():.2f} USDT")

    st.line_chart(filtro["cumulative_ret"])

    # df_trades.to_excel("teste_unitario.xlsx")

    st.table(df_trades[:1000])


calcular1 = st.sidebar.button("Teste Unitário", use_container_width=True)


# teste_unitario
if calcular1:
    df_trades = teste_unitario(
        leitura(filtro1=dt_inicial, filtro2=dt_final),
        int(w),
        float(n),
        valor_investimento,
        checkbox_rsi,
        vct_checkbox,
        taxa,
    )

    padrao(df_trades)


#


colc1, colc2 = st.sidebar.columns([0.7, 0.3])

slider_ano = colc1.slider("Ano:", 2018, 2023, (2018, 2018))
checkbox_fixo = colc2.checkbox("Fixo", value=True)


calcular3 = st.sidebar.button("Teste por  Periodo", use_container_width=True)

if calcular3:
    df = teste_por_periodo(
        leitura(filtrar=False),
        int(w),
        float(n),
        valor_investimento,
        checkbox_rsi,
        vct_checkbox,
        slider_ano,
        checkbox_fixo,
        taxa,
    )

    st.markdown("Backtesting")

    mostra_ano = (
        f" {slider_ano[1]}" if checkbox_fixo else f" {slider_ano[0]} a {slider_ano[1]}"
    )

    st.header(
        f"Aplicação Simulada de Estratégias de Investimento / ANO: {mostra_ano}",
        divider=True,
    )

    st.markdown(f"Lucro BTC/USDT: {df['ret-aplicado'].cumsum().iloc[-1]:.2f}")
    st.markdown(
        f"Lucro: {df['ret-aplicado'].cumsum().iloc[-1] / valor_investimento * 100:.2f} %"
    )
    st.markdown(f"Operacões : {len(df)}")

    st.table(df)


#

st.sidebar.divider()

calcular2 = st.sidebar.button(
    "Teste Multiplos W[10 a 300] - N[1, 2]", use_container_width=True
)


if calcular2:
    lista = teste_multiplos_W_N(
        leitura(filtro1=dt_inicial, filtro2=dt_final),
        valor_investimento,
        checkbox_rsi,
        vct_checkbox,
        taxa,
    )

    df1 = pd.DataFrame(lista[0])

    df2 = pd.DataFrame(lista[1])

    st.table(df1)
    # st.table(df2)

    st.markdown(f"Operacões : {len(df1)}")


st.sidebar.divider()


grafico = st.sidebar.button("Gráfico", use_container_width=True)

if grafico:
    gf = leitura(filtro1=dt_inicial, filtro2=dt_final)

    max = gf["close"].max()
    mim = gf["close"].min()
    med = gf["close"].mean()

    st.markdown(f"MAX : {max:.2f} | MIN : {mim:.2f} | MED : {med:.2f}")

    st.line_chart(gf, height=500)
