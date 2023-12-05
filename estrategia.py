import pyarrow.parquet as pq
from pathlib import Path
import pandas as pd
from datetime import datetime


# ESTRATEGIA 0


def calculate_rsi(data, window=14):
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


# ESTRATEGIA 1


def estrategia(df, w, n, vl, rsi, vct, taxa):
    df = df.copy()

    df["media_movel"] = df["close"].rolling(w).mean()

    df["media_std"] = df["close"].rolling(w).std()

    df["media_volume"] = df["volume"].rolling(w).mean()

    df["parte_superior"] = df["media_movel"] + n * df["media_std"]
    df["parte_inferior"] = df["media_movel"] - n * df["media_std"]
    df["boleano"] = df["volume"] > df["media_volume"]

    df["RSI"] = calculate_rsi(df["close"])

    taxas_custo_percentual = taxa

    operacoes_aberta = 0
    trades = []

    df["time"] = df.index

    dict_map = {j: i for i, j in enumerate(df.columns)}

    quantidade_compra = 0

    for row in df.values:
        preco_atual = row[dict_map["close"]]

        if rsi:
            b = (
                True
                if row[dict_map["RSI"]] <= 30 or row[dict_map["RSI"]] >= 70
                else False
            )
        else:
            b = True

        if (
            operacoes_aberta == 0
            and preco_atual < row[dict_map["parte_inferior"]]
            and row[dict_map["boleano"]] == 1
            and b
        ):
            quantidade_compra = vl / preco_atual

            operacoes_aberta = 1
            trades += [
                {
                    "time": row[dict_map["time"]],
                    "side": "C",
                    "price": preco_atual,
                    "volume": quantidade_compra,
                    "aplicado": vl + (vl * taxas_custo_percentual / 100),
                    "taxa": (vl * taxas_custo_percentual / 100),
                }
            ]
        elif (
            operacoes_aberta == 1
            and preco_atual > row[dict_map["parte_superior"]]
            and b
        ):
            valor = preco_atual * quantidade_compra

            if vct:
                b1 = True if (1 - (vl / valor)) > taxas_custo_percentual else False
            else:
                b1 = True

            if b1:
                operacoes_aberta = 0

                print(f"vct: {vct} - b1: {b1} -> VL:{valor:.2f} {vl:.2f}")

                trades += [
                    {
                        "time": row[dict_map["time"]],
                        "side": "V",
                        "price": preco_atual,
                        "volume": quantidade_compra,
                        "aplicado": valor - (valor * taxas_custo_percentual / 100),
                        "taxa": (valor * taxas_custo_percentual / 100),
                    }
                ]

    df_trades = pd.DataFrame(trades)
    df.set_index("time", inplace=True)

    return df_trades


# TESTE UNITARIO


def teste_unitario(df, w, n, vl, rsi, vct, taxa):
    df_trades = estrategia(df, w, n, vl, rsi, vct, taxa)
    return df_trades

    # df_trades.to_excel("df_trades_teste_unitario.xlsx", index=False)


# TESTE MULTIPLOS


def teste_multiplos_W_N(df, vl, rsi, vct, taxa):
    base = []
    df_dados = pd.DataFrame()

    for i in range(10, 300, 10):
        for j in [1, 2]:
            print(i, j)
            df_trades = estrategia(df, i, j, vl, rsi, vct, taxa)

            df_juntos = pd.concat([df_dados, df_trades])
            df_juntos["W"] = i
            df_juntos["N"] = j

            df_trades["ret-aplicado"] = df_trades["aplicado"] - df_trades[
                "aplicado"
            ].shift(1)

            soma2 = df_trades[df_trades["side"] == "V"]["ret-aplicado"].sum()

            base.append(
                {
                    "Lucro": f"{soma2:.2f}",
                    "W": f"{i:.0f}",
                    "N": f"{j:.2f}",
                    "QTD Operacões": len(df_trades),
                }
            )

    lista_ordenada = sorted(base, key=lambda x: float(x["Lucro"]), reverse=True)[:5]

    return [lista_ordenada, df_juntos]


#  TESTE POR PERIODO


def teste_por_periodo(df, w, n, vl, rsi, vct, ano, fixo, taxa):
    df_filtro_lista = []

    if fixo:
        lista = [ano[1]]
    else:
        lista = [i for i in range(ano[0], ano[1] + 1)]

    for j in lista:
        for i in range(1, 13):
            print(f"{i:02d}/{j}")

            if j == 2023 and i > 10:
                break

            df_filtro = df[
                (df.index.month >= i) & (df.index.month <= i) & (df.index.year == j)
            ]

            df_filtro_lista.append(df_filtro)

    df_filtro = pd.concat(df_filtro_lista)

    df_trades = estrategia(df_filtro, w, n, vl, rsi, vct, taxa)
    df_trades.set_index("time", inplace=True)

    df_trades["ret-aplicado"] = df_trades["aplicado"] - df_trades["aplicado"].shift(1)

    df_trades["lucro %"] = df_trades["ret-aplicado"] / vl * 100

    df_trades.index = pd.to_datetime(df_trades.index)
    print(df_trades.index)

    df_trades["ano"] = df_trades.index.year
    df_trades["mes"] = df_trades.index.month

    grupo = (
        df_trades[df_trades["side"] == "V"]
        .groupby(["ano", "mes", "side"])
        .agg({"ret-aplicado": "sum", "lucro %": "sum", "taxa": "count"})
        .reset_index()
    )

    print(grupo.columns)

    return grupo
