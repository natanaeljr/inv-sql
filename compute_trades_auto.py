import sqlite3
from datetime import *


# TODO:
#  liquidez + acréscimo em 1 operação
#  determinação de operações day-trade vs swing-trade
#  calculo de custo com taxas (tabela de tarifas, tabela de tipo de ação)
#  update trades_auto ao invés de delete e insert all
#  support manual overwrite (skip when column manual = Y)
#  consolidate position algo (gera tabela de posições atuais de ativos em custódia [considerar splits])


# Calcula para cada símbolo:
#  posição (número de ações atual),
#  preço médio (sem custos)
#  valor total (sem custos)
# por ativo e por corretora
# resultado é salvo na tabela trades_auto (id, posicao, preco_medio)
# splits (desdobramento e grupamentos) são levados em consideração
# param trades: list of tuples of trade info ordered by date
# param splits: list of tuples with split events for this symbol ordered by date
def compute_trades_auto(trades, splits):
    posicao = preco_medio = valor_total = 0
    for _id, op, date_str, count, value in trades:
        # adjust for splits
        date = datetime.strptime(date_str, '%Y-%m-%d').date()
        for split_date_str, ratio in splits[:]:  # splits is ordered by date
            split_date = datetime.strptime(split_date_str, '%Y-%m-%d').date()
            if split_date > date:
                break
            posicao *= ratio
            preco_medio /= ratio
            splits.remove((split_date_str, ratio))
        # update accumulated values based on operation
        if op == 'buy':
            if posicao >= 0:  # acréscimo de posição comprada
                posicao += count
                valor_total += value
                preco_medio = valor_total / posicao
            else:  # liquidação de posição vendida
                posicao += count
                preco_medio = preco_medio
                valor_total = posicao * preco_medio
        elif op == 'sell':
            if posicao <= 0:  # acréscimo de posição vendida
                posicao -= count
                valor_total -= value
                preco_medio = valor_total / posicao
            else:  # liquidação de posição comprada
                posicao -= count
                preco_medio = preco_medio
                valor_total = posicao * preco_medio
        else:
            raise Exception(f"Unknown OP {op}")
        yield _id, posicao, preco_medio


def execute_on_db():
    trades_auto = []
    symbols = dbcursor.execute("SELECT symbol, broker FROM trades GROUP BY symbol, broker").fetchall()
    for symbol, broker in symbols:
        all_symbol_trades = dbcursor.execute(
            "SELECT id, op, date, count, value FROM trades WHERE symbol = ? AND broker = ? ORDER BY date",
            (symbol, broker)).fetchall()
        all_symbol_splits = dbcursor.execute(
            "SELECT date, ratio FROM splits WHERE symbol = ? ORDER BY date",
            (symbol,)).fetchall()
        trades_auto += compute_trades_auto(all_symbol_trades, all_symbol_splits)
    dbcursor.execute("DELETE FROM trades_auto WHERE 1")
    dbcursor.executemany("INSERT INTO trades_auto VALUES (?, ?, ?)", trades_auto)
    dbcon.commit()


###############################################################################
# Main
###############################################################################

if __name__ == "__main__":
    dbcon = sqlite3.connect('db/inv.db')
    dbcursor = dbcon.cursor()
    execute_on_db()
    dbcon.close()


###############################################################################
# Tests
###############################################################################

def test_posicao_comprada():
    trades = [
        # id, op, date, count, value
        (1, 'buy', '2022-01-01', 10, 100.0),
        (2, 'buy', '2022-02-02', 10, 120.0),
        (3, 'sell', '2022-03-03', 5, 65.0),
        (4, 'sell', '2022-04-04', 15, 220.0),
    ]
    columns = list(compute_trades_auto(trades, []))
    expected = [
        # id, posicao, preco_medio
        (1, 10, 10.0),
        (2, 20, 11.0),
        (3, 15, 11.0),
        (4, 0, 11.0),
    ]
    assert columns == expected


def test_posicao_vendida():
    trades = [
        # id, op, date, count, value
        (1, 'sell', '2022-01-01', 10, 100.0),
        (2, 'sell', '2022-02-02', 10, 120.0),
        (3, 'buy', '2022-03-03', 5, 40.0),
        (4, 'buy', '2022-04-04', 15, 100.0),
    ]
    columns = list(compute_trades_auto(trades, []))
    expected = [
        # id, posicao, preco_medio
        (1, -10, 10.0),
        (2, -20, 11.0),
        (3, -15, 11.0),
        (4, 0, 11.0),
    ]
    assert columns == expected


def test_posicao_com_splits():
    trades = [
        # id, op, date, count, value
        (1, 'buy', '2022-01-01', 10, 100.0),
        (2, 'buy', '2022-01-02', 10, 80.0),
        (3, 'buy', '2022-01-03', 20, 200.0),
        (4, 'sell', '2022-04-04', 15, 300.0),
    ]
    splits = [
        # date, ratio
        ('2022-01-02', 2.0),
        ('2022-02-02', 5.0),
        ('2022-03-03', 0.5),  # grupamento 2x
    ]
    columns = list(compute_trades_auto(trades, splits))
    expected = [
        # id, posicao, preco_medio
        (1, 10, 10.0),
        (2, 30, 6.0),
        (3, 50, 7.6),
        (4, 110, 3.04),
    ]
    assert columns == expected
