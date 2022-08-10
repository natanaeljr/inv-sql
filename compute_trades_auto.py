import sqlite3


# Calcula:
# - posição (número de ações atual),
# - preço médio (sem custos)
# - valor_total (sem custos)
# por ativo e por corretora
# resultado é salvo na tabela trades_auto (id, posicao, preco_medio)

def compute_trades_auto(symbol_trades):
    posicao = preco_medio = valor_total = 0
    for _id, op, count, value in symbol_trades:
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
            "SELECT id, op, count, value FROM trades WHERE symbol = ? AND broker = ? ORDER BY date",
            (symbol, broker)).fetchall()
        trades_auto += compute_trades_auto(all_symbol_trades)
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
    data = [
        (1, 'buy', 10, 100.0),
        (2, 'buy', 10, 120.0),
        (3, 'sell', 5, 65.0),
        (4, 'sell', 15, 220.0),
    ]
    columns = list(compute_trades_auto(data))
    expected = [
        (1, 10, 10.0),
        (2, 20, 11.0),
        (3, 15, 11.0),
        (4, 0, 11.0),
    ]
    assert columns == expected


def test_posicao_vendida():
    data = [
        (1, 'sell', 10, 100.0),
        (2, 'sell', 10, 120.0),
        (3, 'buy', 5, 40.0),
        (4, 'buy', 15, 100.0),
    ]
    columns = list(compute_trades_auto(data))
    expected = [
        (1, -10, 10.0),
        (2, -20, 11.0),
        (3, -15, 11.0),
        (4, 0, 11.0),
    ]
    assert columns == expected
