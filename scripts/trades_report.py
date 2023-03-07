import sqlite3


# Define o número de ações compradas/vendidas no modo day-trade e no modo swing-trade,
# isto é, para cada operação em trades do mesmo símbolo.
# O resultado é operation id, day_count, swing_count
# na qual: day_count + swing_count = trades.count
#
# Método para day-trade:
# combinar o primeiro negócio de compra com o primeiro de venda,
# ou o primeiro negócio de venda com o primeiro de compra, sucessivamente.
def yield_trades_report(trades):
    trades_cache = []
    trades_report = {}
    # compute day-trade counts
    for id_, op, date_, count, value in trades:
        op_other = {'buy': 'sell', 'sell': 'buy'}[op]
        init_count = count
        # check for a previous counter-operation to liquidate day-trade
        for trade in filter(lambda x: x[2] == date_ and x[1] == op_other and x[3] > 0, trades_cache):
            trades_report.setdefault(trade[0], 0)
            if trade[3] >= count:
                trade[3] -= count
                trades_report[trade[0]] += count
                count = 0
                break
            else:
                count -= trade[3]
                trades_report[trade[0]] += trade[3]
                trade[3] = 0
        if count != init_count:
            trades_report[id_] = init_count - count
        trades_cache.append([id_, op, date_, count, init_count, value])
    # compute remaining swing-trade counts
    for trade in trades_cache:
        trades_report.setdefault(trade[0], 0)
        trades_report[trade[0]] = (trades_report[trade[0]], trade[4] - trades_report[trade[0]])
    # yield all day trades count
    trades_report = dict(sorted(trades_report.items()))
    for id_, (day, swing) in trades_report.items():
        yield id_, day, swing


def execute_on_db():
    trades_report = []
    symbols = dbcursor.execute("SELECT symbol, broker FROM trades GROUP BY symbol, broker").fetchall()
    for symbol, broker in symbols:
        all_symbol_trades = dbcursor.execute(
            "SELECT id, op, date, count, value FROM trades WHERE symbol = ? AND broker = ? ORDER BY date",
            (symbol, broker)).fetchall()
        trades_report += list(yield_trades_report(all_symbol_trades))
    dbcursor.execute("DELETE FROM trades_report WHERE 1")
    dbcursor.executemany("INSERT INTO trades_report VALUES (?, ?, ?)", trades_report)
    dbcon.commit()


###############################################################################
# Main
###############################################################################

if __name__ == "__main__":
    dbcon = sqlite3.connect('../db/inv.db')
    dbcursor = dbcon.cursor()
    execute_on_db()
    dbcon.close()


###############################################################################
# Tests
###############################################################################

def test_trades_report():
    trades = [
        # id, op, date, count, value
        # day-trade liquidado:
        (1, 'buy', '2022-01-01', 10, 100.0),
        (2, 'sell', '2022-01-01', 10, 120.0),
        # saiu comprado:
        (3, 'buy', '2022-01-02', 20, 100.0),
        (4, 'buy', '2022-01-02', 5, 100.0),
        (5, 'sell', '2022-01-02', 15, 120.0),
        # saiu vendido:
        (6, 'sell', '2022-01-03', 15, 180.0),
        (7, 'sell', '2022-01-03', 10, 120.0),
        (8, 'buy', '2022-01-03', 10, 100.0),
        (9, 'buy', '2022-01-03', 10, 110.0),
    ]
    columns = list(yield_trades_report(trades))
    expected = [
        (1, 10, 0),
        (2, 10, 0),
        (3, 15, 5),
        (4, 0, 5),
        (5, 15, 0),
        (6, 15, 0),
        (7, 5, 5),
        (8, 10, 0),
        (9, 10, 0),
    ]
    assert columns == expected
