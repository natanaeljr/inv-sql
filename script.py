import sqlite3
from tarifas import *
from datetime import *


# TODO:
#  liquidez + acréscimo em 1 operação
#  calculo de custo com taxas (tabela de tarifas, tabela de tipo de ação)
#  update trades_auto ao invés de delete e insert all
#  support manual overwrite (skip when column manual = Y)
#  consolidate position algo (gera tabela de posições atuais de ativos em custódia [considerar splits])


# Define o número de ações compradas/vendidas no modo day-trade e no modo swing-trade,
# isto é, para cada operação em trades do mesmo símbolo.
# O resultado é operation id, day_trade_count, swing_trade_count
# na qual: day_trade_count + swing_trade_count = trades.count
#
# Método para day-trade:
# combinar o primeiro negócio de compra com o primeiro de venda,
# ou o primeiro negócio de venda com o primeiro de compra, sucessivamente.
def compute_trades_report(trades):
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


# Calcula para cada símbolo:
#  posição (número de ações atual),
#  preço médio (sem custos)
#  valor total (sem custos)
# por ativo e por corretora
# resultado é salvo na tabela trades_auto (id, posicao, preco_medio)
# splits (desdobramento e grupamentos) são levados em consideração
# param trades: list of tuples of trade info ordered by date
# param splits: list of tuples with split events for this symbol ordered by date
class YieldTradesAuto:
    def __init__(self, broker, symbol, trades, splits):
        self.broker = broker
        self.symbol = symbol
        self.trades = trades
        self.splits = splits
        self.posicao = 0  # posição acumulada de swing-trade
        self.preco_medio = 0  # preço médio sem custos
        self.valor_total = 0  # valor acumulado de aquisição da posição
        self.custo_total = 0  # custos acumulados das operações

    def __iter__(self):
        for id_, op, date_str, count, value, day_count in self.trades:
            swing_count = count - day_count
            if swing_count == 0:  # skip day-trades
                continue
            date_ = datetime.strptime(date_str, '%Y-%m-%d').date()
            self.__adjust_values_for_splits(date_)
            self.__add_swing_trade(date_, op, value, swing_count)
            yield id_, self.posicao, self.preco_medio

    def __adjust_values_for_splits(self, date_):
        for split_date_str, ratio in self.splits[:]:  # splits is ordered by date
            split_date = datetime.strptime(split_date_str, '%Y-%m-%d').date()
            if split_date > date_:  # remaining splits are in the future
                break
            self.posicao *= ratio
            self.preco_medio /= ratio
            self.splits.remove((split_date_str, ratio))

    def __add_swing_trade(self, date_, op, value, swing_count):
        # update accumulated values based on operation
        if op == 'buy':
            if self.posicao >= 0:  # acréscimo de posição comprada
                self.posicao += swing_count
                self.valor_total += value
                self.preco_medio = self.valor_total / self.posicao
                self.custo_total += self.custos_swing_trade(date_, value)
            else:  # liquidação de posição vendida
                self.posicao += swing_count
                self.preco_medio = self.preco_medio
                self.valor_total = self.posicao * self.preco_medio
                self.custo_total += 0
        elif op == 'sell':
            if self.posicao <= 0:  # acréscimo de posição vendida
                self.posicao -= swing_count
                self.valor_total -= value
                self.preco_medio = self.valor_total / self.posicao
                self.custo_total += 0
            else:  # liquidação de posição comprada
                self.posicao -= swing_count
                self.preco_medio = self.preco_medio
                self.valor_total = self.posicao * self.preco_medio
                self.custo_total += 0
        else:
            raise Exception(f"Unknown OP {op}")

    def custos_swing_trade(self, date_, value):
        return value * tarifas_b3(date_) + tarifas_corretora(self.broker, date_)


def execute_on_db():
    trades_auto = []
    symbols = dbcursor.execute("SELECT symbol, broker FROM trades GROUP BY symbol, broker").fetchall()
    for symbol, broker in symbols:
        all_symbol_trades = dbcursor.execute(
            "SELECT trades.id, op, date, count, value, day_trade_count FROM trades " +
            "LEFT JOIN trades_report ON trades.id = trades_report.id " +
            "WHERE symbol = ? AND broker = ? ORDER BY date",
            (symbol, broker)).fetchall()
        all_symbol_splits = dbcursor.execute(
            "SELECT date, ratio FROM splits WHERE symbol = ? ORDER BY date",
            (symbol,)).fetchall()
        trades_auto += YieldTradesAuto(broker, symbol, all_symbol_trades, all_symbol_splits)
    dbcursor.execute("DELETE FROM trades_auto WHERE 1")
    dbcursor.executemany("INSERT INTO trades_auto VALUES (?, ?, ?)", trades_auto)
    dbcon.commit()


def execute_on_db2():
    trades_report = []
    symbols = dbcursor.execute("SELECT symbol, broker FROM trades GROUP BY symbol, broker").fetchall()
    for symbol, broker in symbols:
        all_symbol_trades = dbcursor.execute(
            "SELECT id, op, date, count, value FROM trades WHERE symbol = ? AND broker = ? ORDER BY date",
            (symbol, broker)).fetchall()
        trades_report += compute_trades_report(all_symbol_trades)
    dbcursor.execute("DELETE FROM trades_report WHERE 1")
    dbcursor.executemany("INSERT INTO trades_report VALUES (?, ?, ?)", trades_report)
    dbcon.commit()


###############################################################################
# Main
###############################################################################

if __name__ == "__main__":
    dbcon = sqlite3.connect('db/inv.db')
    dbcursor = dbcon.cursor()
    execute_on_db()
    execute_on_db2()
    dbcon.close()


###############################################################################
# Tests
###############################################################################

def test_posicao_comprada():
    trades = [
        # id, op, date, count, value, day_trade_count
        (1, 'buy', '2022-01-01', 10, 100.0, 0),
        (2, 'buy', '2022-02-02', 15, 120.0, 5),
        (3, 'sell', '2022-03-03', 5, 65.0, 0),
        (4, 'sell', '2022-04-04', 20, 220.0, 5),
        (5, 'sell', '2022-04-04', 10, 100.0, 10),  # day-trade
    ]
    columns = list(YieldTradesAuto(trades, []))
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
        (1, 'sell', '2022-01-01', 10, 100.0, 0),
        (2, 'sell', '2022-02-02', 15, 120.0, 5),
        (3, 'buy', '2022-03-03', 5, 40.0, 0),
        (4, 'buy', '2022-04-04', 20, 100.0, 5),
        (5, 'buy', '2022-04-04', 10, 100.0, 10),  # day-trade
    ]
    columns = list(YieldTradesAuto(trades, []))
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
        (1, 'buy', '2022-01-01', 10, 100.0, 0),
        (2, 'buy', '2022-01-02', 10, 80.0, 0),
        (3, 'buy', '2022-01-03', 20, 200.0, 0),
        (4, 'sell', '2022-04-04', 25, 300.0, 10),
    ]
    splits = [
        # date, ratio
        ('2022-01-02', 2.0),
        ('2022-02-02', 5.0),
        ('2022-03-03', 0.5),  # grupamento 2x
    ]
    columns = list(YieldTradesAuto(trades, splits))
    expected = [
        # id, posicao, preco_medio
        (1, 10, 10.0),
        (2, 30, 6.0),
        (3, 50, 7.6),
        (4, 110, 3.04),
    ]
    assert columns == expected


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
    columns = list(compute_trades_report(trades))
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
