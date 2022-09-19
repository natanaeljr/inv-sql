import sqlite3
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tarifas import tarifas_b3, tarifas_corretora


class YieldMonthReport:
    def __init__(self, broker, symbol, trades, splits):
        self.broker = broker
        self.symbol = symbol
        self.trades = trades
        self.splits = splits
        self.posicao = 0  # posição acumulada de swing-trade
        self.preco_medio = 0  # preço médio sem custos
        self.custo_medio = 0  # preço médio com custos
        self.preco_total = 0  # preço acumulado de aquisição da posição sem custos
        self.custo_total = 0  # preço acumulado de aquisição da posição com custos

    def __iter__(self):
        for id_, op, date_str, count, value, day_count in self.trades:
            swing_count = count - day_count
            if swing_count == 0:  # skip day-trades
                continue
            date_ = datetime.strptime(date_str, '%Y-%m-%d').date()
            self.__adjust_values_for_splits(date_)
            result = self.__add_swing_trade(date_, op, value, swing_count)
            custo_b3, custo_broker, ganho_preco, ganho_custo = result
            yield id_, self.posicao, self.preco_medio, self.preco_total, \
                  custo_b3, custo_broker, self.custo_medio, self.custo_total, \
                  ganho_preco, ganho_custo

    def __adjust_values_for_splits(self, date_):
        for split_date_str, ratio in self.splits[:]:  # splits is ordered by date
            split_date = datetime.strptime(split_date_str, '%Y-%m-%d').date()
            if split_date > date_:  # remaining splits are in the future
                break
            self.posicao *= ratio
            self.preco_medio /= ratio
            self.custo_medio /= ratio
            self.splits.remove((split_date_str, ratio))

    def __add_swing_trade(self, date_, op, value, swing_count):
        custo_b3 = tarifas_b3(date_) * value
        custo_broker = tarifas_corretora(self.broker, date_)
        ganho_preco = ganho_custo = None
        # update accumulated values based on operation
        if op == 'buy':
            if self.posicao >= 0:  # acréscimo de posição comprada
                self.posicao += swing_count
                self.preco_total += value
                self.custo_total += value + custo_b3 + custo_broker
                self.preco_medio = self.preco_total / self.posicao
                self.custo_medio = self.custo_total / self.posicao
            else:  # liquidação de posição vendida
                self.posicao += swing_count
                self.preco_medio = self.preco_medio
                self.custo_medio = self.custo_medio
                self.preco_total = self.posicao * self.preco_medio
                self.custo_total = self.posicao * self.custo_medio
                ganho_preco = (swing_count * self.preco_medio) - value
                ganho_custo = (swing_count * self.custo_medio) - value + custo_b3 + custo_broker
        elif op == 'sell':
            if self.posicao <= 0:  # acréscimo de posição vendida
                self.posicao -= swing_count
                self.preco_total -= value
                self.custo_total -= value - custo_b3 - custo_broker
                self.preco_medio = self.preco_total / self.posicao
                self.custo_medio = self.custo_total / self.posicao
            else:  # liquidação de posição comprada
                self.posicao -= swing_count
                self.preco_medio = self.preco_medio
                self.custo_medio = self.custo_medio
                self.preco_total = self.posicao * self.preco_medio
                self.custo_total = self.posicao * self.custo_medio
                ganho_preco = value - (swing_count * self.preco_medio)
                ganho_custo = value - (swing_count * self.custo_medio) - custo_b3 - custo_broker
        else:
            raise Exception(f"Unknown OP {op}")
        # calcula custos
        return custo_b3, custo_broker, ganho_preco, ganho_custo


def execute_on_db():
    (first_date,) = dbcursor.execute("SELECT min(date) FROM trades ORDER BY date").fetchall()[0]
    (last_date,) = dbcursor.execute("SELECT max(date) FROM trades ORDER BY date").fetchall()[0]

    date_begin = datetime.strptime(first_date, '%Y-%m-%d').date().replace(day=1)
    date_end = date_begin.replace(day=calendar.monthrange(date_begin.year, date_begin.month)[1])
    entries = dbcursor.execute("SELECT * FROM trades WHERE op = 'sell' AND date >= ? AND date <= ?",
                               (date_begin.strftime("%Y-%m-%d"), date_end.strftime("%Y-%m-%d"))).fetchall()
    for entry in entries:
        print(entry)


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
        # id, op, date, count, value, day_count
        (1, 'buy', '2020-01-01', 10, 100.0, 0),
        (2, 'buy', '2021-02-02', 15, 120.0, 5),
        (3, 'sell', '2022-03-03', 5, 65.0, 0),
        (4, 'sell', '2022-04-04', 20, 220.0, 5),
        (5, 'sell', '2022-04-04', 10, 100.0, 10),  # day-trade
    ]
    columns = list(YieldTradesAuto('rico', 'PETR4', trades, []))
    expected = [
        # id, posicao, preco_medio, preco_total, custo_b3, custo_broker, custo_medio, custo_total, ganho_preco, ganho_custo
        (1, 10, 10.0, 100.0, 0.030506000000000002, 8.3000000025, 10.83305060025, 108.3305060025, None, None),
        (2, 20, 11.0, 220.0, 0.036000000000000004, 0.0, 11.418325300125, 228.3665060025, None, None),
        (3, 15, 11.0, 165.0, 0.019500000000000003, 0.0, 11.418325300125, 171.274879501875, 10.0, 7.888873499375009),
        (4, 0, 11.0, 0.0, 0.066, 0.0, 11.418325300125, 0.0, 55.0, 48.65912049812499),
    ]
    assert columns == expected


def test_posicao_vendida():
    trades = [
        # id, op, date, count, value
        (1, 'sell', '2020-01-01', 10, 100.0, 0),
        (2, 'sell', '2021-02-02', 15, 120.0, 5),
        (3, 'buy', '2022-03-03', 5, 40.0, 0),
        (4, 'buy', '2022-04-04', 20, 100.0, 5),
        (5, 'buy', '2022-04-04', 10, 100.0, 10),  # day-trade
    ]
    columns = list(YieldTradesAuto('rico', 'PETR4', trades, []))
    expected = [
        # id, posicao, preco_medio, custo_b3, custo_broker
        (1, -10, 10.0, 0.030506000000000002, 8.3000000025),
        (2, -20, 11.0, 0.036000000000000004, 0.0),
        (3, -15, 11.0, 0.012, 0.0),
        (4, 0, 11.0, 0.030000000000000002, 0.0)
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
    columns = list(YieldTradesAuto('rico', 'PETR4', trades, splits))
    expected = [
        # id, posicao, preco_medio
        (1, 10, 10.0, 0.030000000000000002, 0.0),
        (2, 30, 6.0, 0.024, 0.0),
        (3, 50, 7.6, 0.060000000000000005, 0.0),
        (4, 110, 3.04, 0.09000000000000001, 0.0)
    ]
    assert columns == expected
