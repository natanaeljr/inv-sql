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


# Yield month range from begin_date.month to end_date.month
class MonthRange:
    def __init__(self, begin_date, end_date):
        self.begin_date = begin_date.replace(day=1)
        self.end_date = end_date.replace(day=calendar.monthrange(end_date.year, end_date.month)[1])

    def __iter__(self):
        first = self.begin_date
        while first < self.end_date:
            last = first.replace(day=calendar.monthrange(first.year, first.month)[1])
            yield first, last
            first += relativedelta(months=+1)


def execute_on_db():
    # retrieve oldest and newest trade entry in the DB
    oldest_date, = dbcursor.execute("SELECT min(date) FROM trades ORDER BY date").fetchall()[0]
    newest_date, = dbcursor.execute("SELECT max(date) FROM trades ORDER BY date").fetchall()[0]
    oldest_date = datetime.strptime(oldest_date, '%Y-%m-%d').date()
    newest_date = datetime.strptime(newest_date, '%Y-%m-%d').date()
    # loop through months in range from oldest to newest entry
    for first, last in MonthRange(oldest_date, newest_date):
        trades = dbcursor.execute("SELECT value FROM trades WHERE op = 'sell' AND date >= ? AND date <= ?",
                                  (first.strftime("%Y-%m-%d"), last.strftime("%Y-%m-%d"))).fetchall()
        print(trades)
        total = 0
        for value, in trades:
            total += value
        print(first, last, total)


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
