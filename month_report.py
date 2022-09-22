import sqlite3
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta


def month_report(begin_date, end_date, trades, symbols):
    swing_sells_total = 0
    swing_sells_bdr = 0
    swing_sells_etf = 0
    swing_sells_fii = 0
    for symbol, date, count, value in trades:
        swing_sells_total += value
        class_ = symbols[symbol]
        if class_ == 'BDR':
            swing_sells_bdr += value
        elif class_ == 'ETF':
            swing_sells_etf += value
        elif class_ == 'FII':
            swing_sells_fii += value
        elif class_ != 'AÇÃO':
            raise Exception(f"Unknown symbol {symbol} class {class_}")

    return begin_date.strftime("%Y-%m-%d"), end_date.strftime(
        "%Y-%m-%d"), swing_sells_total, swing_sells_bdr, swing_sells_etf, swing_sells_fii


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
    symbols = dbcursor.execute("SELECT code, class FROM symbols ORDER BY code").fetchall()
    # retrieve oldest and newest trade entry in the DB
    oldest_date, = dbcursor.execute("SELECT min(date) FROM trades ORDER BY date").fetchall()[0]
    newest_date, = dbcursor.execute("SELECT max(date) FROM trades ORDER BY date").fetchall()[0]
    oldest_date = datetime.strptime(oldest_date, '%Y-%m-%d').date()
    newest_date = datetime.strptime(newest_date, '%Y-%m-%d').date()
    # loop through months in range from oldest to newest entry
    report = []
    for first, last in MonthRange(oldest_date, newest_date):
        trades = dbcursor.execute(
            "SELECT symbol, date, count, value FROM trades WHERE op = 'sell' AND date >= ? AND date <= ?",
            (first.strftime("%Y-%m-%d"), last.strftime("%Y-%m-%d"))).fetchall()
        report.append(month_report(first, last, trades, dict(symbols)))
    dbcursor.execute("DELETE FROM month_report WHERE 1")
    dbcursor.executemany(
        "INSERT INTO month_report(date_begin, date_end,"
        "swing_sells_total, swing_sells_bdr, swing_sells_etf, swing_sells_fii)"
        "VALUES (?, ?, ?, ?, ?, ?)",
        report)
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
