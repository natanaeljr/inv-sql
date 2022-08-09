import sqlite3


def compute_trades_auto():
    trades_auto = []
    symbols = dbcursor.execute("SELECT symbol FROM trades GROUP BY symbol").fetchall()
    for symbol, in symbols:
        symbol_trades = dbcursor.execute("SELECT id, op, count, value FROM trades WHERE symbol = ? ORDER BY date",
                                         (symbol,)).fetchall()
        pos = medio = total = 0
        for _id, op, count, value in symbol_trades:
            price = value / count
            if op == "C":
                pos += count
                total += value
                medio = total / pos if pos > 0 else price
            elif op == "V":
                pos -= count
                total = pos * medio
                medio = medio
            else:
                raise Exception(f"Unknown OP {op}")
            trades_auto.append((_id, pos, medio))

    dbcursor.execute("DELETE FROM trades_auto WHERE 1")
    dbcursor.executemany("INSERT INTO trades_auto VALUES (?, ?, ?)", trades_auto)
    dbcon.commit()


if __name__ == "__main__":
    dbcon = sqlite3.connect('inv.db')
    dbcursor = dbcon.cursor()
    compute_trades_auto()
    dbcon.close()
