import sqlite3

con = sqlite3.connect('inv.db')
cur = con.cursor()

output = []
list = cur.execute("SELECT symbol FROM trades GROUP BY symbol").fetchall()
for symbol in list:
    pos = medio = total = 0
    out = cur.execute("SELECT id, symbol, op, count, value FROM trades WHERE symbol = '%s'" % symbol).fetchall()
    for id, symbol, op, count, value in out:
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

        output.append((id, symbol, op, count, pos, value, total, price, medio))

eat = []
for o in output:
    eat.append((o[0], o[4], o[8]))

cur.execute("DELETE FROM trades_auto WHERE 1")
cur.executemany("INSERT INTO trades_auto VALUES (?, ?, ?)", eat)

con.commit()
con.close()
