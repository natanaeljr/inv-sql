insert into trades(symbol, broker, date) values ('PETR5', 'rico', '2022-08-08');

select * from trades inner join (select id as maxId, max(date)
                           from trades) on id = maxId;

select *, (value/count) as price from trades;

CREATE VIEW spreadsheet as
select *, (value/count) as price from trades;

select symbol, name, type from trades left join symbols on symbol = symbols.code group by symbol;
alter table trades add column kind text check (kind = 'swing' OR kind = 'day');
-- UPDATE trades SET kind = 'swing' WHERE kind = 'day';

insert into tradesBkp select * from trades;

-- CARTEIRA_BKP TO TRADES:
insert into trades (broker, symbol, op, date, count, value) select corretora, ativo, op, data, qtde, valor  from carteira_bkp where op = 'C' or op = 'V';
update trades set id = (id - 802) where id > 802;

-- select within 1 month
select * from trades where date >= date('2022-04-01') AND date <= date('2022-04-01','start of month','+1 month','-1 day');

-- start of month
SELECT date('now','start of month');
-- end of month
SELECT date('now','start of month','+1 month','-1 day');

-- get all symbols
select symbol from trades group by symbol;

-- all buys from itau
select symbol, op, date, sum(count) as count, sum(value) as value, sum(value)/sum(count) as price from trades where broker = 'itau' and op = 'C' group by symbol order by date;

-- case/when example
select *, (case when op = 'C' then 'compra' when op = 'V' then 'venda' else 'unknown' end) as operation from trades;

-- preco medio
select trades.id, symbol, op, date, count, value/count as preco, value, posicao, preco_medio, posicao * preco_medio as total
    from trades join trades_auto on trades.id = trades_auto.id where symbol='TSLA34' and broker = 'rico';

-- 'C', 'V' to buy and sell
update trades set op = 'buy' where op = 'C';
update trades set op = 'sell' where op = 'V';
select op from trades where op <> 'buy' and op <> 'sell' group by op