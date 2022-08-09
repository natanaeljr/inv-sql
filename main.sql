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
