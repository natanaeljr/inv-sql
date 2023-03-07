INV-SQL
===

INV SQL tool for keeping track of investments & trades in the stock market.

Repository contains the DB (only referece), SQL snippets gathered during development and testing, and python scripts that
generate analysis data in the form of other DB tables.

## DB Tables:

#### User tables

- **trades**  
    Table to contain all trade operations of buy/sell, day/swing, with date, count and value, and the broker.  
    To be filled manually or via scripts with data from B3 investidor portal.
- **splits**  
    Table to contain splits records for interesting tickets (only those that have been traded).  
    To be filled manually via analysis of stocks that have split records. (See in yahoo finances).
- **symbols**  
    Table to contain info data for each stock symbol (cnpj, name, class: BDR/ETF/FII/AÇÃO).

#### Generated tables
- **trades_auto**  
    Table to contain generated analysis data over _trades_ table.  
    Generated data is referenced to each row in _trades_ giving the posição_atual, preço_medio, preço_total, custos
    and gains for each operation record.
- **trades_report**  
    Table to contain count of stocks trades separated _day_ or _swing_ type for each operation record.  
    Used for calculating closed positions and taxes at the moment of each trade operation.
- **month_report**  
    Table to contain monthly reports of swing trades counts by type of stock, total gains and final tax (IR) values.  
    Should be consulted in order know how much should be paid in taxes each month.

#### Backup tables
- **carteira_bkp**: Table containing all rows from previously manually managed INV spreadsheet.  
    Left as is as for backup only and should not be modified.
- **tradesBkp**: Copied from _trades_ table a given time, for testing.  
    Its meaning was lost. To be deleted in the future if not used anymore.
    