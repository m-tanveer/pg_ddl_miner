# pg_ddl_miner
SQL to setup DDL logging in the PostgreSQL using Event Triggers

Caveats :

* Event triggers does not support to capture the DDL statement. Hence these scripts do not capture the whole SQL command executed. Use log_statement=ddl to log the DDL commands in the PostgreSQL log file.Through the log_ddl table you could easy trace the log file for the SQL executed.
