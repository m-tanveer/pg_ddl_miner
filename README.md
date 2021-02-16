# pg_ddl_miner
SQL to setup DDL logging in the PostgreSQL using Event Triggers

## caveats 
PostgreSQL Logical replication does not support DDL replication.So we can use the Events triggers to capture the DDL commands in the source and replicate it to the target replication instance. 
