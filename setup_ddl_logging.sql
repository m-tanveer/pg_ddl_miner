-- ==============================================================================
-- Author : Mohamed Tanveer (tanveer.munavar@gmail.com)
-- Description : script to setup the ddl logging in the databae
-- ==============================================================================

-- made improvements in the ddl logging required recreation of the objects.
DROP EVENT TRIGGER IF EXISTS log_ddl_start ;
DROP EVENT TRIGGER IF EXISTS log_ddl_end ;
DROP EVENT TRIGGER IF EXISTS log_ddl_drop;
DROP FUNCTION IF EXISTS log_ddl_drop();
DROP FUNCTION IF EXISTS log_ddl_start();
DROP FUNCTION IF EXISTS log_ddl_end();
DROP TABLE IF EXISTS public.log_ddl cascade;
DROP TABLE IF EXISTS public.log_ddl_publication_tables cascade;
DROP TABLE IF EXISTS admin.log_ddl cascade;
DROP TABLE IF EXISTS admin.log_ddl_publication_tables cascade;

-- we have made improvements in the ddl logging required recreation of the objects.
DROP SCHEMA IF EXISTS admin CASCADE ;

CREATE SCHEMA IF NOT EXISTS admin;

-- create ddl logging table
CREATE TABLE IF NOT EXISTS admin.log_ddl(
  id serial,
  tag text not null, -- create table , alter table like commands 
  event text not null, -- ddl_command_start, ddl_command_end
  object_type text default 'none', -- table , function
  schema_name text default 'none',
  object_name text default 'none',
  object_identity text default 'none', -- fully qualified object name
  pid bigint not null,
  txid bigint not null,
  client_addr text not null,
  username text not null,
  query text not null,
  time timestamp with time zone default now() not null,
  primary key (id)
);

-- 
-- ddl end logging function
-- 

CREATE OR REPLACE FUNCTION admin.log_ddl_end()
RETURNS event_trigger SECURITY DEFINER AS $$

DECLARE
  object record;
  pid bigint;
  txid bigint;
  username text;

BEGIN

  SELECT s.pid , txid_current(), usename INTO pid, txid , username from pg_stat_activity s where s.pid = pg_backend_pid();
  -- RAISE NOTICE 'pid : %, txid : %', pid ,txid ;

  FOR object IN SELECT * FROM pg_catalog.pg_event_trigger_ddl_commands()
  LOOP
    EXECUTE format ('INSERT INTO admin.log_ddl (tag,event,object_type,object_identity,schema_name, pid, txid, username, client_addr, time , query)
      VALUES (%L,%L,%L,%L,%L,%L,%L,%L,%L,%L,%L)' ,tg_tag, tg_event, object.object_type , object.object_identity, object.schema_name, pid, txid, username, inet_client_addr(), statement_timestamp(), current_query()) ;
    -- RAISE NOTICE 'Recorded execution of command % with event %', tg_tag, tg_event;

  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ddl drop logging function
--  this is only for user details logging for functions with security definer
CREATE OR REPLACE FUNCTION admin.log_ddl_drop()
RETURNS event_trigger SECURITY DEFINER AS $$

DECLARE
  object record;
  pid bigint;
  txid bigint;
  username text;

BEGIN

  SELECT s.pid , txid_current(), usename INTO pid, txid , username from pg_stat_activity s where s.pid = pg_backend_pid();

  FOR object IN SELECT * FROM pg_catalog.pg_event_trigger_dropped_objects()
  LOOP
    -- RAISE NOTICE 'Recorded execution of command % with event %', tg_tag, tg_event;
    EXECUTE format ('INSERT INTO admin.log_ddl (tag,event,object_type,object_identity,schema_name, pid, txid, username,object_name, client_addr, time, query )
      VALUES (%L,%L,%L,%L,%L,%L,%L,%L,%L,%L,%L,%L)' ,tg_tag, tg_event, object.object_type , object.object_identity, object.schema_name, pid, txid, user,object.object_name, inet_client_addr(), statement_timestamp(), current_query()) ;
  END LOOP;

END;
$$ LANGUAGE plpgsql;


--
-- event triggers
--

-- create event trigger for ddl end
CREATE EVENT TRIGGER log_ddl_end ON ddl_command_end EXECUTE PROCEDURE admin.log_ddl_end();

-- create event trigger for ddl drop
CREATE EVENT TRIGGER log_ddl_drop ON sql_drop EXECUTE PROCEDURE admin.log_ddl_drop();

-- trigger table to track ddl changes in the publication table (logical replication)
CREATE TABLE admin.log_ddl_publication_tables (
    id serial,
    query text,
    created timestamp with time zone default now(),
    primary key (id)
);

-- trigger function to insert the ddl queries
CREATE OR REPLACE FUNCTION admin.log_ddl_publication_tables()
RETURNS trigger
SECURITY DEFINER AS
$FUNCTION$
DECLARE
  _schema_name text ;
  _table_name text;
  _publication_exists int;
BEGIN
  -- string separator
  select split_part(NEW."object_identity",'.',1) into _schema_name;
  select split_part(NEW."object_identity",'.',2) into _table_name;
  -- check if the table is part of the publication
  select count(*) into _publication_exists from pg_publication_tables where schemaname = _schema_name and tablename = _table_name  ;
  -- if the alter table is related to the table in publication proceed
  IF (_publication_exists = 1) THEN
    INSERT INTO admin.log_ddl_publication_tables (query) values (NEW."query");
  ELSE
    RAISE NOTICE 'NOTICE : Table does not exist in the publication, wont be replicated in the logical replica';
  END IF;
  RETURN NULL;
END;
$FUNCTION$
language plpgsql;

-- trigger to call the ddl logging function
CREATE TRIGGER trg_log_ddl_publication_tables AFTER INSERT ON admin.log_ddl FOR EACH ROW  WHEN (NEW."tag" = 'ALTER TABLE' and NEW."event" = 'ddl_command_end') EXECUTE FUNCTION admin.log_ddl_publication_tables() ;


-- end of script

