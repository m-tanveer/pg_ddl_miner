-- ==============================================================================
-- Author : Mohamed Tanveer (mohamedtanveer007@gmail.com)
-- Description : script to setup the ddl logging in the databae using event triggers
-- Date : 21-APR-2020
-- ==============================================================================


-- create ddl logging table
CREATE TABLE log_ddl(
  id serial,
  tag text not null, -- create table , alter table
  event text not null, -- ddl_command_start, ddl_command_end
  object_type text default 'none', -- table , function
  schema_name text default 'none',
  object_name text default 'none',
  object_identity text default 'none', -- fully qualified object name
  pid bigint not null,
  txid bigint not null,
  client_addr text not null,
  username text not null,
  time timestamp with time zone default now() not null,
  primary key (id)
);

-- ddl start logging function
CREATE OR REPLACE FUNCTION log_ddl_start()
RETURNS event_trigger SECURITY DEFINER AS $$
DECLARE
  pid bigint;
  txid bigint;
  username text;

BEGIN

  SELECT s.pid , txid_current(), usename INTO pid, txid , username from pg_stat_activity s where s.pid = pg_backend_pid();
  -- RAISE NOTICE 'pid : %, txid : %', pid ,txid ;

  EXECUTE format ('INSERT INTO log_ddl (tag,event, pid, txid, username, client_addr, time )
    VALUES (%L,%L,%L,%L,%L,%L,%L)' ,tg_tag, tg_event, pid, txid, username, inet_client_addr(), statement_timestamp()) ;
  -- RAISE NOTICE 'Recorded execution of command % with event %', tg_tag, tg_event;

END;
$$ LANGUAGE plpgsql;


-- ddl end logging function
CREATE OR REPLACE FUNCTION log_ddl_end()
RETURNS event_trigger SECURITY DEFINER AS $$

DECLARE
  object record;
  pid bigint;
  txid bigint;
  username text;

BEGIN

  SELECT s.pid , txid_current(), usename INTO pid, txid , username from pg_stat_activity s where s.pid = pg_backend_pid();
  -- RAISE NOTICE 'pid : %, txid : %', pid ,txid ;

  FOR object IN SELECT * FROM pg_event_trigger_ddl_commands()
  LOOP
    EXECUTE format ('INSERT INTO log_ddl (tag,event,object_type,object_identity,schema_name, pid, txid, username, client_addr, time )
      VALUES (%L,%L,%L,%L,%L,%L,%L,%L,%L,%L)' ,tg_tag, tg_event, object.object_type , object.object_identity, object.schema_name, pid, txid, username, inet_client_addr(), statement_timestamp()) ;
    -- RAISE NOTICE 'Recorded execution of command % with event %', tg_tag, tg_event;

  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ddl drop logging function
CREATE OR REPLACE FUNCTION log_ddl_drop()
RETURNS event_trigger SECURITY DEFINER AS $$

DECLARE
  object record;
  pid bigint;
  txid bigint;
  username text;

BEGIN

  SELECT s.pid , txid_current(), usename INTO pid, txid , username from pg_stat_activity s where s.pid = pg_backend_pid();

  FOR object IN SELECT * FROM pg_event_trigger_dropped_objects()
  LOOP
    -- RAISE NOTICE 'Recorded execution of command % with event %', tg_tag, tg_event;
    EXECUTE format ('INSERT INTO log_ddl (tag,event,object_type,object_identity,schema_name, pid, txid, username,object_name, client_addr, time )
      VALUES (%L,%L,%L,%L,%L,%L,%L,%L,%L,%L,%L)' ,tg_tag, tg_event, object.object_type , object.object_identity, object.schema_name, pid, txid, user,object.object_name, inet_client_addr(), statement_timestamp()) ;
  END LOOP;

END;
$$ LANGUAGE plpgsql;


--
-- event triggers
--

-- create event trigger for ddl start
CREATE EVENT TRIGGER log_ddl_start ON ddl_command_start EXECUTE PROCEDURE log_ddl_start();

-- create event trigger for ddl end
CREATE EVENT TRIGGER log_ddl_end ON ddl_command_end EXECUTE PROCEDURE log_ddl_end();

-- create event trigger for ddl drop
CREATE EVENT TRIGGER log_ddl_drop ON sql_drop EXECUTE PROCEDURE log_ddl_drop();

-- end of script
