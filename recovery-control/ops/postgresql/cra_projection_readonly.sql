\set ON_ERROR_STOP on

DO $role$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stream_monitoring_cra_projection_ro') THEN
        CREATE ROLE stream_monitoring_cra_projection_ro LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
END
$role$;

ALTER ROLE stream_monitoring_cra_projection_ro LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
ALTER ROLE stream_monitoring_cra_projection_ro SET default_transaction_read_only = on;
ALTER ROLE stream_monitoring_cra_projection_ro SET statement_timeout = '2s';
ALTER ROLE stream_monitoring_cra_projection_ro SET lock_timeout = '1s';
REVOKE CREATE, TEMPORARY ON DATABASE stream_monitoring_v4 FROM stream_monitoring_cra_projection_ro;
GRANT CONNECT ON DATABASE stream_monitoring_v4 TO stream_monitoring_cra_projection_ro;
REVOKE CREATE ON SCHEMA public FROM stream_monitoring_cra_projection_ro;
GRANT USAGE ON SCHEMA public TO stream_monitoring_cra_projection_ro;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM stream_monitoring_cra_projection_ro;
GRANT SELECT ON TABLE
    public.shadow_cycles,
    public.domain_current,
    public.incident_episodes,
    public.incident_candidates,
    public.observations,
    public.component_health
TO stream_monitoring_cra_projection_ro;

DO $verify$
DECLARE
    table_name text;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_auth_members membership
        JOIN pg_roles role ON role.oid = membership.member
        WHERE role.rolname = 'stream_monitoring_cra_projection_ro'
    ) THEN
        RAISE EXCEPTION 'CRA projection role must not belong to another role';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_roles
        WHERE rolname = 'stream_monitoring_cra_projection_ro'
          AND (
              rolsuper OR rolcreaterole OR rolcreatedb OR rolinherit
              OR rolreplication OR rolbypassrls OR NOT rolcanlogin
          )
    ) THEN
        RAISE EXCEPTION 'CRA projection role has unsafe role attributes';
    END IF;
    IF has_database_privilege(
        'stream_monitoring_cra_projection_ro',
        'stream_monitoring_v4',
        'CREATE'
    ) OR has_schema_privilege(
        'stream_monitoring_cra_projection_ro',
        'public',
        'CREATE'
    ) THEN
        RAISE EXCEPTION 'CRA projection role can create persistent database objects';
    END IF;
    FOREACH table_name IN ARRAY ARRAY[
        'shadow_cycles',
        'domain_current',
        'incident_episodes',
        'incident_candidates',
        'observations',
        'component_health'
    ]
    LOOP
        IF has_table_privilege('stream_monitoring_cra_projection_ro', format('public.%I', table_name), 'INSERT')
           OR has_table_privilege('stream_monitoring_cra_projection_ro', format('public.%I', table_name), 'UPDATE')
           OR has_table_privilege('stream_monitoring_cra_projection_ro', format('public.%I', table_name), 'DELETE')
           OR has_table_privilege('stream_monitoring_cra_projection_ro', format('public.%I', table_name), 'TRUNCATE') THEN
            RAISE EXCEPTION 'CRA projection role has mutation privilege on %', table_name;
        END IF;
        IF NOT has_table_privilege(
            'stream_monitoring_cra_projection_ro',
            format('public.%I', table_name),
            'SELECT'
        ) THEN
            RAISE EXCEPTION 'CRA projection role lacks SELECT on %', table_name;
        END IF;
    END LOOP;
END
$verify$;
