-- Coldstone warehouse, Kenya: database permissions for the Glue load job. Run once in DBeaver on the coldstone_web
-- connection, after 01_tables_ky.sql, as a user allowed to grant on these schemas (the schema owner or an admin).
--
-- The load job connects through the Redshift Data API with its IAM role, and Redshift Serverless maps that role
-- to a database user named "IAMR:<role name>". If the Glue job's role is not called coldstone-glue-ky-role,
-- replace every coldstone-glue-ky-role below with its exact name (case matters).
--
-- What the job needs:
--   coldstone_web_ke_staging   - create, load and drop its own tables there every run
--   coldstone_web_ke_analytics - read (to copy column types), insert, update and delete rows; it never changes the tables

CREATE USER "IAMR:coldstone-glue-ky-role" PASSWORD DISABLE;

GRANT USAGE, CREATE ON SCHEMA coldstone_web_ke_staging TO "IAMR:coldstone-glue-ky-role";
GRANT USAGE ON SCHEMA coldstone_web_ke_analytics TO "IAMR:coldstone-glue-ky-role";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA coldstone_web_ke_analytics TO "IAMR:coldstone-glue-ky-role";

-- Check: should list the role with the four privileges on each of the 24 tables
-- SELECT relation_name, privilege_type FROM svv_relation_privileges
-- WHERE namespace_name = 'coldstone_web_ke_analytics' AND identity_name = 'IAMR:coldstone-glue-ky-role' ORDER BY 1, 2;
