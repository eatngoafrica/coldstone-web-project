-- Coldstone warehouse, Nigeria: database permissions for the Lambda load (coldstone-load-ng), which replaces the Glue
-- load job. Run once in DBeaver on the coldstone_web connection, as a user allowed to grant on these schemas (the
-- schema owner or an admin), BEFORE the first Lambda load. Safe alongside 02_grants_ng.sql: the Glue job's user keeps
-- its grants until you drop it.
--
-- The load connects through the Redshift Data API with its IAM role, and Redshift Serverless maps that role to a
-- database user named "IAMR:<role name>". template.yaml names the role coldstone-load-ng-lambda-role.
--
-- What the load needs:
--   coldstone_web_ng_staging   - create, load and drop its own tables there every run
--   coldstone_web_ng_analytics - read (to copy column types), insert, update and delete rows; it never changes the tables

CREATE USER "IAMR:coldstone-load-ng-lambda-role" PASSWORD DISABLE;

GRANT USAGE, CREATE ON SCHEMA coldstone_web_ng_staging TO "IAMR:coldstone-load-ng-lambda-role";
GRANT USAGE ON SCHEMA coldstone_web_ng_analytics TO "IAMR:coldstone-load-ng-lambda-role";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA coldstone_web_ng_analytics TO "IAMR:coldstone-load-ng-lambda-role";

-- Staging is recreated by every load, and only a table's owner can drop it. The Glue job's leftover staging
-- tables belong to its user, so drop them once here (they hold nothing needed; analytics is untouched):
DROP TABLE IF EXISTS coldstone_web_ng_staging."branches";
DROP TABLE IF EXISTS coldstone_web_ng_staging."roles";
DROP TABLE IF EXISTS coldstone_web_ng_staging."permissions";
DROP TABLE IF EXISTS coldstone_web_ng_staging."sub_permissions";
DROP TABLE IF EXISTS coldstone_web_ng_staging."members";
DROP TABLE IF EXISTS coldstone_web_ng_staging."menu_categories";
DROP TABLE IF EXISTS coldstone_web_ng_staging."menu_items";
DROP TABLE IF EXISTS coldstone_web_ng_staging."menu_modifiers";
DROP TABLE IF EXISTS coldstone_web_ng_staging."discounts";
DROP TABLE IF EXISTS coldstone_web_ng_staging."wallets";
DROP TABLE IF EXISTS coldstone_web_ng_staging."customers";
DROP TABLE IF EXISTS coldstone_web_ng_staging."orders";
DROP TABLE IF EXISTS coldstone_web_ng_staging."activity_logs";
DROP TABLE IF EXISTS coldstone_web_ng_staging."wallet_transactions";
DROP TABLE IF EXISTS coldstone_web_ng_staging."order_items";
DROP TABLE IF EXISTS coldstone_web_ng_staging."order_item_modifiers";
DROP TABLE IF EXISTS coldstone_web_ng_staging."order_timeline";
DROP TABLE IF EXISTS coldstone_web_ng_staging."activity_initiators";
DROP TABLE IF EXISTS coldstone_web_ng_staging."branch_members";
DROP TABLE IF EXISTS coldstone_web_ng_staging."member_additional_branches";
DROP TABLE IF EXISTS coldstone_web_ng_staging."member_roles";
DROP TABLE IF EXISTS coldstone_web_ng_staging."modifier_items";
DROP TABLE IF EXISTS coldstone_web_ng_staging."discount_applicable_items";
DROP TABLE IF EXISTS coldstone_web_ng_staging."discount_combo_items";
DROP TABLE IF EXISTS coldstone_web_ng_staging."_ids_activity_logs";
DROP TABLE IF EXISTS coldstone_web_ng_staging."_ids_discounts";
DROP TABLE IF EXISTS coldstone_web_ng_staging."_ids_members";
DROP TABLE IF EXISTS coldstone_web_ng_staging."_ids_menu_modifiers";
DROP TABLE IF EXISTS coldstone_web_ng_staging."_ids_orders";

-- Check: should list the role with the four privileges on each of the 24 tables
-- SELECT relation_name, privilege_type FROM svv_relation_privileges
-- WHERE namespace_name = 'coldstone_web_ng_analytics' AND identity_name = 'IAMR:coldstone-load-ng-lambda-role' ORDER BY 1, 2;
