--liquibase formatted sql

--changeset anshu:common-schemas-001
--comment: Medallion + audit schemas. BRONZE is the connection schema and must already exist.

CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS AUDIT;

--rollback DROP SCHEMA IF EXISTS AUDIT;
--rollback DROP SCHEMA IF EXISTS GOLD;
--rollback DROP SCHEMA IF EXISTS SILVER;
