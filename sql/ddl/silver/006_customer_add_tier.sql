--liquibase formatted sql

--changeset anshu:silver-customer-add-tier-001

ALTER TABLE CUSTOMER
ADD COLUMN CUSTOMER_TIER VARCHAR;

--rollback ALTER TABLE CUSTOMER DROP COLUMN CUSTOMER_TIER;