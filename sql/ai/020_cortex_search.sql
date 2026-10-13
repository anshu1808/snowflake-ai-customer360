-- OPTIONAL (Cortex Search): semantic search over support tickets for RAG / agents.
-- {{WAREHOUSE}} is the warehouse that refreshes the index.

CREATE CORTEX SEARCH SERVICE IF NOT EXISTS GOLD.SUPPORT_TICKET_SEARCH
    ON SEARCH_TEXT
    ATTRIBUTES CUSTOMER_ID, CATEGORY, PRIORITY, STATUS
    WAREHOUSE = {{WAREHOUSE}}
    TARGET_LAG = '1 day'
AS (
    SELECT
        TICKET_ID,
        CUSTOMER_ID,
        CATEGORY,
        PRIORITY,
        STATUS,
        SUBJECT || '. ' || DESCRIPTION AS SEARCH_TEXT
    FROM SILVER.SUPPORT_TICKET
);
