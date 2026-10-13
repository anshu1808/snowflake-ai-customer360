-- Gold: one row per customer combining profile, accounts, recent activity and support.
-- {{AS_OF}} is injected by the pipeline runner (default: current timestamp; override with
-- PIPELINE_AS_OF for reproducible demos/tests).
--
-- Assumptions (confirm with the business):
--   * ACCOUNT.BALANCE for CREDIT accounts is reported as CREDIT_BALANCE, and is excluded
--     from DEPOSIT_BALANCE, which drives VALUE_TIER.
--   * Only COMPLETED transactions count as customer activity.
--   * CHURN_RISK is a transparent rule-based heuristic, not a trained model.

INSERT OVERWRITE INTO GOLD.CUSTOMER_360 (
    CUSTOMER_ID, FULL_NAME, EMAIL, PHONE, CITY, STATE, CUSTOMER_SEGMENT, CUSTOMER_STATUS,
    AGE, TENURE_MONTHS, ACCOUNT_COUNT, ACTIVE_ACCOUNT_COUNT,
    TOTAL_BALANCE, DEPOSIT_BALANCE, CREDIT_BALANCE,
    TXN_COUNT_30D, TXN_COUNT_90D, DEBIT_AMOUNT_90D, CREDIT_AMOUNT_90D,
    LAST_TXN_TS, DAYS_SINCE_LAST_TXN,
    TICKET_COUNT, OPEN_TICKET_COUNT, HIGH_PRIORITY_OPEN_TICKETS,
    VALUE_TIER, CHURN_RISK_SCORE, CHURN_RISK, AS_OF_TS
)
WITH params AS (
    SELECT {{AS_OF}} AS AS_OF
),
acct AS (
    SELECT
        CUSTOMER_ID,
        COUNT(*)                                                      AS ACCOUNT_COUNT,
        COUNT_IF(ACCOUNT_STATUS = 'ACTIVE')                           AS ACTIVE_ACCOUNT_COUNT,
        SUM(BALANCE)                                                  AS TOTAL_BALANCE,
        SUM(IFF(ACCOUNT_TYPE <> 'CREDIT', BALANCE, 0))                AS DEPOSIT_BALANCE,
        SUM(IFF(ACCOUNT_TYPE = 'CREDIT', BALANCE, 0))                 AS CREDIT_BALANCE
    FROM SILVER.ACCOUNT
    GROUP BY CUSTOMER_ID
),
txn AS (
    SELECT
        t.CUSTOMER_ID,
        COUNT_IF(t.TRANSACTION_TIMESTAMP > DATEADD('day', -30, p.AS_OF))                    AS TXN_COUNT_30D,
        COUNT(*)                                                                            AS TXN_COUNT_90D,
        SUM(IFF(t.TRANSACTION_TYPE = 'DEBIT',  t.AMOUNT, 0))                                AS DEBIT_AMOUNT_90D,
        SUM(IFF(t.TRANSACTION_TYPE = 'CREDIT', t.AMOUNT, 0))                                AS CREDIT_AMOUNT_90D
    FROM SILVER.TRANSACTION t
    CROSS JOIN params p
    WHERE t.TRANSACTION_STATUS = 'COMPLETED'
      AND t.TRANSACTION_TIMESTAMP >  DATEADD('day', -90, p.AS_OF)
      AND t.TRANSACTION_TIMESTAMP <= p.AS_OF
    GROUP BY t.CUSTOMER_ID
),
last_txn AS (
    SELECT t.CUSTOMER_ID, MAX(t.TRANSACTION_TIMESTAMP) AS LAST_TXN_TS
    FROM SILVER.TRANSACTION t
    CROSS JOIN params p
    WHERE t.TRANSACTION_STATUS = 'COMPLETED'
      AND t.TRANSACTION_TIMESTAMP <= p.AS_OF
    GROUP BY t.CUSTOMER_ID
),
tkt AS (
    SELECT
        CUSTOMER_ID,
        COUNT(*)                                                                         AS TICKET_COUNT,
        COUNT_IF(STATUS IN ('OPEN', 'IN_PROGRESS'))                                      AS OPEN_TICKET_COUNT,
        COUNT_IF(STATUS IN ('OPEN', 'IN_PROGRESS') AND PRIORITY = 'HIGH')                AS HIGH_PRIORITY_OPEN_TICKETS
    FROM SILVER.SUPPORT_TICKET
    GROUP BY CUSTOMER_ID
),
base AS (
    SELECT
        c.CUSTOMER_ID,
        TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.LAST_NAME, ''))             AS FULL_NAME,
        c.EMAIL, c.PHONE, c.CITY, c.STATE, c.CUSTOMER_SEGMENT, c.CUSTOMER_STATUS,
        DATEDIFF('year', c.DATE_OF_BIRTH, p.AS_OF::DATE)
            - IFF(DATEADD('year', DATEDIFF('year', c.DATE_OF_BIRTH, p.AS_OF::DATE), c.DATE_OF_BIRTH) > p.AS_OF::DATE, 1, 0)
                                                                                         AS AGE,
        DATEDIFF('month', c.CREATED_AT, p.AS_OF)                                         AS TENURE_MONTHS,
        COALESCE(a.ACCOUNT_COUNT, 0)                                                     AS ACCOUNT_COUNT,
        COALESCE(a.ACTIVE_ACCOUNT_COUNT, 0)                                              AS ACTIVE_ACCOUNT_COUNT,
        COALESCE(a.TOTAL_BALANCE, 0)                                                     AS TOTAL_BALANCE,
        COALESCE(a.DEPOSIT_BALANCE, 0)                                                   AS DEPOSIT_BALANCE,
        COALESCE(a.CREDIT_BALANCE, 0)                                                    AS CREDIT_BALANCE,
        COALESCE(x.TXN_COUNT_30D, 0)                                                     AS TXN_COUNT_30D,
        COALESCE(x.TXN_COUNT_90D, 0)                                                     AS TXN_COUNT_90D,
        COALESCE(x.DEBIT_AMOUNT_90D, 0)                                                  AS DEBIT_AMOUNT_90D,
        COALESCE(x.CREDIT_AMOUNT_90D, 0)                                                 AS CREDIT_AMOUNT_90D,
        l.LAST_TXN_TS,
        DATEDIFF('day', l.LAST_TXN_TS, p.AS_OF)                                          AS DAYS_SINCE_LAST_TXN,
        COALESCE(k.TICKET_COUNT, 0)                                                      AS TICKET_COUNT,
        COALESCE(k.OPEN_TICKET_COUNT, 0)                                                 AS OPEN_TICKET_COUNT,
        COALESCE(k.HIGH_PRIORITY_OPEN_TICKETS, 0)                                        AS HIGH_PRIORITY_OPEN_TICKETS,
        p.AS_OF                                                                          AS AS_OF_TS
    FROM SILVER.CUSTOMER c
    CROSS JOIN params p
    LEFT JOIN acct     a ON a.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN txn      x ON x.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN last_txn l ON l.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN tkt      k ON k.CUSTOMER_ID = c.CUSTOMER_ID
),
scored AS (
    SELECT
        b.*,
        IFF(b.CUSTOMER_STATUS <> 'ACTIVE', 3, 0)
        + CASE
            WHEN b.DAYS_SINCE_LAST_TXN IS NULL THEN 2
            WHEN b.DAYS_SINCE_LAST_TXN > 60    THEN 2
            WHEN b.DAYS_SINCE_LAST_TXN > 30    THEN 1
            ELSE 0
          END
        + IFF(b.HIGH_PRIORITY_OPEN_TICKETS > 0, 2, 0)
        + IFF(b.OPEN_TICKET_COUNT > 0, 1, 0)                                             AS CHURN_RISK_SCORE
    FROM base b
)
SELECT
    CUSTOMER_ID, FULL_NAME, EMAIL, PHONE, CITY, STATE, CUSTOMER_SEGMENT, CUSTOMER_STATUS,
    AGE, TENURE_MONTHS, ACCOUNT_COUNT, ACTIVE_ACCOUNT_COUNT,
    TOTAL_BALANCE, DEPOSIT_BALANCE, CREDIT_BALANCE,
    TXN_COUNT_30D, TXN_COUNT_90D, DEBIT_AMOUNT_90D, CREDIT_AMOUNT_90D,
    LAST_TXN_TS, DAYS_SINCE_LAST_TXN,
    TICKET_COUNT, OPEN_TICKET_COUNT, HIGH_PRIORITY_OPEN_TICKETS,
    CASE
        WHEN DEPOSIT_BALANCE >= 200000 THEN 'HIGH'
        WHEN DEPOSIT_BALANCE >= 50000  THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                                                  AS VALUE_TIER,
    CHURN_RISK_SCORE,
    CASE
        WHEN CHURN_RISK_SCORE >= 4 THEN 'HIGH'
        WHEN CHURN_RISK_SCORE >= 2 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                                                  AS CHURN_RISK,
    AS_OF_TS
FROM scored;
