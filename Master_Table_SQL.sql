/******************************************************************************************
Purpose: This script performs the core data consolidation process, merging all source and intermediate datasets into a master table that is the foundation for downstream delivery.
This process taken place in this script is highlighted in yellow in the ERD Chart.
This script is a simplified version of a real-time enterprise data merging process.
******************************************************************************************/


-- 1. CREATE BASE SOURCE TABLES
---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS CUSTOMER_PROFILE AS
SELECT
    user_id,
    first_name,
    last_name,
    email,
    signup_date,
    region_id,
    membership_tier,
    account_status
FROM source_db.CUSTOMER_PROFILE_RAW;

CREATE TABLE IF NOT EXISTS CUSTOMER_ACTIVITY_LOGS AS
SELECT
    user_id,
    activity_type,
    activity_date,
    activity_channel,
    points_earned,
    points_redeemed,
    feature_used
FROM source_db.CUSTOMER_ACTIVITY_LOGS_RAW;

CREATE TABLE IF NOT EXISTS FEATURE_ADOPTION AS
SELECT
    user_id,
    feature_name,
    adoption_date,
    feature_category
FROM source_db.FEATURE_ADOPTION_RAW;

CREATE TABLE IF NOT EXISTS INTERVENTION_LOG AS
SELECT
    user_id,
    intervention_type,
    intervention_date,
    campaign_name,
    outcome
FROM source_db.INTERVENTION_LOG_RAW;

CREATE TABLE IF NOT EXISTS SUPPORT_TICKET_COUNT_30D AS
SELECT
    user_id,
    COUNT(ticket_id) AS support_tickets_30d
FROM source_db.SUPPORT_TICKET_RAW
WHERE ticket_date >= CURRENT_DATE - INTERVAL '30 DAY'
GROUP BY user_id;

CREATE TABLE IF NOT EXISTS CHAT_SUPPORT_COUNT_30D AS
SELECT
    user_id,
    COUNT(chat_id) AS chat_sessions_30d
FROM source_db.CHAT_SUPPORT_RAW
WHERE chat_date >= CURRENT_DATE - INTERVAL '30 DAY'
GROUP BY user_id;

CREATE TABLE IF NOT EXISTS ISSUE_REPORTS AS
SELECT
    user_id,
    issue_type,
    issue_status,
    reported_date
FROM source_db.ISSUE_REPORTS_RAW;

CREATE TABLE IF NOT EXISTS SERVICE_REQUESTS AS
SELECT
    user_id,
    request_type,
    request_date,
    request_status
FROM source_db.SERVICE_REQUESTS_RAW;

CREATE TABLE IF NOT EXISTS RESOURCE_USAGE AS
SELECT
    user_id,
    resource_type,
    access_date
FROM source_db.RESOURCE_USAGE_RAW;

CREATE TABLE IF NOT EXISTS REPEAT_INTERACTIONS AS
SELECT
    user_id,
    COUNT(DISTINCT interaction_id) AS repeat_interactions
FROM source_db.INTERACTIONS_RAW
GROUP BY user_id;

CREATE TABLE IF NOT EXISTS REGION_METADATA AS
SELECT
    region_id,
    region_name,
    market_segment
FROM source_db.REGION_METADATA_RAW;

CREATE TABLE IF NOT EXISTS REGION_BENCHMARKS AS
SELECT
    region_id,
    avg_engagement_score AS region_avg_score,
    activity_threshold
FROM source_db.REGION_BENCHMARKS_RAW;


-- 2. CREATE CLEANED AND ENRICHED TABLES
---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ENRICHED_PROFILE AS
SELECT
    cp.user_id,
    cp.first_name,
    cp.last_name,
    cp.email,
    cp.signup_date,
    cp.region_id,
    rm.region_name,
    rm.market_segment,
    cp.membership_tier,
    cp.account_status,
    COALESCE(rb.region_avg_score, 0) AS region_baseline
FROM CUSTOMER_PROFILE cp
LEFT JOIN REGION_METADATA rm ON cp.region_id = rm.region_id
LEFT JOIN REGION_BENCHMARKS rb ON cp.region_id = rb.region_id;


CREATE TABLE IF NOT EXISTS USER_ACTIVITY_METRICS AS
SELECT
    ca.user_id,
    COUNT(DISTINCT ca.activity_date) AS active_days_30d,
    SUM(CASE WHEN ca.activity_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
    SUM(CASE WHEN ca.activity_type = 'login' THEN 1 ELSE 0 END) AS login_count,
    SUM(ca.points_earned) AS total_points_earned,
    SUM(ca.points_redeemed) AS total_points_redeemed,
    COUNT(DISTINCT ca.feature_used) AS unique_features_used
FROM CUSTOMER_ACTIVITY_LOGS ca
WHERE ca.activity_date >= CURRENT_DATE - INTERVAL '30 DAY'
GROUP BY ca.user_id;


CREATE TABLE IF NOT EXISTS FEATURE_USAGE_METRICS AS
SELECT
    fa.user_id,
    COUNT(DISTINCT fa.feature_name) AS total_features_adopted,
    MAX(fa.adoption_date) AS last_feature_adopted
FROM FEATURE_ADOPTION fa
GROUP BY fa.user_id;


CREATE TABLE IF NOT EXISTS SUPPORT_METRICS AS
SELECT
    cp.user_id,
    COALESCE(st.support_tickets_30d, 0) AS support_tickets_30d,
    COALESCE(cs.chat_sessions_30d, 0) AS chat_sessions_30d
FROM CUSTOMER_PROFILE cp
LEFT JOIN SUPPORT_TICKET_COUNT_30D st ON cp.user_id = st.user_id
LEFT JOIN CHAT_SUPPORT_COUNT_30D cs ON cp.user_id = cs.user_id;


CREATE TABLE IF NOT EXISTS SERVICE_AND_ISSUES AS
SELECT
    sr.user_id,
    COUNT(DISTINCT sr.request_type) AS service_request_types,
    COUNT(DISTINCT ir.issue_type) AS issue_types_reported
FROM SERVICE_REQUESTS sr
LEFT JOIN ISSUE_REPORTS ir ON sr.user_id = ir.user_id
GROUP BY sr.user_id;


CREATE TABLE IF NOT EXISTS RESOURCE_ENGAGEMENT AS
SELECT
    ru.user_id,
    COUNT(DISTINCT ru.resource_type) AS resource_types_used,
    COUNT(ru.access_date) AS resource_visits
FROM RESOURCE_USAGE ru
GROUP BY ru.user_id;


-- 3. BUILD CENTRAL MASTER TABLE
---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS USER_ENGAGEMENT_MASTER AS
SELECT
    ep.user_id,
    ep.first_name,
    ep.region_name,
    ep.market_segment,
    ep.membership_tier,
    ep.account_status,
    ua.active_days_30d,
    ua.purchase_count,
    ua.login_count,
    ua.total_points_earned,
    ua.total_points_redeemed,
    ua.unique_features_used,
    fm.total_features_adopted,
    fm.last_feature_adopted,
    sm.support_tickets_30d,
    sm.chat_sessions_30d,
    sa.service_request_types,
    sa.issue_types_reported,
    re.resource_types_used,
    re.resource_visits,
    COALESCE(ri.repeat_interactions, 0) AS repeat_interactions,
    ep.region_baseline
FROM ENRICHED_PROFILE ep
LEFT JOIN USER_ACTIVITY_METRICS ua ON ep.user_id = ua.user_id
LEFT JOIN FEATURE_USAGE_METRICS fm ON ep.user_id = fm.user_id
LEFT JOIN SUPPORT_METRICS sm ON ep.user_id = sm.user_id
LEFT JOIN SERVICE_AND_ISSUES sa ON ep.user_id = sa.user_id
LEFT JOIN RESOURCE_ENGAGEMENT re ON ep.user_id = re.user_id
LEFT JOIN REPEAT_INTERACTIONS ri ON ep.user_id = ri.user_id;


-- 4. CALCULATE USER ENGAGEMENT SCORE
---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS USER_ENGAGEMENT_SCORE AS
SELECT
    user_id,
    ROUND((
        (COALESCE(active_days_30d, 0) * 0.25) +
        (COALESCE(purchase_count, 0) * 0.30) +
        (COALESCE(unique_features_used, 0) * 0.10) +
        (COALESCE(total_features_adopted, 0) * 0.10) +
        (COALESCE(repeat_interactions, 0) * 0.10) -
        (CASE WHEN support_tickets_30d > 3 THEN 0.15 ELSE 0 END)
    ), 2) AS engagement_score
FROM USER_ENGAGEMENT_MASTER AS m;


-- ============================================================
-- 5. SEGMENT USERS AND OUTPUT TO TEAMS
-- ============================================================

CREATE TABLE IF NOT EXISTS USER_ENGAGEMENT_SEGMENT AS
SELECT
    um.user_id,
    um.first_name,
    um.region_name,
    um.market_segment,
    ue.engagement_score,
    CASE
        WHEN ue.engagement_score >= 7 THEN 'Highly Engaged'
        WHEN ue.engagement_score BETWEEN 4 AND 6.99 THEN 'Moderately Engaged'
        ELSE 'Low Engagement'
    END AS engagement_segment,
    CURRENT_DATE AS record_date
FROM USER_ENGAGEMENT_MASTER um
LEFT JOIN USER_ENGAGEMENT_SCORE ue ON um.user_id = ue.user_id;


-- 6. DOWNSTREAM TEAM OUTPUT TABLES
---------------------------------------------------------------

-- Marketing Team – Target highly engaged users for rewards
CREATE TABLE IF NOT EXISTS marketing.daily_loyalty_targets AS
SELECT *
FROM USER_ENGAGEMENT_SEGMENT
WHERE engagement_segment = 'Highly Engaged';

-- Customer Success Team – Users at churn risk
CREATE TABLE IF NOT EXISTS cs.daily_outreach_targets AS
SELECT *
FROM USER_ENGAGEMENT_SEGMENT
WHERE engagement_segment = 'Low Engagement';

-- Product Team – Feature usage insights by region
CREATE TABLE IF NOT EXISTS product.feature_adoption_summary AS
SELECT
    region_name,
    AVG(engagement_score) AS avg_engagement,
    AVG(total_features_adopted) AS avg_features_used,
    COUNT(DISTINCT user_id) AS total_users
FROM USER_ENGAGEMENT_MASTER um
JOIN USER_ENGAGEMENT_SCORE ue ON um.user_id = ue.user_id
GROUP BY region_name;

-- Analytics Team – Daily platform snapshot
CREATE TABLE IF NOT EXISTS analytics.daily_engagement_snapshot AS
SELECT
    *,
    CURRENT_TIMESTAMP AS load_timestamp
FROM USER_ENGAGEMENT_SEGMENT;

-- Loading final data to Marketing team and Customer Success Team in s3
----------------------------------------------------------------------
-- Define File Format for S3 Load
CREATE OR REPLACE FILE FORMAT STG.csv_unload_format
TYPE = csv
COMPRESSION = 'NONE'
FIELD_DELIMITER = '|'
FILE_EXTENSION = 'csv'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = TRUE
;

-- Create External Stage for S3 Output
CREATE OR REPLACE STAGE STG.USER_ENGAGEMENT_unload_stage
STORAGE_INTEGRATION = OUTGOING_INTEGRATION
URL = 's3://outgoing/User_Engagement/'
FILE_FORMAT = STG.csv_unload_format
;

-- Export final data to Marketing team's bucket
COPY INTO @STG.USER_ENGAGEMENT_unload_stage/marketing/daily_loyalty_targets/
FROM (
    SELECT
        user_id,
        first_name,
        region_name,
        market_segment,
        engagement_score,
        engagement_segment,
        record_date,
        CURRENT_TIMESTAMP AS export_dttm
    FROM USER_ENGAGEMENT_SEGMENT
    WHERE engagement_segment = 'Highly Engaged'
)
FILE_FORMAT = (FORMAT_NAME = STG.csv_unload_format)
HEADER = TRUE
OVERWRITE = TRUE
;

-- Export final data to Customer Success Team's bucket
COPY INTO @STG.USER_ENGAGEMENT_unload_stage/cs/daily_outreach_targets/
FROM (
    SELECT
        user_id,
        first_name,
        region_name,
        market_segment,
        engagement_score,
        engagement_segment,
        record_date,
        CURRENT_TIMESTAMP AS export_dttm
    FROM USER_ENGAGEMENT_SEGMENT
    WHERE engagement_segment = 'Low Engagement'
)
FILE_FORMAT = (FORMAT_NAME = STG.csv_unload_format)
HEADER = TRUE
OVERWRITE = TRUE
;

------------------------------------------------------------
-- Merge Load the final table to History Table to keep a record with a timestamp
-- This table is used in a monitor dashboard to track volume, loads and detect ETL error
------------------------------------------------------------
MERGE INTO TBLS.USER_ENGAGEMENT_SEGMENT_HISTORY AS H
USING (
    SELECT
        um.user_id,
        um.first_name,
        um.region_name,
        um.market_segment,
        ue.engagement_score,
        CASE
            WHEN ue.engagement_score >= 7 THEN 'Highly Engaged'
            WHEN ue.engagement_score BETWEEN 4 AND 6.99 THEN 'Moderately Engaged'
            ELSE 'Low Engagement'
        END AS engagement_segment,
        CURRENT_TIMESTAMP AS INCLUSION_DTTM,
        DATE(CURRENT_TIMESTAMP) AS INCLUSION_DATE
    FROM USER_ENGAGEMENT_MASTER um
    LEFT JOIN USER_ENGAGEMENT_SCORE ue ON um.user_id = ue.user_id
) AS T
ON (
    T.INCLUSION_DATE = H.INCLUSION_DATE
    AND T.user_id = H.user_id
    AND T.engagement_score = H.engagement_score
    AND T.engagement_segment = H.engagement_segment
    AND T.market_segment = H.market_segment
    AND T.region_name = H.region_name
)
WHEN NOT MATCHED THEN
    INSERT (
        H.INCLUSION_DATE, H.INCLUSION_DTTM, H.user_id, H.first_name, H.region_name,
        H.market_segment, H.engagement_score, H.engagement_segment )
    VALUES (
        T.INCLUSION_DATE, T.INCLUSION_DTTM, T.user_id, T.first_name, T.region_name,
        T.market_segment, T.engagement_score, T.engagement_segment )
;


-- End of Script
---------------------------------------------------------------