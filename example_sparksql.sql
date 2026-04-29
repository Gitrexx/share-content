-- ============================================================
-- Complex Spark SQL: Customer 360 & Risk Scoring Pipeline
-- Input Tables: 10 tables, each 20+ columns
-- Output Tables: 2 tables (customer_360_output, risk_score_output)
-- ============================================================

WITH

-- CTE 1: Clean and filter customer base
cte_customer_base AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        CONCAT(c.first_name, ' ', c.last_name) AS full_name,
        c.email,
        c.phone_number,
        c.date_of_birth,
        FLOOR(DATEDIFF(CURRENT_DATE, c.date_of_birth) / 365.25) AS age,
        c.gender,
        c.nationality,
        c.registration_date,
        c.customer_tier,
        a.address_line_1,
        a.address_line_2,
        a.city,
        a.state,
        a.postal_code,
        a.country,
        CONCAT(a.city, ', ', a.state, ', ', a.country) AS full_address
    FROM customers c
    LEFT JOIN addresses a ON c.customer_id = a.customer_id AND a.is_primary = 1
    WHERE c.is_active = 1
),

-- CTE 2: Aggregate transaction history
cte_txn_summary AS (
    SELECT
        t.customer_id,
        COUNT(t.transaction_id) AS total_transactions,
        SUM(t.amount) AS total_spend,
        AVG(t.amount) AS avg_transaction_amount,
        MAX(t.amount) AS max_transaction_amount,
        MIN(t.amount) AS min_transaction_amount,
        MAX(t.transaction_date) AS last_transaction_date,
        MIN(t.transaction_date) AS first_transaction_date,
        DATEDIFF(CURRENT_DATE, MAX(t.transaction_date)) AS days_since_last_txn,
        COUNT(DISTINCT t.merchant_category) AS distinct_merchant_categories,
        SUM(CASE WHEN t.is_international = 1 THEN t.amount ELSE 0 END) AS international_spend,
        SUM(CASE WHEN t.channel = 'online' THEN t.amount ELSE 0 END) AS online_spend,
        SUM(CASE WHEN t.channel = 'offline' THEN t.amount ELSE 0 END) AS offline_spend
    FROM transactions t
    WHERE t.transaction_date >= DATE_SUB(CURRENT_DATE, 365)
    GROUP BY t.customer_id
),

-- CTE 3: Product holdings aggregation
cte_product_holdings AS (
    SELECT
        ph.customer_id,
        COUNT(DISTINCT ph.product_id) AS num_products,
        SUM(CASE WHEN p.product_category = 'savings' THEN 1 ELSE 0 END) AS num_savings,
        SUM(CASE WHEN p.product_category = 'loan' THEN 1 ELSE 0 END) AS num_loans,
        SUM(CASE WHEN p.product_category = 'investment' THEN 1 ELSE 0 END) AS num_investments,
        SUM(CASE WHEN p.product_category = 'insurance' THEN 1 ELSE 0 END) AS num_insurance,
        SUM(ph.current_balance) AS total_balance,
        MAX(ph.open_date) AS latest_product_open_date,
        MIN(ph.open_date) AS earliest_product_open_date
    FROM product_holdings ph
    JOIN products p ON ph.product_id = p.product_id
    WHERE ph.status = 'active'
    GROUP BY ph.customer_id
),

-- CTE 4: Loan details
cte_loan_details AS (
    SELECT
        l.customer_id,
        COUNT(l.loan_id) AS active_loans,
        SUM(l.principal_amount) AS total_principal,
        SUM(l.outstanding_balance) AS total_outstanding,
        AVG(l.interest_rate) AS avg_interest_rate,
        MAX(l.maturity_date) AS latest_maturity,
        SUM(l.monthly_payment) AS total_monthly_payment,
        SUM(CASE WHEN l.days_past_due > 0 THEN 1 ELSE 0 END) AS num_delinquent_loans,
        MAX(l.days_past_due) AS max_days_past_due,
        SUM(CASE WHEN l.loan_type = 'mortgage' THEN l.outstanding_balance ELSE 0 END) AS mortgage_outstanding,
        SUM(CASE WHEN l.loan_type = 'personal' THEN l.outstanding_balance ELSE 0 END) AS personal_loan_outstanding
    FROM loans l
    WHERE l.status = 'active'
    GROUP BY l.customer_id
),

-- CTE 5: Customer interactions / service
cte_interactions AS (
    SELECT
        ci.customer_id,
        COUNT(ci.interaction_id) AS total_interactions,
        SUM(CASE WHEN ci.channel = 'call_center' THEN 1 ELSE 0 END) AS call_center_contacts,
        SUM(CASE WHEN ci.channel = 'branch' THEN 1 ELSE 0 END) AS branch_visits,
        SUM(CASE WHEN ci.channel = 'digital' THEN 1 ELSE 0 END) AS digital_interactions,
        SUM(CASE WHEN ci.interaction_type = 'complaint' THEN 1 ELSE 0 END) AS num_complaints,
        MAX(ci.interaction_date) AS last_interaction_date,
        AVG(ci.satisfaction_score) AS avg_satisfaction_score
    FROM customer_interactions ci
    WHERE ci.interaction_date >= DATE_SUB(CURRENT_DATE, 365)
    GROUP BY ci.customer_id
),

-- CTE 6: Digital engagement metrics
cte_digital AS (
    SELECT
        dl.customer_id,
        SUM(dl.login_count) AS total_logins,
        AVG(dl.session_duration_sec) AS avg_session_duration,
        MAX(dl.last_login_date) AS last_login_date,
        SUM(dl.feature_clicks) AS total_feature_clicks,
        dl.preferred_device,
        dl.app_version,
        SUM(CASE WHEN dl.channel = 'mobile_app' THEN dl.login_count ELSE 0 END) AS mobile_logins,
        SUM(CASE WHEN dl.channel = 'web' THEN dl.login_count ELSE 0 END) AS web_logins
    FROM digital_logs dl
    WHERE dl.log_date >= DATE_SUB(CURRENT_DATE, 90)
    GROUP BY dl.customer_id, dl.preferred_device, dl.app_version
),

-- CTE 7: Credit bureau data
cte_credit AS (
    SELECT
        cb.customer_id,
        cb.credit_score,
        cb.credit_score_date,
        cb.num_open_accounts,
        cb.num_derogatory_marks,
        cb.total_credit_limit,
        cb.total_credit_used,
        ROUND(cb.total_credit_used / NULLIF(cb.total_credit_limit, 0), 4) AS credit_utilization,
        cb.num_hard_inquiries,
        cb.oldest_account_age_months,
        cb.bankruptcy_flag
    FROM credit_bureau cb
    INNER JOIN (
        SELECT customer_id, MAX(credit_score_date) AS max_date
        FROM credit_bureau
        GROUP BY customer_id
    ) latest ON cb.customer_id = latest.customer_id AND cb.credit_score_date = latest.max_date
),

-- CTE 8: Marketing campaign responses
cte_marketing AS (
    SELECT
        mr.customer_id,
        COUNT(mr.campaign_id) AS campaigns_targeted,
        SUM(CASE WHEN mr.response = 'positive' THEN 1 ELSE 0 END) AS positive_responses,
        SUM(CASE WHEN mr.response = 'negative' THEN 1 ELSE 0 END) AS negative_responses,
        SUM(mr.revenue_attributed) AS marketing_attributed_revenue,
        mc.latest_campaign_channel,
        mc.latest_campaign_name
    FROM marketing_responses mr
    LEFT JOIN (
        SELECT customer_id,
               FIRST_VALUE(campaign_channel) OVER (PARTITION BY customer_id ORDER BY response_date DESC) AS latest_campaign_channel,
               FIRST_VALUE(campaign_name) OVER (PARTITION BY customer_id ORDER BY response_date DESC) AS latest_campaign_name
        FROM marketing_responses
    ) mc ON mr.customer_id = mc.customer_id
    GROUP BY mr.customer_id, mc.latest_campaign_channel, mc.latest_campaign_name
),

-- CTE 9: KYC / Compliance flags
cte_compliance AS (
    SELECT
        k.customer_id,
        k.kyc_status,
        k.kyc_last_updated,
        k.risk_rating AS compliance_risk_rating,
        k.pep_flag,
        k.sanctions_flag,
        k.adverse_media_flag,
        CASE
            WHEN k.pep_flag = 1 OR k.sanctions_flag = 1 THEN 'HIGH'
            WHEN k.adverse_media_flag = 1 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS compliance_risk_level
    FROM kyc_compliance k
),

-- CTE 10: Combine for customer 360 base
cte_customer_360_base AS (
    SELECT
        cb.customer_id,
        cb.full_name,
        cb.email,
        cb.phone_number,
        cb.age,
        cb.gender,
        cb.nationality,
        cb.registration_date,
        cb.customer_tier,
        cb.full_address,
        cb.city,
        cb.country,
        ts.total_transactions,
        ts.total_spend,
        ts.avg_transaction_amount,
        ts.days_since_last_txn,
        ts.online_spend,
        ts.offline_spend,
        ts.international_spend,
        ph.num_products,
        ph.num_savings,
        ph.num_loans,
        ph.num_investments,
        ph.total_balance,
        ld.active_loans,
        ld.total_outstanding,
        ld.avg_interest_rate,
        ld.total_monthly_payment,
        ld.num_delinquent_loans,
        ld.max_days_past_due,
        ci.total_interactions,
        ci.num_complaints,
        ci.avg_satisfaction_score,
        dg.total_logins,
        dg.avg_session_duration,
        dg.preferred_device,
        dg.mobile_logins,
        dg.web_logins,
        cr.credit_score,
        cr.credit_utilization,
        cr.num_hard_inquiries,
        cr.bankruptcy_flag,
        mk.campaigns_targeted,
        mk.positive_responses,
        mk.marketing_attributed_revenue,
        mk.latest_campaign_channel,
        co.kyc_status,
        co.compliance_risk_rating,
        co.pep_flag,
        co.sanctions_flag,
        co.compliance_risk_level
    FROM cte_customer_base cb
    LEFT JOIN cte_txn_summary ts ON cb.customer_id = ts.customer_id
    LEFT JOIN cte_product_holdings ph ON cb.customer_id = ph.customer_id
    LEFT JOIN cte_loan_details ld ON cb.customer_id = ld.customer_id
    LEFT JOIN cte_interactions ci ON cb.customer_id = ci.customer_id
    LEFT JOIN cte_digital dg ON cb.customer_id = dg.customer_id
    LEFT JOIN cte_credit cr ON cb.customer_id = cr.customer_id
    LEFT JOIN cte_marketing mk ON cb.customer_id = mk.customer_id
    LEFT JOIN cte_compliance co ON cb.customer_id = co.customer_id
),

-- CTE 11: Derived risk features
cte_risk_features AS (
    SELECT
        b.customer_id,
        b.full_name,
        b.credit_score,
        b.credit_utilization,
        b.total_outstanding,
        b.total_monthly_payment,
        b.num_delinquent_loans,
        b.max_days_past_due,
        b.total_spend,
        b.days_since_last_txn,
        b.compliance_risk_level,
        b.pep_flag,
        b.sanctions_flag,
        b.bankruptcy_flag,
        b.num_complaints,
        b.age,
        COALESCE(b.total_monthly_payment, 0) / NULLIF(b.total_spend / 12.0, 0) AS debt_to_income_proxy,
        CASE
            WHEN b.credit_score >= 750 THEN 1
            WHEN b.credit_score >= 650 THEN 2
            WHEN b.credit_score >= 550 THEN 3
            ELSE 4
        END AS credit_score_band,
        CASE
            WHEN b.days_since_last_txn <= 30 THEN 'active'
            WHEN b.days_since_last_txn <= 90 THEN 'warm'
            WHEN b.days_since_last_txn <= 180 THEN 'cooling'
            ELSE 'dormant'
        END AS activity_status,
        CASE
            WHEN b.num_delinquent_loans > 0 AND b.max_days_past_due > 90 THEN 'HIGH'
            WHEN b.num_delinquent_loans > 0 OR b.credit_utilization > 0.8 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS behavioral_risk_flag
    FROM cte_customer_360_base b
),

-- CTE 12: Final risk scoring
cte_risk_scored AS (
    SELECT
        rf.customer_id,
        rf.full_name,
        rf.credit_score,
        rf.credit_score_band,
        rf.credit_utilization,
        rf.debt_to_income_proxy,
        rf.activity_status,
        rf.behavioral_risk_flag,
        rf.compliance_risk_level,
        rf.num_delinquent_loans,
        rf.max_days_past_due,
        rf.bankruptcy_flag,
        rf.pep_flag,
        rf.sanctions_flag,
        (
            CASE rf.credit_score_band
                WHEN 1 THEN 10
                WHEN 2 THEN 30
                WHEN 3 THEN 60
                WHEN 4 THEN 90
            END
            + CASE WHEN rf.bankruptcy_flag = 1 THEN 50 ELSE 0 END
            + CASE WHEN rf.pep_flag = 1 THEN 20 ELSE 0 END
            + CASE WHEN rf.sanctions_flag = 1 THEN 100 ELSE 0 END
            + CASE WHEN rf.num_delinquent_loans > 2 THEN 40 ELSE rf.num_delinquent_loans * 15 END
            + CASE WHEN rf.credit_utilization > 0.9 THEN 30
                   WHEN rf.credit_utilization > 0.7 THEN 15
                   ELSE 0 END
        ) AS composite_risk_score,
        CASE
            WHEN (
                CASE rf.credit_score_band WHEN 1 THEN 10 WHEN 2 THEN 30 WHEN 3 THEN 60 WHEN 4 THEN 90 END
                + CASE WHEN rf.bankruptcy_flag = 1 THEN 50 ELSE 0 END
                + CASE WHEN rf.pep_flag = 1 THEN 20 ELSE 0 END
                + CASE WHEN rf.sanctions_flag = 1 THEN 100 ELSE 0 END
                + CASE WHEN rf.num_delinquent_loans > 2 THEN 40 ELSE rf.num_delinquent_loans * 15 END
                + CASE WHEN rf.credit_utilization > 0.9 THEN 30 WHEN rf.credit_utilization > 0.7 THEN 15 ELSE 0 END
            ) >= 100 THEN 'CRITICAL'
            WHEN (
                CASE rf.credit_score_band WHEN 1 THEN 10 WHEN 2 THEN 30 WHEN 3 THEN 60 WHEN 4 THEN 90 END
                + CASE WHEN rf.bankruptcy_flag = 1 THEN 50 ELSE 0 END
                + CASE WHEN rf.pep_flag = 1 THEN 20 ELSE 0 END
                + CASE WHEN rf.sanctions_flag = 1 THEN 100 ELSE 0 END
                + CASE WHEN rf.num_delinquent_loans > 2 THEN 40 ELSE rf.num_delinquent_loans * 15 END
                + CASE WHEN rf.credit_utilization > 0.9 THEN 30 WHEN rf.credit_utilization > 0.7 THEN 15 ELSE 0 END
            ) >= 60 THEN 'HIGH'
            WHEN (
                CASE rf.credit_score_band WHEN 1 THEN 10 WHEN 2 THEN 30 WHEN 3 THEN 60 WHEN 4 THEN 90 END
                + CASE WHEN rf.bankruptcy_flag = 1 THEN 50 ELSE 0 END
                + CASE WHEN rf.pep_flag = 1 THEN 20 ELSE 0 END
                + CASE WHEN rf.sanctions_flag = 1 THEN 100 ELSE 0 END
                + CASE WHEN rf.num_delinquent_loans > 2 THEN 40 ELSE rf.num_delinquent_loans * 15 END
                + CASE WHEN rf.credit_utilization > 0.9 THEN 30 WHEN rf.credit_utilization > 0.7 THEN 15 ELSE 0 END
            ) >= 30 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_category
    FROM cte_risk_features rf
)

-- ==========================================
-- OUTPUT TABLE 1: customer_360_output
-- ==========================================
SELECT
    b.customer_id,
    b.full_name,
    b.email,
    b.phone_number,
    b.age,
    b.gender,
    b.nationality,
    b.customer_tier,
    b.full_address,
    b.city,
    b.country,
    b.total_transactions,
    b.total_spend,
    b.avg_transaction_amount,
    b.days_since_last_txn,
    b.online_spend,
    b.offline_spend,
    b.international_spend,
    b.num_products,
    b.total_balance,
    b.active_loans,
    b.total_outstanding,
    b.total_monthly_payment,
    b.total_interactions,
    b.num_complaints,
    b.avg_satisfaction_score,
    b.total_logins,
    b.preferred_device,
    b.credit_score,
    b.credit_utilization,
    b.campaigns_targeted,
    b.positive_responses,
    b.marketing_attributed_revenue,
    b.kyc_status,
    b.compliance_risk_level,
    rs.composite_risk_score,
    rs.risk_category
FROM cte_customer_360_base b
LEFT JOIN cte_risk_scored rs ON b.customer_id = rs.customer_id
;

-- ==========================================
-- OUTPUT TABLE 2: risk_score_output
-- ==========================================
SELECT
    rs.customer_id,
    rs.full_name,
    rs.credit_score,
    rs.credit_score_band,
    rs.credit_utilization,
    rs.debt_to_income_proxy,
    rs.activity_status,
    rs.behavioral_risk_flag,
    rs.compliance_risk_level,
    rs.num_delinquent_loans,
    rs.max_days_past_due,
    rs.bankruptcy_flag,
    rs.pep_flag,
    rs.sanctions_flag,
    rs.composite_risk_score,
    rs.risk_category,
    b.customer_tier,
    b.total_spend,
    b.num_products,
    b.total_balance,
    b.num_complaints,
    b.avg_satisfaction_score
FROM cte_risk_scored rs
LEFT JOIN cte_customer_360_base b ON rs.customer_id = b.customer_id
;
