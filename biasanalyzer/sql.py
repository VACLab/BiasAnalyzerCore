# SQL templates for querying in OMOP database

AGE_DISTRIBUTION_QUERY = '''
    WITH Age_Cohort AS (
        SELECT p.person_id, EXTRACT(YEAR FROM c.cohort_start_date) - p.year_of_birth AS age 
        FROM cohort c JOIN person p ON c.subject_id = p.person_id
        WHERE c.cohort_definition_id = {}
        ),
    -- Define age bins manually using SELECT statements and UNION ALL
    Age_Bins AS (
        SELECT '0-10' AS age_bin, 0 AS min_age, 10 AS max_age
        UNION ALL SELECT '11-20', 11, 20
        UNION ALL SELECT '21-30', 21, 30
        UNION ALL SELECT '31-40', 31, 40
        UNION ALL SELECT '41-50', 41, 50
        UNION ALL SELECT '51-60', 51, 60
        UNION ALL SELECT '61-70', 61, 70
        UNION ALL SELECT '71-80', 71, 80
        UNION ALL SELECT '81-90', 81, 90
        UNION ALL SELECT '91+', 91, 150  -- Max age is 150 for the last bin
    ),
    -- Define age bins and count individuals in each bin
    Age_Distribution AS (    
        SELECT
            b.age_bin,
            COUNT(ac.person_id) AS bin_count
        FROM Age_Bins b
        LEFT JOIN Age_Cohort ac on ac.age BETWEEN b.min_age AND b.max_age
        GROUP BY age_bin  
    )
    -- Calculate total cohort size and normalize to get probability distribution
    SELECT 
        age_bin,
        bin_count,
        ROUND(bin_count * 1.0 / SUM(bin_count) OVER (), 4) AS probability -- Normalize to get probability
    FROM Age_Distribution
    ORDER BY age_bin                  
'''

GENDER_DISTRIBUTION_QUERY = '''
    WITH Gender_Categories AS (
        SELECT 'male' AS gender, 8507 AS gender_concept_id
        UNION ALL SELECT 'female', 8532
        UNION ALL SELECT 'other', NULL  -- NULL to represent any non-male/female cases
    ),
    Gender_Distribution AS (
        SELECT
            gc.gender,
            COUNT(cd.person_id) AS gender_count
        FROM Gender_Categories gc
        LEFT JOIN (
            SELECT
                CASE
                    WHEN p.gender_concept_id = 8507 THEN 'male'
                    WHEN p.gender_concept_id = 8532 THEN 'female'
                    ELSE 'other'
                END AS gender,
                p.person_id
            FROM cohort c 
            JOIN person p ON c.subject_id = p.person_id 
            WHERE c.cohort_definition_id = {}
        ) cd ON gc.gender = cd.gender
        GROUP BY gc.gender
    )
    -- Calculate total cohort size and normalize to get probability distribution
    SELECT 
        gender,
        COALESCE(gender_count, 0) AS gender_count,  -- Ensure that NULL gender counts are treated as 0
        ROUND(COALESCE(gender_count, 0) * 100.0 / SUM(COALESCE(gender_count, 0)) OVER (), 2) AS probability
    FROM Gender_Distribution
    ORDER BY gender;
'''

AGE_STATS_QUERY = '''
    WITH Age_Cohort AS (
        SELECT p.person_id, EXTRACT(YEAR FROM c.cohort_start_date) - p.year_of_birth AS age 
        FROM cohort c JOIN person p ON c.subject_id = p.person_id
        WHERE c.cohort_definition_id = {}
        )
    -- Calculate age distribution statistics    
    SELECT
        COUNT(*) AS total_count,
        MIN(age) AS min_age,
        MAX(age) AS max_age,
        ROUND(AVG(age), 2) AS avg_age,
        CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) AS INT) AS median_age,
        ROUND(STDDEV(age), 2) as stddev_age
    FROM Age_Cohort                
'''

GENDER_STATS_QUERY = '''
    SELECT
        CASE
            WHEN p.gender_concept_id = 8507 THEN 'male'
            WHEN p.gender_concept_id = 8532 THEN 'female'
            ELSE 'other'
        END AS gender,     
        COUNT(*) AS gender_count,
        ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 2) as probability
    FROM cohort c JOIN person p ON c.subject_id = p.person_id 
    WHERE c.cohort_definition_id = {}
    GROUP BY p.gender_concept_id
'''

RACE_STATS_QUERY = '''
        SELECT
            CASE
                WHEN p.race_concept_id = 8516 THEN 'Black or African American'
                WHEN p.race_concept_id = 8515 THEN 'Asian'
                WHEN p.race_concept_id = 8657 THEN 'American Indian or Alaska Native'
                WHEN p.race_concept_id = 8527 THEN 'White'
                WHEN p.race_concept_id = 8557 THEN 'Native Hawaiian or Other Pacific Islander'
                ELSE 'Other'
            END AS race,     
            COUNT(*) AS race_count,
            ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS probability
        FROM cohort c JOIN person p ON c.subject_id = p.person_id
        WHERE c.cohort_definition_id = {}
        GROUP BY p.race_concept_id 
'''

ETHNICITY_STATS_QUERY = '''
    SELECT
        CASE
            WHEN p.ethnicity_concept_id = 38003563 THEN 'Hispanic or Latino'
            WHEN p.ethnicity_concept_id = 38003564 THEN 'Not Hispanic or Latino'
            ELSE 'other'
        END AS ethnicity,     
        COUNT(*) AS ethnicity_count,
        ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS probability
    FROM cohort c JOIN person p ON c.subject_id = p.person_id
    WHERE c.cohort_definition_id = {}
    GROUP BY p.ethnicity_concept_id
'''

COHORT_CONCEPT_CONDITION_PREVALENCE_QUERY = '''
    WITH cohort_conditions AS (
        -- Compute the counts for each condition node
        SELECT
            co.condition_concept_id AS concept_id,
            ct.subject_id
        FROM
            cohort ct
        JOIN
            condition_occurrence co ON ct.subject_id = co.person_id
            AND co.condition_start_date >= ct.cohort_start_date
            AND (co.condition_end_date IS NULL OR co.condition_start_date <= ct.cohort_end_date)
        WHERE ct.cohort_definition_id = {cid}   
    ),
    aggregated_counts AS (
        -- Aggregate counts for parent nodes using the concept_ancestor table
        SELECT
            ca.ancestor_concept_id AS concept_id,
            COUNT(DISTINCT cc.subject_id) AS count_in_cohort
        FROM
            cohort_conditions cc
        JOIN
            concept_ancestor ca 
            ON cc.concept_id = ca.descendant_concept_id
        WHERE 
            ca.min_levels_of_separation >= 0
        GROUP BY
            ca.ancestor_concept_id
    ),
    concept_hierarchy AS (
        -- Retrieve the direct parent-child hierarchy for all concepts involved
        SELECT
            ca.ancestor_concept_id,
            ca.descendant_concept_id,
        FROM
            concept_ancestor ca
        WHERE
            ca.min_levels_of_separation = 1 
            AND ca.descendant_concept_id IN (SELECT concept_id FROM aggregated_counts where count_in_cohort > {filter_count})
            AND ca.ancestor_concept_id IN (SELECT concept_id FROM aggregated_counts where count_in_cohort > {filter_count})
    )
    -- Combine counts and hierarchy with concept details
    SELECT DISTINCT
        c.concept_name,
        c.concept_code,
        ac.count_in_cohort,
        (ac.count_in_cohort * 1.0 / (SELECT COUNT(*) FROM cohort WHERE cohort_definition_id = {cid})) AS prevalence,
        ch.ancestor_concept_id,
        ch.descendant_concept_id
    FROM
        aggregated_counts ac
    JOIN
        concept_hierarchy ch ON ac.concept_id = ch.descendant_concept_id
    JOIN
        concept c ON ac.concept_id = c.concept_id
    WHERE ac.count_in_cohort > {filter_count} 
    ORDER BY 
        prevalence DESC;
'''
COHORT_CONCEPT_DRUG_PREVALENCE_QUERY = '''
    WITH cohort_drugs AS (
        -- Compute the counts for each drug node
        SELECT
            de.drug_concept_id AS concept_id,
            ct.subject_id
        FROM
            cohort ct
        JOIN
            drug_exposure de ON ct.subject_id = de.person_id
            AND de.drug_exposure_start_date >= ct.cohort_start_date
            AND (de.drug_exposure_start_date IS NULL OR de.drug_exposure_start_date <= ct.cohort_end_date)
        WHERE ct.cohort_definition_id = {cid}       
    ),
    aggregated_counts AS (
        -- Aggregate counts for parent nodes using the concept_ancestor table
        SELECT
            ca.ancestor_concept_id AS concept_id,
            COUNT(DISTINCT cd.subject_id) AS count_in_cohort
        FROM
            cohort_drugs cd
        JOIN
            concept_ancestor ca 
            ON cd.concept_id = ca.descendant_concept_id
        JOIN
            concept anc ON ca.ancestor_concept_id = anc.concept_id    
        WHERE
            anc.vocabulary_id = '{vocab}' AND 
            ca.min_levels_of_separation >= 0 -- Ensure valid ancestor relationships
        GROUP BY
            ca.ancestor_concept_id
    ),
    concept_hierarchy AS (
        -- Retrieve the hierarchy for all concepts involved
        SELECT
            ca.ancestor_concept_id,
            ca.descendant_concept_id
        FROM
            concept_ancestor ca
        WHERE
            ca.min_levels_of_separation = 1
            AND ca.descendant_concept_id IN (SELECT concept_id FROM aggregated_counts where count_in_cohort > {filter_count})
            AND ca.ancestor_concept_id IN (SELECT concept_id FROM aggregated_counts where count_in_cohort > {filter_count})
    )
    -- Combine counts and hierarchy with concept details
    SELECT DISTINCT
        c.concept_name,
        c.concept_code,
        ac.count_in_cohort,
        (ac.count_in_cohort * 1.0 / (SELECT COUNT(*) FROM cohort WHERE cohort_definition_id = {cid})) AS prevalence,
        ch.ancestor_concept_id,
        ch.descendant_concept_id
    FROM
        aggregated_counts ac
    JOIN
        concept_hierarchy ch ON ac.concept_id = ch.descendant_concept_id
    JOIN
        concept c ON ac.concept_id = c.concept_id
    WHERE ac.count_in_cohort > {filter_count} 
    ORDER BY 
        prevalence DESC;
'''