/*
What tags on a Stack Overflow question lead to the most answers and the highest rate
of approved answers for the current year? What tags lead to the least? How about
combinations of tags?
 */
DECLARE analysis_year int64;
SET analysis_year = 2022;

with sanitized_post_questions as (
     select q.* EXCEPT (accepted_answer_id), a.id accepted_answer_id
     from `bigquery-public-data.stackoverflow.posts_questions` q
              LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` a
                        ON q.accepted_answer_id = a.id
                                 )
-- since I'm grouping by tag, I don't have to worry about row duplication
-- (unless the same question had the same tag multiple times, which isn't the case as proven in QC checks)
SELECT tag,
       count(1) total_questions,
       sum(answer_count) total_answers,
       COUNTIF(accepted_answer_id IS NOT NULL) / COUNT(1) approved_answer_rate,
       sum(answer_count) / count(1) avg_answers_per_question
FROM sanitized_post_questions
LEFT JOIN UNNEST(SPLIT(tags, '|')) tag
where EXTRACT(YEAR FROM creation_date) = analysis_year
group by tag
order by approved_answer_rate desc, avg_answers_per_question desc
--uncomment this line and comment the one above to answer least instead of most
-- order by approved_answer_rate , avg_answers_per_question
;

-- Multi-tag querying. This could've been the sole answer of the question, but since I started from the simple approach,
-- I'm keeping both options

DECLARE analysis_year int64;
SET analysis_year = 2022;

WITH RECURSIVE
sanitized_post_questions AS (
  SELECT
    q.* EXCEPT (tags, accepted_answer_id),
      a.id accepted_answer_id,
    ARRAY(
      SELECT tag
      FROM UNNEST(SPLIT(q.tags, '|')) AS tag
      ORDER BY tag
    ) AS tag_array
     from `bigquery-public-data.stackoverflow.posts_questions` q
              LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` a
                        ON q.accepted_answer_id = a.id
  WHERE EXTRACT(YEAR FROM q.creation_date) = analysis_year
),

unnested_tags AS (
  SELECT
    id AS question_id,
    tag,
    view_count,
    answer_count,
    accepted_answer_id
  FROM sanitized_post_questions,
  UNNEST(tag_array) AS tag
),

tag_combinations AS (
  -- base term: single tags
  SELECT
    question_id,
    [tag] AS tag_subset,
    tag AS last_tag,
    view_count,
    answer_count,
    accepted_answer_id
  FROM unnested_tags

  UNION ALL

  -- recursive: tag combination
  SELECT
    tc.question_id,
    ARRAY_CONCAT(tc.tag_subset, [ut.tag]) AS tag_subset,
    ut.tag AS last_tag,
    tc.view_count,
    tc.answer_count,
    tc.accepted_answer_id
  FROM tag_combinations tc
  JOIN unnested_tags ut
    ON tc.question_id = ut.question_id
   -- enforcing order to avoid permutations
  WHERE ut.tag > tc.last_tag
)

SELECT
  ARRAY_TO_STRING(tag_subset, ', ') AS combo_key,
  ARRAY_LENGTH(tag_subset) AS tag_length,

  COUNT(DISTINCT question_id) AS total_questions,

  SUM(answer_count) AS total_answers,
  SAFE_DIVIDE(SUM(answer_count), COUNT(DISTINCT question_id)) AS avg_answers_per_question,
  SAFE_DIVIDE(COUNTIF(accepted_answer_id IS NOT NULL), COUNT(DISTINCT question_id)) AS accepted_answer_rate

FROM tag_combinations
GROUP BY combo_key, tag_length
HAVING total_questions > 5 -- filter out meaningless combinations
ORDER BY total_questions DESC, total_answers DESC; --invert this to get least instead of most (this should've been a ranking, but I'm out of time)
