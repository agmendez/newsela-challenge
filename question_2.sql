DECLARE analysis_end_year int64;
DECLARE analysis_start_year int64;
DECLARE reporting_start_year int64;

/*
In a larger dataset I might have opted to reduce the data being processed by using the year filters analysis_start_year and analysis_end_year
from the very first CTE, and then filtered once more by reporting_start_year to include just 10 periods.
As it stands, it makes more sense to let the calculation be done for all periods and then retrieve only the relevant timeframe

 */
SET analysis_end_year = 2022;
SET reporting_start_year = analysis_end_year - 10;
SET analysis_start_year = reporting_start_year - 1;

with prefiltered_post_questions as (
   SELECT * EXCEPT (tags),
   FROM `bigquery-public-data.stackoverflow.posts_questions`
    , UNNEST(SPLIT(tags, '|')) tag
   WHERE 1 = 1
     AND tag IN ('python', 'dbt')
     AND ARRAY_LENGTH(SPLIT(tags, '|')) = 1 -- only questions with a single tag
     and EXTRACT(YEAR FROM creation_date) between analysis_start_year and analysis_end_year
)
-- nulling out any broken accepted answer ids
, sanitized_post_questions as (
  select q.* EXCEPT(accepted_answer_id), a.id accepted_answer_id
  from prefiltered_post_questions q
  LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` a
  ON q.accepted_answer_id = a.id

)
   , yearly_tag_metrics as (
   SELECT tag,
          EXTRACT(YEAR FROM creation_date)                   post_year,
          count(1)                                           total_questions,
          sum(answer_count)                                  total_answers,
          COUNTIF(accepted_answer_id IS NOT NULL)            total_approved_answers,
          SAFE_DIVIDE(count(1), sum(answer_count))           question_to_answer_ratio,
          COUNTIF(accepted_answer_id IS NOT NULL) / COUNT(1) approved_answer_ratio
   FROM sanitized_post_questions
   group by tag, EXTRACT(YEAR FROM creation_date)
)
   , tag_change_yoy as (
   select curr.tag,
          curr.post_year,

          --base metrics
          curr.total_questions                                          curr_total_questions,
          prev.total_questions                                          prev_total_questions,
          curr.total_answers                                            curr_total_answers,
          prev.total_answers                                            prev_total_answers,
          curr.total_approved_answers                                   curr_total_approved_answers,
          prev.total_approved_answers                                   prev_total_approved_answers,


          curr.approved_answer_ratio                                    curr_approved_answer_ratio,
          prev.approved_answer_ratio                                    prev_approved_answer_ratio,
          -- defining these as relative changes instead of absolute ones for YoY
          SAFE_DIVIDE(curr.approved_answer_ratio - prev.approved_answer_ratio,
                      prev.approved_answer_ratio)                         answer_rate_yoy_relative_change,
          coalesce(curr.approved_answer_ratio, 0) - coalesce(prev.approved_answer_ratio, 0)       approved_answer_ratio_yoy_absolute_change,


          curr.question_to_answer_ratio                                 curr_question_to_answer_ratio,
          prev.question_to_answer_ratio                                 prev_question_to_answer_ratio,
          safe_divide(curr.question_to_answer_ratio - prev.question_to_answer_ratio,
                      prev.question_to_answer_ratio)                      question_to_answer_ratio_yoy_relative_change,
          coalesce(curr.question_to_answer_ratio, 0) - coalesce(prev.question_to_answer_ratio, 0) question_to_answer_ratio_yoy_absolute_change,
   -- this logic could've been solved with a lag(), but it would be operating under the (true in this case, but shaky)
   -- assumption that no gaps exist in the data for the past 10 years
   from yearly_tag_metrics curr
            left join yearly_tag_metrics prev
                      on curr.post_year - 1 = prev.post_year and curr.tag = prev.tag
                           )
/*
 I already have everything I need to answer both parts of the question separately
 */
select tag,
       post_year,

       curr_question_to_answer_ratio,
       question_to_answer_ratio_yoy_relative_change,
       question_to_answer_ratio_yoy_absolute_change,

       curr_approved_answer_ratio,
       answer_rate_yoy_relative_change,
       approved_answer_ratio_yoy_absolute_change,


       -- below fields are just for quick sanity checks, not really needed to answer
       curr_total_questions,
       curr_total_answers,
       curr_total_approved_answers,

from tag_change_yoy
where post_year between reporting_start_year and analysis_end_year
order by post_year desc, tag