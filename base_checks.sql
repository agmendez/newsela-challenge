/*
 To be able to safely use the posts_questions' precalculated metrics,
 I first need to prove they align with the underlying data from posts_answers
 */

-- answer_count field cross-table validation: yields no results, so all good
select question.id question_id, question.answer_count, coalesce(count(answer.id), 0) answer_count_actual
from `bigquery-public-data`.stackoverflow.posts_answers question
left join `bigquery-public-data`.stackoverflow.posts_answers answer on answer.parent_id = question.id
group by question.id, question.answer_count
-- the cast here is because BQ is inferring a string type due to nulls
having coalesce(cast(question.answer_count as int64), 0) <> answer_count_actual


-- accepted_answer_id field validation: 230 outliers. this could be because the accepted answer was deleted
-- after this question's metrics got calculated, but it forces me to go through the posts_answers table for any calcs
-- involving this metric
select *
from `bigquery-public-data`.stackoverflow.posts_questions question
left join `bigquery-public-data`.stackoverflow.posts_answers answer on answer.id = question.accepted_answer_id
where answer.id is null and question.accepted_answer_id is not null;

-- sanity check: questions with an accepted answer id but 0 answers (from both checks above, this one is almost a given)
select *
from stackoverflow.posts_questions
where accepted_answer_id is not null and answer_count = 0;

-- tag field format validation: visually observed the tag separator is '|'.
-- I'll do a manual check of any parsed tag fields whose length is < 2 in case another separator was ever used
select distinct tags
from `bigquery-public-data`.stackoverflow.posts_questions
where ARRAY_LENGTH(SPLIT(tags, '|')) <= 1
order by LENGTH(tags) desc
-- found no outliers

--duplicate tag per question validation: yields no results
select id, tag, count(*)
from `bigquery-public-data`.stackoverflow.posts_questions
, UNNEST(SPLIT(tags, '|')) tag
group by id, tag
having count(*) > 1