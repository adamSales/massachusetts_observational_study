drop function if exists last_func cascade; 
create function last_func(anyelement, anyelement) 
returns anyelement language sql immutable strict 
as $$ 
    select $2; 
$$; 

create aggregate last(anyelement) ( 
    sfunc = last_func, 
    stype = anyelement 
); 

create or replace view assignment_xrefs as 
select id as xid, target_id as id, xref 
from core.external_references 
where xref_type_id=3 
and partner_id=5; 

create or replace view student_xrefs as 
select id as xid, target_id as id, xref 
from core.external_references 
where xref_type_id=1 
and partner_id=5 
and external_references.id not in 
( 
	select user_xid 
	from users.user_roles 
	where user_role_definition_id != 6 
); 


-- Determine input and target alogs

drop table if exists ordered_alogs; 
create table ordered_alogs as 
select 
assignment_logs.id as assignment_log_id, 
assignment_logs.start_time, 
assignments.sequence_id, 
student_xrefs.id as student_id, 
row_number() over(partition by assignment_logs.user_xid, assignments.sequence_id order by assignment_logs.start_time) as log_order 
from student_data.assignment_logs 
inner join student_xrefs on student_xrefs.xid = assignment_logs.user_xid 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id; 

drop table if exists target_alogs; 
create table target_alogs as 
select *, 
encode_ceri('PS', sequence_id) as target_sequence 
from ordered_alogs 
where log_order = 1 
and encode_ceri('PS', sequence_id) in ('PSAHQV'); 

drop table if exists input_alogs; 
create table input_alogs as 
select 
target_alogs.target_sequence, 
ordered_alogs.* 
from ordered_alogs 
inner join target_alogs on target_alogs.student_id = ordered_alogs.student_id and target_alogs.start_time > ordered_alogs.start_time; 


-- Assignment Level Features

drop table if exists assignment_level_agg_features cascade; 
create table assignment_level_agg_features as 
select 
assignment_actions.assignment_log_id, 
sum((action_defn_type_id = 2)::int) + 1 as session_count, 
count(distinct extract(doy from timestamp)) as day_count, 
sum((action_defn_type_id = 12 and path not like '%SP%')::int) as completed_problem_count 
from student_data.assignment_actions 
inner join input_alogs on input_alogs.assignment_log_id = assignment_actions.assignment_log_id 
group by assignment_actions.assignment_log_id; 

drop table if exists assignment_level_stats cascade; 
create table assignment_level_stats as 
select 
assignments.sequence_id, 
avg(assignment_level_agg_features.session_count) as session_count_avg, 
coalesce(stddev(assignment_level_agg_features.session_count), 0) as session_count_stddev, 
avg(assignment_level_agg_features.day_count) as day_count_avg, 
coalesce(stddev(assignment_level_agg_features.day_count), 0) as day_count_stddev, 
avg(assignment_level_agg_features.completed_problem_count) as completed_problem_count_avg, 
coalesce(stddev(assignment_level_agg_features.completed_problem_count), 0) as completed_problem_count_stddev 
from assignment_level_agg_features 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_level_agg_features.assignment_log_id 
inner join core.assignments on assignments.id = assignment_xrefs.id 
group by assignments.sequence_id; 

drop table if exists assignment_level_features cascade; 
create table assignment_level_features as 
with filled as 
( 
	select 
	id, 
	last(end_time) over (partition by user_xid order by start_time) as end_time 
	from student_data.assignment_logs 
) 
select 
input_alogs.assignment_log_id, 
input_alogs.target_sequence, 
input_alogs.student_id, 
assignment_logs.start_time as assignment_start_time, 
(sequences.parameters like '%pseudo_skill_builder%' or sections.type='MasterySection' or sections.type='LinearMasterySection')::int as is_skill_builder, 
(assignments.due_date is not null)::int as has_due_date, 
(assignment_logs.end_time is not null)::int as assignment_completed, 
coalesce(extract(epoch from (assignment_logs.start_time - lag(assignment_logs.start_time, 1) over (partition by assignment_logs.user_xid order by assignment_logs.start_time))), 0) as time_since_last_assignment_start, 
coalesce(extract(epoch from (assignment_logs.start_time - lag(filled.end_time, 1) over (partition by assignment_logs.user_xid order by assignment_logs.start_time))), 0) as time_since_last_assignment_end, 
assignment_level_agg_features.session_count as session_count_raw, 
case when assignment_level_stats.session_count_stddev = 0 then 0 else (assignment_level_agg_features.session_count - assignment_level_stats.session_count_avg) / assignment_level_stats.session_count_stddev end as session_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by case when assignment_level_stats.session_count_stddev = 0 then 0 else (assignment_level_agg_features.session_count - assignment_level_stats.session_count_avg) / assignment_level_stats.session_count_stddev end) as session_count_class_percentile, 
assignment_level_agg_features.day_count as day_count_raw, 
case when assignment_level_stats.day_count_stddev = 0 then 0 else (assignment_level_agg_features.day_count - assignment_level_stats.day_count_avg) / assignment_level_stats.day_count_stddev end as day_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by case when assignment_level_stats.day_count_stddev = 0 then 0 else (assignment_level_agg_features.day_count - assignment_level_stats.day_count_avg) / assignment_level_stats.day_count_stddev end) as day_count_class_percentile, 
assignment_level_agg_features.completed_problem_count as completed_problem_count_raw, 
case when assignment_level_stats.completed_problem_count_stddev = 0 then 0 else (assignment_level_agg_features.completed_problem_count - assignment_level_stats.completed_problem_count_avg) / assignment_level_stats.completed_problem_count_stddev end as completed_problem_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by case when assignment_level_stats.completed_problem_count_stddev = 0 then 0 else (assignment_level_agg_features.completed_problem_count - assignment_level_stats.completed_problem_count_avg) / assignment_level_stats.completed_problem_count_stddev end) as completed_problem_count_class_percentile 
from student_data.assignment_logs 
inner join input_alogs on input_alogs.assignment_log_id = assignment_logs.id 
inner join student_xrefs on student_xrefs.xid = assignment_logs.user_xid 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join legacy.sequences on sequences.id = assignments.sequence_id 
inner join legacy.sections on sections.id = sequences.head_section_id 
inner join filled on filled.id = assignment_logs.id 
inner join assignment_level_agg_features on assignment_level_agg_features.assignment_log_id = assignment_logs.id
inner join assignment_level_stats on assignment_level_stats.sequence_id = sequences.id; 


-- Problem Level Features

drop table if exists problem_level_stats cascade;
create table problem_level_stats as 
with medians as
(
	select 
	problem_id as pid, 
	median(extract(epoch from (problem_logs.end_time - problem_logs.start_time))::numeric) as time_on_task_med, 
	median((problem_logs.first_response_time::float / 1000)::numeric) as first_response_time_med 
	from student_data.problem_logs 
	where end_time is not null 
	group by problem_id 
)
select 
problem_id, 
medians.time_on_task_med, 
median(abs(extract(epoch from (problem_logs.end_time - problem_logs.start_time)) - medians.time_on_task_med)::numeric) as time_on_task_mad, 
medians.first_response_time_med, 
median(abs((problem_logs.first_response_time::float / 1000) - medians.first_response_time_med)::numeric) as first_response_time_mad, 
avg(problem_logs.attempt_count) as attempt_count_avg, 
coalesce(stddev(problem_logs.attempt_count), 0) as attempt_count_stddev, 
avg((problem_logs.first_action_type_id = 1)::int) as answer_first_avg, 
coalesce(stddev((problem_logs.first_action_type_id = 1)::int), 0) as answer_first_stddev, 
avg((problem_logs.discrete_score = 1)::int) as correctness_avg, 
coalesce(stddev((problem_logs.discrete_score = 1)::int), 0) as correctness_stddev, 
avg((problem_logs.bottom_hint)::int) as answer_given_avg, 
coalesce(stddev((problem_logs.bottom_hint)::int), 0) as answer_given_stddev 
from student_data.problem_logs 
inner join input_alogs on input_alogs.assignment_log_id = problem_logs.assignment_log_id 
inner join medians on medians.pid = problem_logs.problem_id 
where end_time is not null 
group by problem_id, medians.time_on_task_med, medians.first_response_time_med; 

drop table if exists problem_level_features cascade; 
create table problem_level_features as 
select 
input_alogs.assignment_log_id as assignment_log_id_2, 
median(extract(epoch from (problem_logs.end_time - problem_logs.start_time))::numeric) as median_problem_time_on_task_raw, 
median((case when problem_level_stats.time_on_task_mad = 0 then 0 else (extract(epoch from (problem_logs.end_time - problem_logs.start_time)) - problem_level_stats.time_on_task_med) / problem_level_stats.time_on_task_mad end)::numeric) as median_problem_time_on_task_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by median((case when problem_level_stats.time_on_task_mad = 0 then 0 else (extract(epoch from (problem_logs.end_time - problem_logs.start_time)) - problem_level_stats.time_on_task_med) / problem_level_stats.time_on_task_mad end)::numeric)) as median_problem_time_on_task_class_percentile, 
median((problem_logs.first_response_time::float / 1000)::numeric) as median_problem_first_response_time_raw, 
median((case when problem_level_stats.first_response_time_mad = 0 then 0 else ((problem_logs.first_response_time::float / 1000) - problem_level_stats.first_response_time_med) / problem_level_stats.first_response_time_mad end)::numeric) as median_problem_first_response_time_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by median((case when problem_level_stats.first_response_time_mad = 0 then 0 else ((problem_logs.first_response_time::float / 1000) - problem_level_stats.first_response_time_med) / problem_level_stats.first_response_time_mad end)::numeric)) as median_problem_first_response_time_class_percentile,
avg(problem_logs.attempt_count) as average_problem_attempt_count, 
avg(case when problem_level_stats.attempt_count_stddev = 0 then 0 else (problem_logs.attempt_count - problem_level_stats.attempt_count_avg) / problem_level_stats.attempt_count_stddev end) as average_problem_attempt_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when problem_level_stats.attempt_count_stddev = 0 then 0 else (problem_logs.attempt_count - problem_level_stats.attempt_count_avg) / problem_level_stats.attempt_count_stddev end)) as average_problem_attempt_count_class_percentile, 
avg((problem_logs.first_action_type_id = 1)::int) as average_problem_answer_first, 
avg(case when problem_level_stats.answer_first_stddev = 0 then 0 else ((problem_logs.first_action_type_id = 1)::int - problem_level_stats.answer_first_avg) / problem_level_stats.answer_first_stddev end) as average_problem_answer_first_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when problem_level_stats.answer_first_stddev = 0 then 0 else ((problem_logs.first_action_type_id = 1)::int - problem_level_stats.answer_first_avg) / problem_level_stats.answer_first_stddev end)) as average_problem_answer_first_class_percentile, 
avg((problem_logs.discrete_score = 1)::int) as average_problem_correctness, 
avg(case when problem_level_stats.correctness_stddev = 0 then 0 else ((problem_logs.discrete_score = 1)::int - problem_level_stats.correctness_avg) / problem_level_stats.correctness_stddev end) as average_problem_correctness_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when problem_level_stats.correctness_stddev = 0 then 0 else ((problem_logs.discrete_score = 1)::int - problem_level_stats.correctness_avg) / problem_level_stats.correctness_stddev end)) as average_problem_correctness_class_percentile, 
avg((problem_logs.bottom_hint)::int) as average_problem_answer_given, 
avg(case when problem_level_stats.answer_given_stddev = 0 then 0 else ((problem_logs.bottom_hint)::int - problem_level_stats.answer_given_avg) / problem_level_stats.answer_given_stddev end) as average_problem_answer_given_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when problem_level_stats.answer_given_stddev = 0 then 0 else ((problem_logs.bottom_hint)::int - problem_level_stats.answer_given_avg) / problem_level_stats.answer_given_stddev end)) as average_problem_answer_given_class_percentile 
from student_data.problem_logs 
inner join input_alogs on input_alogs.assignment_log_id = problem_logs.assignment_log_id 
inner join student_data.assignment_logs on assignment_logs.id = problem_logs.assignment_log_id 
inner join student_xrefs on student_xrefs.xid = assignment_logs.user_xid 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
left join problem_level_stats on problem_level_stats.problem_id = problem_logs.id 
group by input_alogs.assignment_log_id, assignments.group_context_xid; 


-- Put the tables together

select * 
from assignment_level_features 
left join problem_level_features on assignment_level_features.assignment_log_id = problem_level_features.assignment_log_id_2
where assignment_log_id_2 is null; 

