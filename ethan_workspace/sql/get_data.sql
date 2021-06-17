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

drop table if exists experiment_target_alogs; 
create table experiment_target_alogs as 
select *, 
encode_ceri('PS', sequence_id) as target_sequence 
from ordered_alogs 
where log_order = 1 
and encode_ceri('PS', sequence_id) in ('PSAHQV'); 

drop table if exists experiment_input_alogs; 
create table experiment_input_alogs as 
select 
experiment_target_alogs.target_sequence, 
ordered_alogs.* 
from ordered_alogs 
inner join experiment_target_alogs on experiment_target_alogs.student_id = ordered_alogs.student_id and experiment_target_alogs.start_time > ordered_alogs.start_time; 

drop table if exists experiment_excluded_alogs; 
create table experiment_excluded_alogs as 
select 0 as assignment_log_id; 

drop table if exists remnant_target_alogs; 
create table remnant_target_alogs as 
select *, 
encode_ceri('PS', sequence_id) as target_sequence 
from ordered_alogs 
where log_order = 1 
and encode_ceri('PS', sequence_id) in ('PSA2H6G'); 

drop table if exists remnant_input_alogs; 
create table remnant_input_alogs as 
select 
remnant_target_alogs.target_sequence, 
ordered_alogs.* 
from ordered_alogs 
inner join remnant_target_alogs on remnant_target_alogs.student_id = ordered_alogs.student_id and remnant_target_alogs.start_time > ordered_alogs.start_time; 

drop table if exists remnant_excluded_alogs; 
create table remnant_excluded_alogs as 
select ordered_alogs.* 
from ordered_alogs 
inner join experiment_target_alogs on experiment_target_alogs.student_id = ordered_alogs.student_id; 

-- Assignment Level Features


drop table if exists assignment_level_agg_features cascade; 
create table assignment_level_agg_features as 
--with dt as 
--( 
--	select 
--	id, 
--	timestamp - lag(timestamp, 1) over (partition by assignment_log_id order by timestamp) as dt 
--	from student_data.assignment_actions 
--) 
select 
assignment_log_id, 
sum((action_defn_type_id = 2)::int) + 1 as session_count, 
count(distinct extract(doy from timestamp)) as day_count, 
sum((action_defn_type_id = 12 and path not like '%SP%')::int) as completed_problem_count 
--sum((action_defn_type_id != 2)::int * dt.dt) as time_on_task 
from student_data.assignment_actions 
--inner join dt on dt.id = assignment_actions.id 
where assignment_log_id not in (select assignment_log_id from remnant_excluded_alogs) 
group by assignment_log_id; 

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
--avg(assignment_level_agg_features.time_on_task) as time_on_task_avg, 
--coalesce(stddev(assignment_level_agg_features.time_on_task), 0) as time_on_task_stddev 
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
remnant_input_alogs.assignment_log_id, 
remnant_input_alogs.target_sequence, 
remnant_input_alogs.student_id, 
assignment_logs.start_time as assignment_start_time, 
row_number() over(partition by remnant_input_alogs.student_id order by assignment_logs.start_time) as assignment_order, 
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
--assignment_level_agg_features.time_on_task as time_on_task_raw, 
--case when assignment_level_stats.time_on_task_stddev = 0 then 0 else (assignment_level_agg_features.time_on_task - assignment_level_stats.time_on_task_avg) / assignment_level_stats.time_on_task_stddev end as time_on_task_normalized, 
--percent_rank() over (partition by assignments.group_context_xid order by case when assignment_level_stats.time_on_task_stddev = 0 then 0 else (assignment_level_agg_features.time_on_task - assignment_level_stats.time_on_task_avg) / assignment_level_stats.time_on_task_stddev end) as time_on_task_class_percentile, 
assignment_level_agg_features.completed_problem_count as completed_problem_count_raw, 
case when assignment_level_stats.completed_problem_count_stddev = 0 then 0 else (assignment_level_agg_features.completed_problem_count - assignment_level_stats.completed_problem_count_avg) / assignment_level_stats.completed_problem_count_stddev end as completed_problem_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by case when assignment_level_stats.completed_problem_count_stddev = 0 then 0 else (assignment_level_agg_features.completed_problem_count - assignment_level_stats.completed_problem_count_avg) / assignment_level_stats.completed_problem_count_stddev end) as completed_problem_count_class_percentile 
from student_data.assignment_logs 
inner join remnant_input_alogs on remnant_input_alogs.assignment_log_id = assignment_logs.id 
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
	where problem_logs.end_time is not null 
	and problem_logs.first_response_time is not null 
	and problem_logs.path_info not like '%SP%' 
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
coalesce(avg((problem_logs.discrete_score = 1)::int), 1) as correctness_avg, 
coalesce(stddev((problem_logs.discrete_score = 1)::int), 0) as correctness_stddev, 
avg((problem_logs.bottom_hint)::int) as answer_given_avg, 
coalesce(stddev((problem_logs.bottom_hint)::int), 0) as answer_given_stddev 
from student_data.problem_logs 
inner join medians on medians.pid = problem_logs.problem_id 
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
and problem_logs.assignment_log_id not in (select assignment_log_id from remnant_excluded_alogs) 
group by problem_id, medians.time_on_task_med, medians.first_response_time_med; 

drop table if exists problem_level_features cascade; 
create table problem_level_features as 
select 
remnant_input_alogs.assignment_log_id as assignment_log_id_2, 
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
coalesce(avg((problem_logs.discrete_score = 1)::int), 1) as average_problem_correctness, 
coalesce(avg(case when problem_level_stats.correctness_stddev = 0 then 0 else ((problem_logs.discrete_score = 1)::int - problem_level_stats.correctness_avg) / problem_level_stats.correctness_stddev end), 0) as average_problem_correctness_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when problem_level_stats.correctness_stddev = 0 then 0 else ((problem_logs.discrete_score = 1)::int - problem_level_stats.correctness_avg) / problem_level_stats.correctness_stddev end)) as average_problem_correctness_class_percentile, 
avg((problem_logs.bottom_hint)::int) as average_problem_answer_given, 
avg(case when problem_level_stats.answer_given_stddev = 0 then 0 else ((problem_logs.bottom_hint)::int - problem_level_stats.answer_given_avg) / problem_level_stats.answer_given_stddev end) as average_problem_answer_given_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when problem_level_stats.answer_given_stddev = 0 then 0 else ((problem_logs.bottom_hint)::int - problem_level_stats.answer_given_avg) / problem_level_stats.answer_given_stddev end)) as average_problem_answer_given_class_percentile 
from student_data.problem_logs 
inner join remnant_input_alogs on remnant_input_alogs.assignment_log_id = problem_logs.assignment_log_id 
inner join student_data.assignment_logs on assignment_logs.id = problem_logs.assignment_log_id 
inner join student_xrefs on student_xrefs.xid = assignment_logs.user_xid 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join problem_level_stats on problem_level_stats.problem_id = problem_logs.problem_id 
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by remnant_input_alogs.assignment_log_id, assignments.group_context_xid; 


-- Check the tables together

select 
count(*) - count(problem_id) as problem_id,
count(*) - count(time_on_task_med) as time_on_task_med,
count(*) - count(time_on_task_mad) as time_on_task_mad,
count(*) - count(first_response_time_med) as first_response_time_med,
count(*) - count(first_response_time_mad) as first_response_time_mad,
count(*) - count(attempt_count_avg) as attempt_count_avg,
count(*) - count(attempt_count_stddev) as attempt_count_stddev,
count(*) - count(answer_first_avg) as answer_first_avg,
count(*) - count(answer_first_stddev) as answer_first_stddev,
count(*) - count(correctness_avg) as correctness_avg,
count(*) - count(correctness_stddev) as correctness_stddev,
count(*) - count(answer_given_avg) as answer_given_avg,
count(*) - count(answer_given_stddev) as answer_given_stddev
from problem_level_stats;

select 
count(*) - count(assignment_log_id_2) as assignment_log_id_2,
count(*) - count(median_problem_time_on_task_raw) as median_problem_time_on_task_raw,
count(*) - count(median_problem_time_on_task_normalized) as median_problem_time_on_task_normalized,
count(*) - count(median_problem_time_on_task_class_percentile) as median_problem_time_on_task_class_percentile,
count(*) - count(median_problem_first_response_time_raw) as median_problem_first_response_time_raw,
count(*) - count(median_problem_first_response_time_normalized) as median_problem_first_response_time_normalized,
count(*) - count(median_problem_first_response_time_class_percentile) as median_problem_first_response_time_class_percentile,
count(*) - count(average_problem_attempt_count) as average_problem_attempt_count,
count(*) - count(average_problem_attempt_count_normalized) as average_problem_attempt_count_normalized,
count(*) - count(average_problem_attempt_count_class_percentile) as average_problem_attempt_count_class_percentile,
count(*) - count(average_problem_answer_first) as average_problem_answer_first,
count(*) - count(average_problem_answer_first_normalized) as average_problem_answer_first_normalized,
count(*) - count(average_problem_answer_first_class_percentile) as average_problem_answer_first_class_percentile,
count(*) - count(average_problem_correctness) as average_problem_correctness,
count(*) - count(average_problem_correctness_normalized) as average_problem_correctness_normalized,
count(*) - count(average_problem_correctness_class_percentile) as average_problem_correctness_class_percentile,
count(*) - count(average_problem_answer_given) as average_problem_answer_given,
count(*) - count(average_problem_answer_given_normalized) as average_problem_answer_given_normalized,
count(*) - count(average_problem_answer_given_class_percentile) as average_problem_answer_given_class_percentile
from problem_level_features



-- Put the tables together

select * from assignment_level_features 