-- Views to help create logs

create or replace view first_alogs as 
with log_orders as 
( 
	select assignment_logs.*, row_number() over(partition by assignment_logs.user_xid, assignments.sequence_id order by assignment_logs.start_time) as log_order 
	from student_data.assignment_logs 
	inner join public.assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
	inner join core.assignments on assignments.id = assignment_xrefs.id 
	where assignments.sequence_id = decode_ceri('epsvar') 
) 
select * from log_orders 
where log_order = 1; 



-- Student Logs

drop table if exists exp_slogs cascade; 
create table exp_slogs as 
select encode_ceri('PS', assignments.sequence_id) as experiment_id, 
student_xrefs.id as student_id, 
case when problems.assistment_id is not null then encode_ceri('PR', problems.assistment_id) else null end as problem_id, 
problems.position as problem_part, 
case when substring(assignment_actions.path || '/', 'SP(.*?)/')::int is not null then substring(assignment_actions.path || '/', 'SP(.*?)/')::int else null end as scaffold_id, 
btrim(concat(s1.name, '/', s2.name, '/', s3.name, '/', s4.name, '/', s5.name), '/') as experiment_tag_path, 
case when student_action_map.action = 'closed_response' and action_responses.correct is not null and action_responses.correct = True then 'correct_response' 
when student_action_map.action = 'closed_response' then 'wrong_response' 
else student_action_map.action end as action, 
assignment_actions.timestamp, 
assignment_actions.id as assistments_reference_action_log_id 
from student_data.assignment_actions 
inner join public.first_alogs on first_alogs.id = assignment_actions.assignment_log_id 
inner join public.student_xrefs on student_xrefs.xid = first_alogs.user_xid 
inner join public.assignment_xrefs on assignment_xrefs.xid = first_alogs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join public.student_action_map on student_action_map.raw_action = assignment_actions.action_defn_type_id 
left join student_data.problem_actions on problem_actions.id = assignment_actions.action_id 
left join student_data.action_responses on action_responses.id = problem_actions.action_details_id 
left join legacy.problems on problems.id = substring(assignment_actions.path || '/', '#(.*?)/')::int 
left join legacy.sections as s1 on s1.id = ('0' || split_part(btrim(replace(split_part(assignment_actions.path, 'LPR', 1), 'LPS', ''), '/'), '/', 1))::int 
left join legacy.sections as s2 on s2.id = ('0' || split_part(btrim(replace(split_part(assignment_actions.path, 'LPR', 1), 'LPS', ''), '/'), '/', 2))::int 
left join legacy.sections as s3 on s3.id = ('0' || split_part(btrim(replace(split_part(assignment_actions.path, 'LPR', 1), 'LPS', ''), '/'), '/', 3))::int 
left join legacy.sections as s4 on s4.id = ('0' || split_part(btrim(replace(split_part(assignment_actions.path, 'LPR', 1), 'LPS', ''), '/'), '/', 4))::int 
left join legacy.sections as s5 on s5.id = ('0' || split_part(btrim(replace(split_part(assignment_actions.path, 'LPR', 1), 'LPS', ''), '/'), '/', 5))::int 
order by student_id, timestamp; 



-- Views to help create Problem Logs

create or replace view problem_tot as 
select student_id, problem_id, problem_part, null as scaffold_id, extract(epoch from sum(dt)) as tot 
from 
(
	select *, lag(timestamp, -1) over (partition by student_id, problem_id, problem_part order by timestamp) - timestamp as dt 
	from public.exp_slogs 
	
) as deltat 
where problem_id is not null 
and action != 'assignment_resumed' 
group by student_id, problem_id, problem_part 
union all 
select student_id, problem_id, problem_part, scaffold_id, extract(epoch from sum(dt)) as tot 
from 
(
	select *, lag(timestamp, -1) over (partition by student_id, problem_id, problem_part order by timestamp) - timestamp as dt 
	from public.exp_slogs 
	
) as deltat 
where problem_id is not null 
and scaffold_id is not null 
and action != 'assignment_resumed' 
group by student_id, problem_id, problem_part, scaffold_id; 


create or replace view problem_session_count as 
select student_id, problem_id, problem_part, null as scaffold_id, count(*) + 1 as count 
from 
(
	select *, lag(action, 1) over (partition by student_id order by timestamp) as previous_action 
	from public.exp_slogs 
) slogs 
where previous_action = 'assignment_resumed' 
group by student_id, problem_id, problem_part 
union all 
select student_id, problem_id, problem_part, scaffold_id, count(*) + 1 as count 
from 
(
	select *, lag(action, 1) over (partition by student_id order by timestamp) as previous_action 
	from public.exp_slogs 
) slogs 
where previous_action = 'assignment_resumed' 
and scaffold_id is not null 
group by student_id, problem_id, problem_part, scaffold_id; 


create or replace view problem_hint_total as 
select problem_id, count(*) as hint_total 
from legacy.tutor_strategies 
where content_type = 'Hint' 
and enabled = true 
group by problem_id; 


create or replace view problem_scaf_total as 
select problem_id, scaffold_count.count as scaf_total 
from legacy.problems 
inner join legacy.tutor_strategies on tutor_strategies.problem_id = problems.id 
inner join legacy.scaffolds on scaffolds.tutor_strategy_id = tutor_strategies.id 
inner join (select scaffold_id, count(*) from legacy.problems group by scaffold_id) scaffold_count on scaffold_count.scaffold_id = scaffolds.id;


create or replace view student_scaf_count as 
select student_id, problem_id, problem_part, count(distinct scaffold_id) as scaf_count 
from exp_slogs 
group by student_id, problem_id, problem_part; 


create or replace view problem_has_exp as 
select distinct problem_id 
from legacy.tutor_strategies 
where content_type = 'Explanation' 
and enabled = true; 


create or replace view student_saw_exp as 
select distinct student_id, problem_id, problem_part, scaffold_id 
from exp_slogs 
where action = 'explanation_requested'; 


create or replace view problem_condition as 
select distinct student_id, problem_id, problem_part, 
case 
when char_length(experiment_tag_path) = 0 
then 'Unknown' 
when ((char_length(experiment_tag_path) - position(reverse('[ignore]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path) > (char_length(experiment_tag_path) - position(reverse('[pretest]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path)) 
and ((char_length(experiment_tag_path) - position(reverse('[ignore]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path) > (char_length(experiment_tag_path) - position(reverse('[posttest]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path)) 
and ((char_length(experiment_tag_path) - position(reverse('[ignore]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path) > (char_length(experiment_tag_path) - position(reverse('[control') in reverse(experiment_tag_path))) % char_length(experiment_tag_path)) 
and ((char_length(experiment_tag_path) - position(reverse('[ignore]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path) > (char_length(experiment_tag_path) - position(reverse('[treatment') in reverse(experiment_tag_path))) % char_length(experiment_tag_path)) 
then 'Nonexperimental' 
when ((char_length(experiment_tag_path) - position(reverse('[pretest]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path) > (char_length(experiment_tag_path) - position(reverse('[posttest]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path)) 
and ((char_length(experiment_tag_path) - position(reverse('[pretest]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path) > (char_length(experiment_tag_path) - position(reverse('[control') in reverse(experiment_tag_path))) % char_length(experiment_tag_path)) 
and ((char_length(experiment_tag_path) - position(reverse('[pretest]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path) > (char_length(experiment_tag_path) - position(reverse('[treatment') in reverse(experiment_tag_path))) % char_length(experiment_tag_path)) 
then 'Pretest' 
when ((char_length(experiment_tag_path) - position(reverse('[posttest]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path) > (char_length(experiment_tag_path) - position(reverse('[control') in reverse(experiment_tag_path))) % char_length(experiment_tag_path)) 
and ((char_length(experiment_tag_path) - position(reverse('[posttest]') in reverse(experiment_tag_path))) % char_length(experiment_tag_path) > (char_length(experiment_tag_path) - position(reverse('[treatment') in reverse(experiment_tag_path))) % char_length(experiment_tag_path)) 
then 'Posttest' 
when ((char_length(experiment_tag_path) - position(reverse('[control') in reverse(experiment_tag_path))) % char_length(experiment_tag_path) > (char_length(experiment_tag_path) - position(reverse('[treatment') in reverse(experiment_tag_path))) % char_length(experiment_tag_path)) 
then concat('Control', ' ' || substring(lower(experiment_tag_path), '\[control(.+)\]')) 
when ((char_length(experiment_tag_path) - position(reverse('[treatment') in reverse(experiment_tag_path))) % char_length(experiment_tag_path) > 0) 
then concat('Treatment', ' ' || substring(lower(experiment_tag_path), '\[treatment(.+)\]')) 
else 'Unknown' end as problem_condition 
from public.exp_slogs 
where problem_id is not null; 


-- Problem Logs

drop table if exists exp_plogs cascade; 
create table exp_plogs as 
select encode_ceri('PS', assignments.sequence_id) as experiment_id, 
student_xrefs.id as student_id, 
encode_ceri('PR', problems.assistment_id) as problem_id, 
main_problems.position as problem_part, 
case when substring(problem_logs.path_info || '/', 'SP(.*?)/')::int is not null then problem_logs.problem_id end as scaffold_id, 
problem_condition.problem_condition, 
problem_logs.start_time, 
problem_logs.end_time, 
coalesce(problem_session_count.count, 1) as session_count, 
problem_tot.tot as time_on_task, 
problem_logs.first_response_time::float / 1000 as first_response_or_request_time, 
case when problems.problem_type_id = 8 then 'REDACTED' else problem_logs.answer_text end as first_answer, 
problem_logs.discrete_score = 1 as correct, 
problem_logs.continuous_score as reported_score, 
problem_logs.first_action_type_id = 1 as answer_before_tutoring, 
problem_logs.attempt_count as attempt_count, 
coalesce(problem_hint_total.hint_total, 0) as hints_available, 
greatest(problem_logs.hint_count - 1, 0) as hints_given, 
coalesce(problem_scaf_total.scaf_total, 0) as scaffold_problems_available, 
coalesce(student_scaf_count.scaf_count, 0) as scaffold_problems_given, 
problem_has_exp.problem_id is not null as explanation_available, 
student_saw_exp.problem_id is not null as explanation_given, 
problem_logs.bottom_hint as answer_given, 
problem_logs.id as assistments_reference_problem_log_id 
from student_data.problem_logs 
inner join public.first_alogs on first_alogs.id = problem_logs.assignment_log_id 
inner join public.student_xrefs on student_xrefs.xid = first_alogs.user_xid 
inner join public.assignment_xrefs on assignment_xrefs.xid = first_alogs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
left join legacy.problems on problems.id = problem_logs.problem_id 
left join legacy.problems as main_problems on main_problems.id = substring(problem_logs.path_info || '/', '#(.*?)/')::int 
left join public.problem_hint_total on problem_hint_total.problem_id = problem_logs.problem_id 
left join public.problem_scaf_total on problem_scaf_total.problem_id = problem_logs.problem_id 
left join public.problem_tot on problem_tot.student_id = student_xrefs.id and problem_tot.problem_id = encode_ceri('PR', main_problems.assistment_id) and problem_tot.problem_part = main_problems.position and ((problem_tot.scaffold_id is null and problem_logs.path_info not ilike '%SP%') or problem_tot.scaffold_id = problems.id) 
left join public.problem_session_count on problem_session_count.student_id = student_xrefs.id and problem_session_count.problem_id = encode_ceri('PR', main_problems.assistment_id) and problem_session_count.problem_part = main_problems.position and ((problem_session_count.scaffold_id is null and problem_logs.path_info not ilike '%SP%') or problem_session_count.scaffold_id = problems.id) 
left join public.problem_has_exp on problem_has_exp.problem_id = problem_logs.problem_id 
left join public.student_saw_exp on student_saw_exp.student_id = student_xrefs.id and student_saw_exp.problem_id = encode_ceri('PR', main_problems.assistment_id) and student_saw_exp.problem_part = main_problems.position and ((student_saw_exp.scaffold_id is null and problem_logs.path_info not ilike '%SP%') or student_saw_exp.scaffold_id = problems.id) 
left join public.student_scaf_count on student_scaf_count.student_id = student_xrefs.id and student_scaf_count.problem_id = encode_ceri('PR', main_problems.assistment_id) and student_scaf_count.problem_part = main_problems.position and problem_logs.path_info not ilike '%SP%' 
left join public.problem_condition on problem_condition.student_id = student_xrefs.id and problem_condition.problem_id = encode_ceri('PR', main_problems.assistment_id) and problem_condition.problem_part = main_problems.position 
order by student_id, start_time; 



-- Views to help create Assignment Logs

create or replace view assigned_condition as 
select distinct student_id, problem_condition as assigned_condition 
from public.exp_plogs 
where problem_condition ilike '%Control%' 
or problem_condition ilike '%Treatment%'; 


create or replace view assignment_session_count as 
select student_id, count(*) + 1 as count 
from 
(
	select *, lag(action, 1) over (partition by student_id order by timestamp) as previous_action 
	from public.exp_slogs 
) slogs 
where previous_action = 'assignment_resumed' 
group by student_id; 


create or replace view pretest_session_count as 
select problem_condition.student_id, count(*) + 1 as count 
from public.problem_session_count 
left join public.problem_condition on problem_condition.student_id = problem_session_count.student_id and problem_condition.problem_id = problem_session_count.problem_id and problem_condition.problem_part = problem_session_count.problem_part 
where problem_condition.problem_condition = 'Pretest' 
group by problem_condition.student_id; 


create or replace view posttest_session_count as 
select problem_condition.student_id, count(*) + 1 as count 
from public.problem_session_count 
left join public.problem_condition on problem_condition.student_id = problem_session_count.student_id and problem_condition.problem_id = problem_session_count.problem_id and problem_condition.problem_part = problem_session_count.problem_part 
where problem_condition.problem_condition = 'Posttest' 
group by problem_condition.student_id; 


create or replace view condition_session_count as 
select problem_condition.student_id, count(*) + 1 as count 
from public.problem_session_count 
left join public.problem_condition on problem_condition.student_id = problem_session_count.student_id and problem_condition.problem_id = problem_session_count.problem_id and problem_condition.problem_part = problem_session_count.problem_part 
where problem_condition.problem_condition ilike '%Control%' or problem_condition.problem_condition ilike '%Treatment%' 
group by problem_condition.student_id; 


create or replace view pretest_stats as 
select student_id, 
count(*) as pretest_problem_count, 
sum(correct::int) as pretest_correct, 
sum(time_on_task) as pretest_time_on_task, 
avg(first_response_or_request_time) as pretest_average_first_response_time 
from public.exp_plogs 
where problem_condition = 'Pretest' 
and scaffold_id is null 
group by student_id; 


create or replace view posttest_stats as 
select student_id, 
count(*) as posttest_problem_count, 
sum(correct::int) as posttest_correct, 
sum(time_on_task) as posttest_time_on_task, 
avg(first_response_or_request_time) as posttest_average_first_response_time 
from public.exp_plogs 
where problem_condition = 'Posttest' 
and scaffold_id is null 
group by student_id;


create or replace view condition_stats as 
select student_id, 
sum(time_on_task) as condition_time_on_task, 
avg(first_response_or_request_time) as condition_average_first_response_or_request_time, 
count(distinct problem_id) as condition_problem_count, 
sum(correct::int) as condition_total_correct, 
sum(answer_before_tutoring::int) as condition_total_answers_before_tutoring, 
sum(attempt_count) as condition_total_attempt_count, 
sum(hints_available) as condition_total_hints_available, 
sum(hints_given) as condition_total_hints_given, 
sum(scaffold_problems_available) as condition_total_scaffold_problems_available, 
sum(scaffold_problems_given) as condition_total_scaffold_problems_given, 
sum(explanation_available::int) as condition_total_explanations_available, 
sum(explanation_given::int) as condition_total_explanations_given, 
sum(answer_given::int) as condition_total_answers_given 
from public.exp_plogs 
where problem_condition ilike '%Control%' or problem_condition ilike '%Treatment%' 
and scaffold_id is null 
group by student_id;


create or replace view npc_wrong as 
select student_id, sum(next_problem_correct::int) as condition_total_correct_after_wrong_response 
from 
(
	select *, lag(correct, -1) over (partition by student_id order by start_time) as next_problem_correct 
	from (
		select * from public.exp_plogs 
		where problem_condition ilike '%Control%' or problem_condition ilike '%Treatment%' 
		and scaffold_id is null
	) filtered_plogs
) plogs 
where correct = false 
group by student_id; 


create or replace view npc_tutoring as 
select student_id, sum(next_problem_correct::int) as condition_total_correct_after_tutoring 
from 
(
	select *, lag(correct, -1) over (partition by student_id order by start_time) as next_problem_correct 
	from (
		select * from public.exp_plogs 
		where problem_condition ilike '%Control%' or problem_condition ilike '%Treatment%' 
		and scaffold_id is null
	) filtered_plogs
) plogs 
where hints_given > 0 
or scaffold_problems_given > 0 
or explanation_given = true 
group by student_id; 


-- Assignment Logs

drop table if exists exp_alogs cascade; 
create table exp_alogs as 
select encode_ceri('PS', assignments.sequence_id) as experiment_id, 
student_xrefs.id as student_id, 
assignments.release_date, 
assignments.due_date, 
first_alogs.start_time, 
first_alogs.end_time, 
coalesce(assignment_session_count.count, 1) as assignment_session_count, 
pretest_stats.pretest_problem_count, 
pretest_stats.pretest_correct, 
pretest_stats.pretest_time_on_task, 
pretest_stats.pretest_average_first_response_time, 
case when pretest_stats.pretest_problem_count is null then null else coalesce(pretest_session_count.count, 1) end as pretest_session_count, 
coalesce(assigned_condition.assigned_condition, 'Not Assigned') as assigned_condition, 
condition_stats.condition_time_on_task, 
condition_stats.condition_average_first_response_or_request_time, 
condition_stats.condition_problem_count, 
condition_stats.condition_total_correct, 
npc_wrong.condition_total_correct_after_wrong_response, 
npc_tutoring.condition_total_correct_after_tutoring, 
condition_stats.condition_total_answers_before_tutoring, 
condition_stats.condition_total_attempt_count, 
condition_stats.condition_total_hints_available, 
condition_stats.condition_total_hints_given, 
condition_stats.condition_total_scaffold_problems_available, 
condition_stats.condition_total_scaffold_problems_given, 
condition_stats.condition_total_explanations_available, 
condition_stats.condition_total_explanations_given, 
condition_stats.condition_total_answers_given, 
case when assigned_condition.assigned_condition is null then null else coalesce(condition_session_count.count, 1) end as condition_session_count, 
posttest_stats.posttest_problem_count, 
posttest_stats.posttest_correct, 
posttest_stats.posttest_time_on_task, 
posttest_stats.posttest_average_first_response_time, 
case when posttest_stats.posttest_problem_count is null then null else coalesce(posttest_session_count.count, 1) end as posttest_session_count, 
first_alogs.id as assistments_reference_assignment_log_id 
from public.first_alogs 
inner join public.student_xrefs on student_xrefs.xid = first_alogs.user_xid 
inner join public.assignment_xrefs on assignment_xrefs.xid = first_alogs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
left join public.assigned_condition on assigned_condition.student_id = student_xrefs.id 
left join public.assignment_session_count on assignment_session_count.student_id = student_xrefs.id 
left join public.pretest_session_count on pretest_session_count.student_id = student_xrefs.id 
left join public.condition_session_count on condition_session_count.student_id = student_xrefs.id 
left join public.posttest_session_count on posttest_session_count.student_id = student_xrefs.id 
left join public.pretest_stats on pretest_stats.student_id = student_xrefs.id 
left join public.posttest_stats on posttest_stats.student_id = student_xrefs.id 
left join public.condition_stats on condition_stats.student_id = student_xrefs.id 
left join public.npc_wrong on npc_wrong.student_id = student_xrefs.id 
left join public.npc_tutoring on npc_tutoring.student_id = student_xrefs.id 
order by student_id, start_time; 



-- Table for skill_builder vs problem_set

drop table if exists assignment_type cascade; 
create table assignment_type as 
select assignments.id as assignment_id, 
case when sequences.parameters like '%pseudo_skill_builder%' or sections.type='MasterySection' or sections.type='LinearMasterySection' then 'skill_builder' else 'problem_set' end as assignment_type 
from core.assignments 
left join legacy.sequences on sequences.id = assignments.sequence_id 
left join legacy.sections on sections.id = sequences.head_section_id; 


-- Views to help create Teacher Priors 

drop table if exists teacher_first_start cascade; 
create table teacher_first_start as 
with assignment_orders as 
(
	select teacher_xrefs.id, assignments.release_date, row_number() over(partition by teacher_xrefs.id order by assignments.release_date) as log_order 
	from public.first_alogs 
	inner join public.assignment_xrefs on assignment_xrefs.xid = first_alogs.assignment_xid 
	inner join core.assignments on assignments.id = assignment_xrefs.id 
	inner join public.teacher_xrefs on teacher_xrefs.xid = assignments.owner_xid 
)
select id as teacher_id, release_date 
from assignment_orders 
where log_order = 1; 


create or replace view teacher_stats as 
select distinct users.id as teacher_id, 
users.created as teacher_account_creation_date, 
substring(users.username, '.*@(.*)') as district_alias 
from public.first_alogs 
inner join public.assignment_xrefs on assignment_xrefs.xid = first_alogs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join public.teacher_xrefs on teacher_xrefs.xid = assignments.owner_xid 
inner join users.users on users.id = teacher_xrefs.id; 


-- Views to help create Class Priors

drop table if exists class_first_start cascade; 
create table class_first_start as 
with assignment_orders as 
(
	select class_xrefs.id, assignments.release_date, row_number() over(partition by class_xrefs.id order by assignments.release_date) as log_order 
	from public.first_alogs 
	inner join public.assignment_xrefs on assignment_xrefs.xid = first_alogs.assignment_xid 
	inner join core.assignments on assignments.id = assignment_xrefs.id 
	inner join public.class_xrefs on class_xrefs.xid = assignments.group_context_xid 
)
select id as class_id, release_date 
from assignment_orders 
where log_order = 1; 


create or replace view class_student_count as 
select principal_group_memberships.group_id as class_id, 
count(*) as class_student_count 
from groups.principal_group_memberships 
where principal_group_memberships.group_member_type_id = 1 
and principal_group_memberships.member_id in (select id from public.student_xrefs) 
group by principal_group_memberships.group_id; 


create or replace view class_assignment_counts as 
select class_xrefs.id as class_id, 
sum((assignment_type.assignment_type = 'skill_builder')::int) as class_prior_skill_builder_count, 
sum((assignment_type.assignment_type = 'problem_set')::int) as class_prior_problem_set_count 
from core.assignments 
inner join public.class_xrefs on class_xrefs.xid = assignments.group_context_xid 
inner join public.class_first_start on class_first_start.class_id = class_xrefs.id and class_first_start.release_date > assignments.release_date 
inner join public.assignment_type on assignment_type.assignment_id = assignments.id
group by class_xrefs.id; 


create or replace view class_completion as 
with log_orders as 
(
	select assignment_logs.*, assignment_type.assignment_type, row_number() over(partition by assignment_logs.user_xid, assignment_logs.assignment_xid order by assignment_logs.start_time) as log_order 
	from student_data.assignment_logs 
	inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
	inner join assignment_type on assignment_type.assignment_id = assignment_xrefs.id 
)
select class_xrefs.id as class_id, 
sum((log_orders.start_time is not null and log_orders.assignment_type = 'skill_builder')::int) as skill_builders_started, 
sum((log_orders.end_time is not null and log_orders.assignment_type = 'skill_builder')::int) as skill_builders_finished, 
sum((log_orders.start_time is not null and log_orders.assignment_type = 'problem_set')::int) as problem_sets_started, 
sum((log_orders.end_time is not null and log_orders.assignment_type = 'problem_set')::int) as problem_sets_finished 
from log_orders 
inner join public.assignment_xrefs on assignment_xrefs.xid = log_orders.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join public.class_xrefs on class_xrefs.xid = assignments.group_context_xid 
inner join public.class_first_start on class_first_start.class_id = class_xrefs.id and class_first_start.release_date > log_orders.start_time 
where log_order = 1 
group by class_xrefs.id; 


create or replace view class_problem_stats as 
select class_first_start.class_id, 
count(*) as class_prior_completed_problem_count, 
median(extract(epoch from (problem_logs.end_time - problem_logs.start_time))::numeric) as class_prior_median_time_on_task, 
median((problem_logs.first_response_time::float / 1000)::numeric) as class_prior_median_first_response_time, 
avg(problem_logs.discrete_score) as class_prior_average_correctness, 
avg(problem_logs.attempt_count) as class_prior_average_attempt_count 
from student_data.problem_logs 
inner join student_data.assignment_logs on assignment_logs.id = problem_logs.assignment_log_id 
inner join public.assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join public.class_xrefs on class_xrefs.xid = assignments.group_context_xid 
inner join public.class_first_start on class_first_start.class_id = class_xrefs.id and class_first_start.release_date > problem_logs.end_time 
group by class_first_start.class_id; 


create or replace view class_stats as 
select class_first_start.class_id, 
group_definitions.created_at as class_creation_date, 
class_student_count.class_student_count, 
class_assignment_counts.class_prior_skill_builder_count, 
class_assignment_counts.class_prior_problem_set_count, 
case when class_assignment_counts.class_prior_skill_builder_count != 0 then class_completion.skill_builders_started::float / (class_student_count.class_student_count::float * class_assignment_counts.class_prior_skill_builder_count::float) * 100 else 0 end as class_prior_skill_builder_percent_started, 
case when class_completion.skill_builders_started != 0 then class_completion.skill_builders_finished::float / class_completion.skill_builders_started::float * 100 else 0 end as class_prior_skill_builder_percent_completed, 
case when class_assignment_counts.class_prior_problem_set_count != 0 then class_completion.problem_sets_started::float / (class_student_count.class_student_count::float * class_assignment_counts.class_prior_problem_set_count::float) * 100 else 0 end as class_prior_problem_set_percent_started, 
case when class_completion.problem_sets_started != 0 then class_completion.problem_sets_finished::float / class_completion.problem_sets_started::float * 100 else 0 end as class_prior_problem_set_percent_completed, 
class_problem_stats.class_prior_completed_problem_count, 
class_problem_stats.class_prior_median_time_on_task, 
class_problem_stats.class_prior_median_first_response_time, 
class_problem_stats.class_prior_average_correctness, 
class_problem_stats.class_prior_average_attempt_count 
from public.class_first_start 
left join public.class_student_count on class_student_count.class_id = class_first_start.class_id 
left join public.class_assignment_counts on class_assignment_counts.class_id = class_first_start.class_id 
left join public.class_completion on class_completion.class_id = class_first_start.class_id 
left join public.class_problem_stats on class_problem_stats.class_id = class_first_start.class_id 
left join groups.group_definitions on group_definitions.id = class_first_start.class_id; 


-- Views to help create Student Priors

create or replace view student_completion as 
with log_orders as 
(
	select assignment_logs.*, assignment_type.assignment_type, row_number() over(partition by assignment_logs.user_xid, assignment_logs.assignment_xid order by assignment_logs.start_time) as log_order 
	from student_data.assignment_logs 
	inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
	inner join assignment_type on assignment_type.assignment_id = assignment_xrefs.id 
)
select student_xrefs.id as student_id, 
sum((log_orders.start_time is not null and log_orders.assignment_type = 'skill_builder')::int) as skill_builders_started, 
sum((log_orders.end_time is not null and log_orders.assignment_type = 'skill_builder')::int) as skill_builders_finished, 
sum((log_orders.start_time is not null and log_orders.assignment_type = 'problem_set')::int) as problem_sets_started, 
sum((log_orders.end_time is not null and log_orders.assignment_type = 'problem_set')::int) as problem_sets_finished 
from log_orders 
inner join public.student_xrefs on student_xrefs.xid = log_orders.user_xid 
inner join public.exp_alogs on exp_alogs.student_id = student_xrefs.id and exp_alogs.start_time > log_orders.start_time 
where log_order = 1 
group by student_xrefs.id; 


create or replace view student_stats as 
select exp_alogs.student_id, 
student_completion.skill_builders_started as student_prior_started_skill_builder_count, 
student_completion.skill_builders_finished as student_prior_completed_skill_builder_count, 
student_completion.problem_sets_started as student_prior_started_problem_set_count, 
student_completion.problem_sets_finished as student_prior_completed_problem_set_count, 
count(*) as student_prior_completed_problem_count, 
median(extract(epoch from (problem_logs.end_time - problem_logs.start_time))::numeric) as student_prior_median_time_on_task, 
median((problem_logs.first_response_time::float / 1000)::numeric) as student_prior_median_first_response_time, 
avg(problem_logs.discrete_score) as student_prior_average_correctness, 
avg(problem_logs.attempt_count) as student_prior_average_attempt_count 
from student_data.problem_logs 
inner join student_data.assignment_logs on assignment_logs.id = problem_logs.assignment_log_id 
inner join public.student_xrefs on student_xrefs.xid = assignment_logs.user_xid 
inner join public.assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join public.exp_alogs on exp_alogs.student_id = student_xrefs.id and exp_alogs.start_time > problem_logs.end_time 
left join public.student_completion on student_completion.student_id = student_xrefs.id 
group by exp_alogs.student_id, student_completion.skill_builders_started, student_completion.skill_builders_finished, student_completion.problem_sets_started, student_completion.problem_sets_finished; 



-- Priors
drop table if exists priors cascade; 
create table priors as 
select encode_ceri('PS', assignments.sequence_id) as experiment_id, 
student_xrefs.id as student_id, 
coalesce(student_stats.student_prior_started_skill_builder_count, 0) as student_prior_started_skill_builder_count, 
coalesce(student_stats.student_prior_completed_skill_builder_count, 0) as student_prior_completed_skill_builder_count, 
coalesce(student_stats.student_prior_started_problem_set_count, 0) as student_prior_started_problem_set_count, 
coalesce(student_stats.student_prior_completed_problem_set_count, 0) as student_prior_completed_problem_set_count, 
coalesce(student_stats.student_prior_completed_problem_count, 0) as student_prior_completed_problem_count, 
student_stats.student_prior_median_first_response_time, 
student_stats.student_prior_median_time_on_task, 
student_stats.student_prior_average_correctness, 
student_stats.student_prior_average_attempt_count, 
class_stats.class_id, 
class_stats.class_creation_date, 
class_stats.class_student_count, 
coalesce(class_stats.class_prior_skill_builder_count, 0) as class_prior_skill_builder_count, 
coalesce(class_stats.class_prior_problem_set_count, 0) as class_prior_problem_set_count, 
class_stats.class_prior_skill_builder_percent_started, 
class_stats.class_prior_skill_builder_percent_completed, 
class_stats.class_prior_problem_set_percent_started, 
class_stats.class_prior_problem_set_percent_completed, 
coalesce(class_stats.class_prior_completed_problem_count, 0) as class_prior_completed_problem_count, 
class_stats.class_prior_median_time_on_task, 
class_stats.class_prior_median_first_response_time, 
class_stats.class_prior_average_correctness, 
class_stats.class_prior_average_attempt_count, 
teacher_stats.* 
from first_alogs 
inner join public.student_xrefs on student_xrefs.xid = first_alogs.user_xid 
inner join public.assignment_xrefs on assignment_xrefs.xid = first_alogs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join public.class_xrefs on class_xrefs.xid = assignments.group_context_xid 
inner join public.teacher_xrefs on teacher_xrefs.xid = assignments.owner_xid 
left join public.student_stats on student_stats.student_id = student_xrefs.id 
left join public.class_stats on class_stats.class_id = class_xrefs.id 
left join public.teacher_stats on teacher_stats.teacher_id = teacher_xrefs.id; 
