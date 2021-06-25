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


-- Input and Target Assignment Logs

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

drop table if exists experiment_excluded_alogs;
create table experiment_excluded_alogs as 
select 0 as assignment_log_id; 

drop table if exists experiment_target_alogs; 
create table experiment_target_alogs as 
select *
from ordered_alogs 
where log_order = 1 
and assignment_log_id not in (select assignment_log_id from experiment_excluded_alogs) 
and encode_ceri('PS', sequence_id) in ('PSAHQV', 'PSAH9CV', 'PSAJDQG', 'PSAJDQJ', 'PSAJEQW', 'PSAJJXN', 'PSAJVPW', 'PSAJVP8', 'PSAJ2EE', 'PSAJ4ZZ', 'PSAJ43P', 'PSAKUSU', 'PSAMC2V', 'PSAMGHG', 'PSAMQJD', 'PSAMR8Z', 'PSAM4NK', 'PSAPTW7', 'PSAP8PW', 'PSAQJFP', 'PSAR9Y9', 'PSASDZY', 'PSASRKH', 'PSAS25R', 'PSATNB2', 'PSATNCQ', 'PSATZEJ', 'PSAUKPM', 'PSAUKPR', 'PSAUK57', 'PSAUTWT', 'PSAU6Y4', 'PSAU88D', 'PSAV89B', 'PSAWHF4', 'PSAWU6Z', 'PSABANRN'); 

drop table if exists experiment_input_alogs; 
create table experiment_input_alogs as 
select 
ordered_alogs.* 
from ordered_alogs 
inner join experiment_target_alogs on experiment_target_alogs.student_id = ordered_alogs.student_id and experiment_target_alogs.start_time > ordered_alogs.start_time; 

drop table if exists remnant_excluded_alogs; 
create table remnant_excluded_alogs as 
select ordered_alogs.* 
from ordered_alogs 
inner join experiment_target_alogs on experiment_target_alogs.student_id = ordered_alogs.student_id; 

drop table if exists remnant_target_alogs; 
create table remnant_target_alogs as 
select *
from ordered_alogs 
where log_order = 1 
and assignment_log_id not in (select assignment_log_id from remnant_excluded_alogs) 
-- Just the 37 with corresponding experiments
and encode_ceri('PS', sequence_id) in ('PSA2H6G', 'PSAVUGH', 'PSAVUGJ', 'PSAVTVP', 'PSAVUGD', 'PSAVUGP', 'PSAVUGC', 'PSAVUGA', 'PSAVUGF', 'PSAVTVM', 'PSAVUF9', 'PSAVUBA', 'PSAVUF6', 'PSAVUBC', 'PSAVUF2', 'PSAVUA4', 'PSAVTVR', 'PSAVUMC', 'PSAVUMH', 'PSAX692', 'PSAVTUH', 'PSAVTUU', 'PSAVUGR', 'PSAVUGQ', 'PSAVUK7', 'PSAVUMA', 'PSAVTVJ', 'PSAVTV6', 'PSAVUAR', 'PSAVUK8', 'PSAVUGY', 'PSAVUME', 'PSA2H6H', 'PSAV9DV', 'PSAVUF5', 'PSAWU7D', 'PSAHK8'); 
-- All active non-experimental skill builders
--and sequence_id in (10192 ,552775 ,622720 ,10765 ,10264 ,38733 ,14157 ,453434 ,74674 ,204171 ,73681 ,320550 ,407947 ,5897 ,7165 ,12422 ,418944 ,413975 ,9278 ,8585 ,9045 ,12450 ,407954 ,32134 ,10597 ,8752 ,9426 ,448349 ,7171 ,452625 ,9047 ,204166 ,140004 ,24173 ,7212 ,73021 ,407267 ,26468 ,13935 ,5968 ,9180 ,11833 ,8741 ,5924 ,7199 ,204100 ,8884 ,37824 ,5922 ,442518 ,38739 ,9052 ,5898 ,204169 ,6039 ,11893 ,10767 ,33792 ,458321 ,9420 ,6060 ,452624 ,7179 ,6895 ,5971 ,41300 ,7149 ,8886 ,5920 ,9046 ,32179 ,8924 ,541189 ,452121 ,408928 ,7037 ,53233 ,37002 ,96850 ,87321 ,23755 ,11901 ,11899 ,6948 ,448363 ,7158 ,7168 ,26902 ,5962 ,6473 ,459534 ,37213 ,692764 ,692763 ,40996 ,407950 ,78955 ,32177 ,26695 ,202153 ,26469 ,552763 ,552764 ,552756 ,552883 ,15296 ,14168 ,14155 ,10730 ,7893 ,10135 ,9058 ,37980 ,37697 ,8957 ,37210 ,37211 ,37986 ,37765 ,8742 ,14543 ,8878 ,10764 ,179175 ,73028 ,39569 ,447228 ,26696 ,565179 ,8949 ,38728 ,35009 ,33791 ,189895 ,38724 ,72442 ,22457 ,164496 ,14247 ,10763 ,6402 ,72438 ,283473 ,35008 ,9051 ,37091 ,55693 ,31284 ,39659 ,11889 ,204172 ,78704 ,205354 ,72445 ,552186 ,552182 ,552199 ,37982 ,552889 ,552760 ,552885 ,552773 ,552768 ,552589 ,552606 ,552774 ,552608 ,552782 ,552887 ,552757 ,552753 ,552761 ,552769 ,552766 ,552882 ,583237 ,204176 ,7166 ,5956 ,7196 ,6937 ,5933 ,6022 ,6151 ,6065 ,7020 ,748183 ,749415 ,8917 ,54623 ,38734 ,7219 ,7922 ,7023 ,37846 ,442511 ,746480 ,7229 ,748113 ,217163 ,7209 ,31271 ,384407 ,217637 ,243624 ,8928 ,731572 ,732198 ,732340 ,552600 ,388629 ,7155 ,9054 ,9053 ,6915 ,552180 ,9428 ,552184 ,36551 ,14442 ,552892 ,6465 ,7035 ,8876 ,7167 ,26783 ,756586 ,7014 ,7159 ,37983 ,10293 ,699779 ,411598 ,497930 ,552148 ,552158 ,7181);

drop table if exists remnant_input_alogs; 
create table remnant_input_alogs as 
select distinct 
ordered_alogs.* 
from ordered_alogs 
inner join remnant_target_alogs on remnant_target_alogs.student_id = ordered_alogs.student_id and remnant_target_alogs.start_time > ordered_alogs.start_time; 



-- Remnant Assignment Level Features

drop table if exists assignment_level_agg_features; 
create table assignment_level_agg_features as 
select 
assignment_log_id, 
sum((action_defn_type_id = 2)::int) + 1 as session_count, 
count(distinct extract(doy from timestamp)) as day_count, 
sum((action_defn_type_id = 12 and path not like '%SP%')::int) as completed_problem_count 
from 
(
	select assignment_actions.* 
	from student_data.assignment_actions 
	inner join ordered_alogs on ordered_alogs.assignment_log_id = assignment_actions.assignment_log_id
	inner join (select distinct sequence_id from remnant_input_alogs) good_sequences on good_sequences.sequence_id = ordered_alogs.sequence_id
) good_logs
group by assignment_log_id; 

drop table if exists assignment_level_stats; 
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

drop table if exists assignment_paths;
create table assignment_paths as 
select * from
(
	with recursive recursed_folders(parent_id, child_id, dir) as 
	(
		select f1.parent_id as parent_id, f1.id as child_id, f1.id::text as dir 
		from legacy.folders f1 
		where id in (select id from legacy.folders where parent_id in (86954)) 
		union all 
		(
			select f2.parent_id as parent_id, f2.id as child_id, (rf.dir::text || '!' || f2.id::text)::varchar(255) as dir 
			from legacy.folders f2, recursed_folders rf 
			where f2.parent_id = rf.child_id 
		)
	)
	select distinct
	row_number() over(partition by curriculum_items.sequence_id order by recursed_folders.dir) as path_number, 
	case when split_part(recursed_folders.dir, '!', 1) = '' then 0 else split_part(recursed_folders.dir, '!', 1)::int end as directory_1,
	case when split_part(recursed_folders.dir, '!', 2) = '' then 0 else split_part(recursed_folders.dir, '!', 2)::int end as directory_2,
	case when split_part(recursed_folders.dir, '!', 3) = '' then 0 else split_part(recursed_folders.dir, '!', 3)::int end as directory_3,
	curriculum_items.sequence_id 
	from recursed_folders
	left join legacy.folder_items on folder_items.folder_id = recursed_folders.child_id
	left join legacy.curriculum_items on curriculum_items.id = folder_items.item_id
	where folder_items.item_type = 'CurriculumItem'
	and sequence_id is not null
) paths
where path_number = 1;


drop table if exists assignment_level_features; 
create table assignment_level_features as 
select 
remnant_input_alogs.assignment_log_id, 
remnant_input_alogs.student_id, 
assignment_paths.directory_1,
assignment_paths.directory_2,
assignment_paths.directory_3,
assignment_paths.sequence_id,
extract(epoch from assignment_logs.start_time) as assignment_start_time, 
(sequences.parameters like '%pseudo_skill_builder%' or sections.type='MasterySection' or sections.type='LinearMasterySection')::int as is_skill_builder, 
(assignments.due_date is not null)::int as has_due_date, 
(assignment_logs.end_time is not null)::int as assignment_completed, 
ln(coalesce(extract(epoch from (assignment_logs.start_time - lag(assignment_logs.start_time, 1) over (partition by assignment_logs.user_xid order by assignment_logs.start_time))), extract(epoch from (assignment_logs.start_time - users.created)))) as time_since_last_assignment_start, 
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
inner join remnant_input_alogs on remnant_input_alogs.assignment_log_id = assignment_logs.id 
inner join users.users on users.id = remnant_input_alogs.student_id 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join legacy.sequences on sequences.id = assignments.sequence_id 
inner join legacy.sections on sections.id = sequences.head_section_id 
inner join assignment_level_agg_features on assignment_level_agg_features.assignment_log_id = assignment_logs.id 
inner join assignment_level_stats on assignment_level_stats.sequence_id = sequences.id
left join assignment_paths on assignment_paths.sequence_id = sequences.id; 


-- Remnant Problem Level Features

drop table if exists problem_level_stats;
create table problem_level_stats as 
with ln_medians as
(
	select 
	problem_id as pid, 
	median(ln(extract(epoch from (problem_logs.end_time - problem_logs.start_time)))::numeric) as time_on_task_med, 
	median(ln(problem_logs.first_response_time::float / 1000)::numeric) as first_response_time_med 
	from student_data.problem_logs 
	where problem_logs.end_time is not null 
	and problem_logs.first_response_time is not null 
	and problem_logs.path_info not like '%SP%' 
	group by problem_id 
)
select 
problem_logs.problem_id, 
ln_medians.time_on_task_med, 
median(abs(ln(extract(epoch from (problem_logs.end_time - problem_logs.start_time))) - ln_medians.time_on_task_med)::numeric) as time_on_task_mad, 
ln_medians.first_response_time_med, 
median(abs(ln(problem_logs.first_response_time::float / 1000) - ln_medians.first_response_time_med)::numeric) as first_response_time_mad, 
avg(problem_logs.attempt_count) as attempt_count_avg, 
coalesce(stddev(problem_logs.attempt_count), 0) as attempt_count_stddev, 
avg((problem_logs.first_action_type_id = 1)::int) as answer_first_avg, 
coalesce(stddev((problem_logs.first_action_type_id = 1)::int), 0) as answer_first_stddev, 
coalesce(avg((problem_logs.discrete_score = 1)::int), 1) as correctness_avg, 
coalesce(stddev((problem_logs.discrete_score = 1)::int), 0) as correctness_stddev, 
avg(problem_logs.hint_count) as hint_count_avg, 
coalesce(stddev(problem_logs.hint_count), 0) as hint_count_stddev, 
avg((problem_logs.bottom_hint)::int) as answer_given_avg, 
coalesce(stddev((problem_logs.bottom_hint)::int), 0) as answer_given_stddev 
from student_data.problem_logs 
inner join ordered_alogs on ordered_alogs.assignment_log_id = problem_logs.assignment_log_id
inner join (select distinct sequence_id from remnant_input_alogs) good_sequences on good_sequences.sequence_id = ordered_alogs.sequence_id
inner join ln_medians on ln_medians.pid = problem_logs.problem_id 
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by problem_logs.problem_id, ln_medians.time_on_task_med, ln_medians.first_response_time_med; 


drop table if exists problem_level_features; 
create table problem_level_features as 
select 
remnant_input_alogs.assignment_log_id as assignment_log_id, 
median(ln(extract(epoch from (problem_logs.end_time - problem_logs.start_time)))::numeric) as median_ln_problem_time_on_task_raw, 
median((case when problem_level_stats.time_on_task_mad = 0 then 0 else (ln(extract(epoch from (problem_logs.end_time - problem_logs.start_time))) - problem_level_stats.time_on_task_med) / problem_level_stats.time_on_task_mad end)::numeric) as median_ln_problem_time_on_task_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by median((case when problem_level_stats.time_on_task_mad = 0 then 0 else (ln(extract(epoch from (problem_logs.end_time - problem_logs.start_time))) - problem_level_stats.time_on_task_med) / problem_level_stats.time_on_task_mad end)::numeric)) as median_ln_problem_time_on_task_class_percentile, 
median(ln(problem_logs.first_response_time::float / 1000)::numeric) as median_ln_problem_first_response_time_raw, 
median((case when problem_level_stats.first_response_time_mad = 0 then 0 else (ln(problem_logs.first_response_time::float / 1000) - problem_level_stats.first_response_time_med) / problem_level_stats.first_response_time_mad end)::numeric) as median_ln_problem_first_response_time_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by median((case when problem_level_stats.first_response_time_mad = 0 then 0 else (ln(problem_logs.first_response_time::float / 1000) - problem_level_stats.first_response_time_med) / problem_level_stats.first_response_time_mad end)::numeric)) as median_ln_problem_first_response_time_class_percentile,
avg(problem_logs.attempt_count) as average_problem_attempt_count, 
avg(case when problem_level_stats.attempt_count_stddev = 0 then 0 else (problem_logs.attempt_count - problem_level_stats.attempt_count_avg) / problem_level_stats.attempt_count_stddev end) as average_problem_attempt_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when problem_level_stats.attempt_count_stddev = 0 then 0 else (problem_logs.attempt_count - problem_level_stats.attempt_count_avg) / problem_level_stats.attempt_count_stddev end)) as average_problem_attempt_count_class_percentile, 
avg((problem_logs.first_action_type_id = 1)::int) as average_problem_answer_first, 
avg(case when problem_level_stats.answer_first_stddev = 0 then 0 else ((problem_logs.first_action_type_id = 1)::int - problem_level_stats.answer_first_avg) / problem_level_stats.answer_first_stddev end) as average_problem_answer_first_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when problem_level_stats.answer_first_stddev = 0 then 0 else ((problem_logs.first_action_type_id = 1)::int - problem_level_stats.answer_first_avg) / problem_level_stats.answer_first_stddev end)) as average_problem_answer_first_class_percentile, 
coalesce(avg((problem_logs.discrete_score = 1)::int), 1) as average_problem_correctness, 
coalesce(avg(case when problem_level_stats.correctness_stddev = 0 then 0 else ((problem_logs.discrete_score = 1)::int - problem_level_stats.correctness_avg) / problem_level_stats.correctness_stddev end), 0) as average_problem_correctness_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when problem_level_stats.correctness_stddev = 0 then 0 else ((problem_logs.discrete_score = 1)::int - problem_level_stats.correctness_avg) / problem_level_stats.correctness_stddev end)) as average_problem_correctness_class_percentile, 
avg(problem_logs.hint_count) as average_problem_hint_count, 
avg(case when problem_level_stats.hint_count_stddev = 0 then 0 else (problem_logs.hint_count - problem_level_stats.hint_count_avg) / problem_level_stats.hint_count_stddev end) as average_problem_hint_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when problem_level_stats.hint_count_stddev = 0 then 0 else (problem_logs.hint_count - problem_level_stats.hint_count_avg) / problem_level_stats.hint_count_stddev end)) as average_problem_hint_count_class_percentile, 
avg((problem_logs.bottom_hint)::int) as average_problem_answer_given, 
avg(case when problem_level_stats.answer_given_stddev = 0 then 0 else ((problem_logs.bottom_hint)::int - problem_level_stats.answer_given_avg) / problem_level_stats.answer_given_stddev end) as average_problem_answer_given_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when problem_level_stats.answer_given_stddev = 0 then 0 else ((problem_logs.bottom_hint)::int - problem_level_stats.answer_given_avg) / problem_level_stats.answer_given_stddev end)) as average_problem_answer_given_class_percentile 
from student_data.problem_logs 
inner join remnant_input_alogs on remnant_input_alogs.assignment_log_id = problem_logs.assignment_log_id 
inner join student_data.assignment_logs on assignment_logs.id = problem_logs.assignment_log_id 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join problem_level_stats on problem_level_stats.problem_id = problem_logs.problem_id 
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by remnant_input_alogs.assignment_log_id, assignments.group_context_xid; 



-- Remnant Inputs

drop table if exists remnant_inputs; 
create table remnant_inputs as 
select 
assignment_level_features.student_id,
assignment_level_features.assignment_start_time,
assignment_level_features.directory_1,
assignment_level_features.directory_2,
assignment_level_features.directory_3,
assignment_level_features.sequence_id,
assignment_level_features.is_skill_builder,
assignment_level_features.has_due_date,
assignment_level_features.assignment_completed,
assignment_level_features.time_since_last_assignment_start,
assignment_level_features.session_count_raw,
assignment_level_features.session_count_normalized,
assignment_level_features.session_count_class_percentile,
assignment_level_features.day_count_raw,
assignment_level_features.day_count_normalized,
assignment_level_features.day_count_class_percentile,
assignment_level_features.completed_problem_count_raw,
assignment_level_features.completed_problem_count_normalized,
assignment_level_features.completed_problem_count_class_percentile,
problem_level_features.median_ln_problem_time_on_task_raw,
problem_level_features.median_ln_problem_time_on_task_normalized,
problem_level_features.median_ln_problem_time_on_task_class_percentile,
problem_level_features.median_ln_problem_first_response_time_raw,
problem_level_features.median_ln_problem_first_response_time_normalized,
problem_level_features.median_ln_problem_first_response_time_class_percentile,
problem_level_features.average_problem_attempt_count,
problem_level_features.average_problem_attempt_count_normalized,
problem_level_features.average_problem_attempt_count_class_percentile,
problem_level_features.average_problem_answer_first,
problem_level_features.average_problem_answer_first_normalized,
problem_level_features.average_problem_answer_first_class_percentile,
problem_level_features.average_problem_correctness,
problem_level_features.average_problem_correctness_normalized,
problem_level_features.average_problem_correctness_class_percentile,
problem_level_features.average_problem_hint_count,
problem_level_features.average_problem_hint_count_normalized,
problem_level_features.average_problem_hint_count_class_percentile,
problem_level_features.average_problem_answer_given,
problem_level_features.average_problem_answer_given_normalized,
problem_level_features.average_problem_answer_given_class_percentile
from assignment_level_features 
left join problem_level_features on assignment_level_features.assignment_log_id = problem_level_features.assignment_log_id
order by assignment_level_features.student_id, assignment_level_features.assignment_start_time; 



-- Remnant Targets

drop table if exists remnant_targets; 
create table remnant_targets as 
select 
encode_ceri('PS', remnant_target_alogs.sequence_id) as target_sequence, 
remnant_target_alogs.student_id, 
extract(epoch from remnant_target_alogs.start_time) as assignment_start_time, 
(assignment_logs.end_time is not null)::int as assignment_completed, 
count(*) as problems_completed 
from remnant_target_alogs
left join student_data.assignment_logs on assignment_logs.id = remnant_target_alogs.assignment_log_id 
left join student_data.problem_logs on problem_logs.assignment_log_id = remnant_target_alogs.assignment_log_id 
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by encode_ceri('PS', remnant_target_alogs.sequence_id), remnant_target_alogs.student_id, (assignment_logs.end_time is not null)::int, extract(epoch from remnant_target_alogs.start_time) 
order by remnant_target_alogs.student_id; 

