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
assignment_logs.end_time, 
assignments.sequence_id, 
assignments.group_context_xid as class_id, 
student_xrefs.id as student_id, 
(assignments.due_date is not null)::int as has_due_date, 
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
and encode_ceri('PS', sequence_id) in ('PSABANRN','PSABF5JC','PSAGF4','PSAH9CV','PSAHQV','PSAJ2EE','PSAJ43P','PSAJ4YN','PSAJ4ZU','PSAJ4ZZ','PSAJDQG','PSAJDQJ','PSAJJXN','PSAJVP8','PSAJVPW','PSAJY7C','PSAKUSU','PSAMC2V','PSAMGHG','PSAMQJD','PSAMR8Z','PSAQJFP','PSAR9Y9','PSAS25R','PSASDZY','PSASRKH','PSATNB2','PSATNCQ','PSATZEJ','PSAU6Y4','PSAU7GZ','PSAU85Y','PSAU88D','PSAUK57','PSAUKPM','PSAUKPR','PSAV89B','PSAWHF4','PSAWU6Z','PSAXTEE'); 

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
from
ordered_alogs 
where log_order = 1 
and encode_ceri('PS', sequence_id) in ('PSA2H6E','PSA2H6G','PSA2H6H','PSA2NQ','PSA2SFT','PSA3U7K','PSA3UJD','PSA3VB6','PSA3ZK','PSA4B3A','PSA4D9T','PSA4DRY','PSA4DT8','PSA4E3','PSA4E4Y','PSA4PKA','PSA63E','PSA63F','PSA659','PSA694','PSA6T4','PSA6T5','PSAB3PG','PSAB45B','PSAB58U','PSABBT2','PSABBUF','PSABCQV','PSABCR9','PSABCSB','PSABEFB','PSABEFC','PSABFQK','PSABFQM','PSABFTC','PSABHBC','PSABHSW','PSABHUM','PSABHVS','PSABHZM','PSABHZN','PSABHZQ','PSABJC8','PSABJHB','PSABJKH','PSABJN5','PSABJNE','PSABJP4','PSABJTF','PSABJTH','PSABJTJ','PSABJTN','PSABK27','PSABK2K','PSABKJ5','PSABKJ9','PSABKKF','PSABKKK','PSABKKM','PSABKKR','PSABKKS','PSABKKW','PSABKKX','PSABMFQ','PSABMJM','PSABMSW','PSABN9J','PSABNXR','PSABTKXQ','PSAC754','PSACQ92','PSACQ9T','PSACQN5','PSACQN8','PSACQNZ','PSACRX4','PSACSY5','PSACUED','PSACW65','PSACXE8','PSAD9C6','PSADAWC','PSADH3G','PSAEYYJ','PSAFTFJ','PSAFTFM','PSAG3NC','PSAG5K5','PSAG5N6','PSAG5N7','PSAG5PY','PSAG5RA','PSAG5RD','PSAG5RF','PSAG5RG','PSAG5RM','PSAG6YM','PSAGAQ5','PSAGE9','PSAGEH','PSAGEJ','PSAGFB','PSAGFD','PSAGFP','PSAGGE','PSAGGM','PSAGGQ','PSAGGT','PSAGGU','PSAGGW','PSAGH7','PSAGJ4','PSAGJJ','PSAGKN','PSAGKS','PSAGKX','PSAGKY','PSAGNVX','PSAGPQ','PSAGXT','PSAGZ4','PSAGZU','PSAHEA','PSAHFQ','PSAHG3','PSAHGC','PSAHGJ','PSAHHE','PSAHJ9J','PSAHK8','PSAHKE','PSAHKJ','PSAHKR','PSAHKRU','PSAHKU','PSAHMA','PSAHQ4','PSAHQ6','PSAHQ7','PSAHQ8','PSAHQW','PSAHRE','PSAHRF','PSAHRG','PSAHRH','PSAHRM','PSAHRV','PSAHRX','PSAHS6','PSAHSD','PSAHSE','PSAHSH','PSAHSU','PSAHSX','PSAHTG','PSAJ78','PSAJFS5','PSAJGW','PSAJHU','PSAK36','PSAK4C','PSAK4D','PSAK4E','PSAK6J','PSAKC9','PSAKDA','PSAKDM','PSAKH7','PSAKHM','PSAKHP','PSAKHV','PSAKHX','PSAKJ6','PSAKJX','PSAKK8','PSAKKA','PSAKKC','PSAKKV','PSAKKY','PSAKP3','PSAKP4','PSAKP5','PSAKP9','PSAKQA','PSAKQB','PSAKQC','PSAKQG','PSAKS9K','PSAKUE','PSAKWG','PSAKXK','PSAM2UM','PSAMT8','PSAMV3','PSAMV6','PSAMYD','PSAMYE','PSAMZB','PSANA5','PSANFE','PSANGG','PSANGH','PSANGJ','PSANGM','PSAP35H','PSAP7AH','PSAP7Z','PSAP8V','PSAP8W','PSAPKZ','PSAPN4','PSAPN5','PSAPN7','PSAPNT','PSAPNX','PSAQ4KN','PSAQ63B','PSAQBPQ','PSAQX3X','PSAQYS4','PSAQYSV','PSAQYSY','PSAQZTH','PSAQZTJ','PSAR2B','PSAR4V','PSAR5R3','PSAR5RU','PSARB8M','PSARST','PSARZW','PSARZY','PSAS7Q','PSASA6','PSASANZ','PSASBT6','PSASBUM','PSASEE','PSASF93','PSASF94','PSASFRU','PSASG47','PSASN7U','PSASQFY','PSATE7','PSAV9DV','PSAVFEZ','PSAVFGT','PSAVTJ5','PSAVTK2','PSAVTT8','PSAVTT9','PSAVTUA','PSAVTUH','PSAVTUU','PSAVTV6','PSAVTVJ','PSAVTVM','PSAVTVP','PSAVTVR','PSAVTVX','PSAVUA4','PSAVUA6','PSAVUAR','PSAVUBA','PSAVUBC','PSAVUF2','PSAVUF3','PSAVUF5','PSAVUF6','PSAVUF8','PSAVUF9','PSAVUFX','PSAVUGA','PSAVUGC','PSAVUGD','PSAVUGF','PSAVUGH','PSAVUGJ','PSAVUGK','PSAVUGM','PSAVUGP','PSAVUGQ','PSAVUGR','PSAVUGT','PSAVUGV','PSAVUGX','PSAVUGY','PSAVUK5','PSAVUK7','PSAVUK8','PSAVUK9','PSAVUMA','PSAVUMC','PSAVUMD','PSAVUME','PSAVUMH','PSAVUMJ','PSAVUMK','PSAVUMN','PSAW5Q9','PSAWFXY','PSAWU7D','PSAWUUQ','PSAX692','PSAX7DZ','PSAX7E9','PSAXQMQ','PSAXVDX','PSAXWCT','PSAXWXY','PSAZDZ','PSAZV4') 
except 
select * from 
remnant_excluded_alogs;

drop table if exists remnant_input_alogs; 
create table remnant_input_alogs as 
select distinct 
ordered_alogs.* 
from ordered_alogs 
inner join remnant_target_alogs on remnant_target_alogs.student_id = ordered_alogs.student_id and remnant_target_alogs.start_time > ordered_alogs.start_time; 










-- Remnant Actions

drop table if exists remnant_actions; 
create table remnant_actions as 
select 
remnant_input_alogs.student_id, 
extract(epoch from assignment_actions.timestamp)::int - (extract(epoch from assignment_actions.timestamp)::int % 86400) as timestamp, 
sum((assignment_actions.action_defn_type_id = 1)::int) as action_1_count, 
sum((assignment_actions.action_defn_type_id = 2)::int) as action_2_count, 
sum((assignment_actions.action_defn_type_id = 3)::int) as action_3_count, 
sum((assignment_actions.action_defn_type_id = 4)::int) as action_4_count, 
sum((assignment_actions.action_defn_type_id = 5)::int) as action_5_count, 
sum((assignment_actions.action_defn_type_id = 6)::int) as action_6_count, 
sum((assignment_actions.action_defn_type_id = 7)::int) as action_7_count, 
sum((assignment_actions.action_defn_type_id = 8)::int) as action_8_count, 
sum((assignment_actions.action_defn_type_id = 9)::int) as action_9_count, 
sum((assignment_actions.action_defn_type_id = 10)::int) as action_10_count, 
sum((assignment_actions.action_defn_type_id = 11)::int) as action_11_count, 
sum((assignment_actions.action_defn_type_id = 12)::int) as action_12_count, 
sum((assignment_actions.action_defn_type_id = 13)::int) as action_13_count, 
sum((assignment_actions.action_defn_type_id = 14)::int) as action_14_count, 
sum((assignment_actions.action_defn_type_id = 15)::int) as action_15_count, 
sum((assignment_actions.action_defn_type_id = 16)::int) as action_16_count, 
sum((assignment_actions.action_defn_type_id = 17)::int) as action_17_count, 
sum((assignment_actions.action_defn_type_id = 18)::int) as action_18_count, 
sum((assignment_actions.action_defn_type_id = 19)::int) as action_19_count, 
sum((assignment_actions.action_defn_type_id = 20 and coalesce(action_responses.correct, false))::int) as action_20a_count, 
sum((assignment_actions.action_defn_type_id = 20 and not coalesce(action_responses.correct, false))::int) as action_20b_count, 
sum((assignment_actions.action_defn_type_id = 21)::int) as action_21_count, 
sum((assignment_actions.action_defn_type_id = 22)::int) as action_22_count, 
sum((assignment_actions.action_defn_type_id = 23)::int) as action_23_count, 
sum((assignment_actions.action_defn_type_id = 24)::int) as action_24_count, 
sum((assignment_actions.action_defn_type_id = 25)::int) as action_25_count, 
sum((assignment_actions.action_defn_type_id = 26)::int) as action_26_count, 
sum((assignment_actions.action_defn_type_id = 27)::int) as action_27_count, 
sum((assignment_actions.action_defn_type_id = 28)::int) as action_28_count, 
sum((assignment_actions.action_defn_type_id = 29)::int) as action_29_count, 
sum((assignment_actions.action_defn_type_id = 30)::int) as action_30_count, 
sum((assignment_actions.action_defn_type_id = 31)::int) as action_31_count, 
sum((assignment_actions.action_defn_type_id = 32)::int) as action_32_count, 
sum((assignment_actions.action_defn_type_id = 33)::int) as action_33_count, 
sum((assignment_actions.action_defn_type_id = 34)::int) as action_34_count, 
sum((assignment_actions.action_defn_type_id = 35)::int) as action_35_count 
from student_data.assignment_actions 
left join student_data.problem_actions on problem_actions.id = assignment_actions.action_id 
left join student_data.action_responses on action_responses.id = problem_actions.action_details_id 
inner join remnant_input_alogs on remnant_input_alogs.assignment_log_id = assignment_actions.assignment_log_id 
group by remnant_input_alogs.student_id, extract(epoch from assignment_actions.timestamp)::int - (extract(epoch from assignment_actions.timestamp)::int % 86400); 



-- Remnant Assignment Level Features

drop table if exists remnant_assignment_level_agg_features; 
create table remnant_assignment_level_agg_features as 
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
	where ordered_alogs.student_id not in (select student_id from experiment_target_alogs) 
) good_logs 
group by assignment_log_id; 

drop table if exists assignment_level_stats; 
create table assignment_level_stats as 
select 
assignments.sequence_id, 
avg(remnant_assignment_level_agg_features.session_count) as session_count_avg, 
coalesce(stddev(remnant_assignment_level_agg_features.session_count), 0) as session_count_stddev, 
avg(remnant_assignment_level_agg_features.day_count) as day_count_avg, 
coalesce(stddev(remnant_assignment_level_agg_features.day_count), 0) as day_count_stddev, 
avg(remnant_assignment_level_agg_features.completed_problem_count) as completed_problem_count_avg, 
coalesce(stddev(remnant_assignment_level_agg_features.completed_problem_count), 0) as completed_problem_count_stddev 
from remnant_assignment_level_agg_features 
inner join assignment_xrefs on assignment_xrefs.xid = remnant_assignment_level_agg_features.assignment_log_id 
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


drop table if exists remnant_assignment_level_features; 
create table remnant_assignment_level_features as 
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
ln(coalesce(extract(epoch from (assignment_logs.start_time - lag(assignment_logs.start_time, 1) over (partition by assignment_logs.user_xid order by assignment_logs.start_time))), extract(epoch from (assignment_logs.start_time - users.created))) + 0.00001) as time_since_last_assignment_start, 
remnant_assignment_level_agg_features.session_count as session_count_raw, 
case when assignment_level_stats.session_count_stddev = 0 then 0 else (remnant_assignment_level_agg_features.session_count - assignment_level_stats.session_count_avg) / assignment_level_stats.session_count_stddev end as session_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by case when assignment_level_stats.session_count_stddev = 0 then 0 else (remnant_assignment_level_agg_features.session_count - assignment_level_stats.session_count_avg) / assignment_level_stats.session_count_stddev end) as session_count_class_percentile, 
remnant_assignment_level_agg_features.day_count as day_count_raw, 
case when assignment_level_stats.day_count_stddev = 0 then 0 else (remnant_assignment_level_agg_features.day_count - assignment_level_stats.day_count_avg) / assignment_level_stats.day_count_stddev end as day_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by case when assignment_level_stats.day_count_stddev = 0 then 0 else (remnant_assignment_level_agg_features.day_count - assignment_level_stats.day_count_avg) / assignment_level_stats.day_count_stddev end) as day_count_class_percentile, 
remnant_assignment_level_agg_features.completed_problem_count as completed_problem_count_raw, 
case when assignment_level_stats.completed_problem_count_stddev = 0 then 0 else (remnant_assignment_level_agg_features.completed_problem_count - assignment_level_stats.completed_problem_count_avg) / assignment_level_stats.completed_problem_count_stddev end as completed_problem_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by case when assignment_level_stats.completed_problem_count_stddev = 0 then 0 else (remnant_assignment_level_agg_features.completed_problem_count - assignment_level_stats.completed_problem_count_avg) / assignment_level_stats.completed_problem_count_stddev end) as completed_problem_count_class_percentile 
from student_data.assignment_logs 
inner join remnant_input_alogs on remnant_input_alogs.assignment_log_id = assignment_logs.id 
inner join users.users on users.id = remnant_input_alogs.student_id 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join legacy.sequences on sequences.id = assignments.sequence_id 
inner join legacy.sections on sections.id = sequences.head_section_id 
inner join remnant_assignment_level_agg_features on remnant_assignment_level_agg_features.assignment_log_id = assignment_logs.id 
inner join assignment_level_stats on assignment_level_stats.sequence_id = sequences.id
left join assignment_paths on assignment_paths.sequence_id = sequences.id; 


-- Remnant Problem Level Features

drop table if exists remnant_problem_level_stats;
create table remnant_problem_level_stats as 
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
where ordered_alogs.student_id not in (select student_id from experiment_target_alogs) 
and problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by problem_logs.problem_id, ln_medians.time_on_task_med, ln_medians.first_response_time_med; 


drop table if exists remnant_problem_level_features; 
create table remnant_problem_level_features as 
select 
remnant_input_alogs.assignment_log_id as assignment_log_id, 
median(ln(extract(epoch from (problem_logs.end_time - problem_logs.start_time)))::numeric) as median_ln_problem_time_on_task_raw, 
median((case when remnant_problem_level_stats.time_on_task_mad = 0 then 0 else (ln(extract(epoch from (problem_logs.end_time - problem_logs.start_time))) - remnant_problem_level_stats.time_on_task_med) / remnant_problem_level_stats.time_on_task_mad end)::numeric) as median_ln_problem_time_on_task_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by median((case when remnant_problem_level_stats.time_on_task_mad = 0 then 0 else (ln(extract(epoch from (problem_logs.end_time - problem_logs.start_time))) - remnant_problem_level_stats.time_on_task_med) / remnant_problem_level_stats.time_on_task_mad end)::numeric)) as median_ln_problem_time_on_task_class_percentile, 
median(ln(problem_logs.first_response_time::float / 1000)::numeric) as median_ln_problem_first_response_time_raw, 
median((case when remnant_problem_level_stats.first_response_time_mad = 0 then 0 else (ln(problem_logs.first_response_time::float / 1000) - remnant_problem_level_stats.first_response_time_med) / remnant_problem_level_stats.first_response_time_mad end)::numeric) as median_ln_problem_first_response_time_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by median((case when remnant_problem_level_stats.first_response_time_mad = 0 then 0 else (ln(problem_logs.first_response_time::float / 1000) - remnant_problem_level_stats.first_response_time_med) / remnant_problem_level_stats.first_response_time_mad end)::numeric)) as median_ln_problem_first_response_time_class_percentile,
avg(problem_logs.attempt_count) as average_problem_attempt_count, 
avg(case when remnant_problem_level_stats.attempt_count_stddev = 0 then 0 else (problem_logs.attempt_count - remnant_problem_level_stats.attempt_count_avg) / remnant_problem_level_stats.attempt_count_stddev end) as average_problem_attempt_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when remnant_problem_level_stats.attempt_count_stddev = 0 then 0 else (problem_logs.attempt_count - remnant_problem_level_stats.attempt_count_avg) / remnant_problem_level_stats.attempt_count_stddev end)) as average_problem_attempt_count_class_percentile, 
avg((problem_logs.first_action_type_id = 1)::int) as average_problem_answer_first, 
avg(case when remnant_problem_level_stats.answer_first_stddev = 0 then 0 else ((problem_logs.first_action_type_id = 1)::int - remnant_problem_level_stats.answer_first_avg) / remnant_problem_level_stats.answer_first_stddev end) as average_problem_answer_first_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when remnant_problem_level_stats.answer_first_stddev = 0 then 0 else ((problem_logs.first_action_type_id = 1)::int - remnant_problem_level_stats.answer_first_avg) / remnant_problem_level_stats.answer_first_stddev end)) as average_problem_answer_first_class_percentile, 
coalesce(avg((problem_logs.discrete_score = 1)::int), 1) as average_problem_correctness, 
coalesce(avg(case when remnant_problem_level_stats.correctness_stddev = 0 then 0 else ((problem_logs.discrete_score = 1)::int - remnant_problem_level_stats.correctness_avg) / remnant_problem_level_stats.correctness_stddev end), 0) as average_problem_correctness_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when remnant_problem_level_stats.correctness_stddev = 0 then 0 else ((problem_logs.discrete_score = 1)::int - remnant_problem_level_stats.correctness_avg) / remnant_problem_level_stats.correctness_stddev end)) as average_problem_correctness_class_percentile, 
avg(problem_logs.hint_count) as average_problem_hint_count, 
avg(case when remnant_problem_level_stats.hint_count_stddev = 0 then 0 else (problem_logs.hint_count - remnant_problem_level_stats.hint_count_avg) / remnant_problem_level_stats.hint_count_stddev end) as average_problem_hint_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when remnant_problem_level_stats.hint_count_stddev = 0 then 0 else (problem_logs.hint_count - remnant_problem_level_stats.hint_count_avg) / remnant_problem_level_stats.hint_count_stddev end)) as average_problem_hint_count_class_percentile, 
avg((problem_logs.bottom_hint)::int) as average_problem_answer_given, 
avg(case when remnant_problem_level_stats.answer_given_stddev = 0 then 0 else ((problem_logs.bottom_hint)::int - remnant_problem_level_stats.answer_given_avg) / remnant_problem_level_stats.answer_given_stddev end) as average_problem_answer_given_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when remnant_problem_level_stats.answer_given_stddev = 0 then 0 else ((problem_logs.bottom_hint)::int - remnant_problem_level_stats.answer_given_avg) / remnant_problem_level_stats.answer_given_stddev end)) as average_problem_answer_given_class_percentile 
from student_data.problem_logs 
inner join remnant_input_alogs on remnant_input_alogs.assignment_log_id = problem_logs.assignment_log_id 
inner join student_data.assignment_logs on assignment_logs.id = problem_logs.assignment_log_id 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join remnant_problem_level_stats on remnant_problem_level_stats.problem_id = problem_logs.problem_id 
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by remnant_input_alogs.assignment_log_id, assignments.group_context_xid; 



-- Remnant Inputs

drop table if exists remnant_inputs; 
create table remnant_inputs as 
select 
remnant_assignment_level_features.student_id,
remnant_assignment_level_features.assignment_start_time,
remnant_assignment_level_features.directory_1,
remnant_assignment_level_features.directory_2,
remnant_assignment_level_features.directory_3,
remnant_assignment_level_features.sequence_id,
remnant_assignment_level_features.is_skill_builder,
remnant_assignment_level_features.has_due_date,
remnant_assignment_level_features.assignment_completed,
remnant_assignment_level_features.time_since_last_assignment_start,
remnant_assignment_level_features.session_count_raw,
remnant_assignment_level_features.session_count_normalized,
remnant_assignment_level_features.session_count_class_percentile,
remnant_assignment_level_features.day_count_raw,
remnant_assignment_level_features.day_count_normalized,
remnant_assignment_level_features.day_count_class_percentile,
remnant_assignment_level_features.completed_problem_count_raw,
remnant_assignment_level_features.completed_problem_count_normalized,
remnant_assignment_level_features.completed_problem_count_class_percentile,
remnant_problem_level_features.median_ln_problem_time_on_task_raw,
remnant_problem_level_features.median_ln_problem_time_on_task_normalized,
remnant_problem_level_features.median_ln_problem_time_on_task_class_percentile,
remnant_problem_level_features.median_ln_problem_first_response_time_raw,
remnant_problem_level_features.median_ln_problem_first_response_time_normalized,
remnant_problem_level_features.median_ln_problem_first_response_time_class_percentile, 
remnant_problem_level_features.average_problem_attempt_count, 
remnant_problem_level_features.average_problem_attempt_count_normalized, 
remnant_problem_level_features.average_problem_attempt_count_class_percentile, 
remnant_problem_level_features.average_problem_answer_first, 
remnant_problem_level_features.average_problem_answer_first_normalized, 
remnant_problem_level_features.average_problem_answer_first_class_percentile, 
remnant_problem_level_features.average_problem_correctness, 
remnant_problem_level_features.average_problem_correctness_normalized, 
remnant_problem_level_features.average_problem_correctness_class_percentile, 
remnant_problem_level_features.average_problem_hint_count, 
remnant_problem_level_features.average_problem_hint_count_normalized, 
remnant_problem_level_features.average_problem_hint_count_class_percentile, 
remnant_problem_level_features.average_problem_answer_given, 
remnant_problem_level_features.average_problem_answer_given_normalized, 
remnant_problem_level_features.average_problem_answer_given_class_percentile 
from remnant_assignment_level_features 
left join remnant_problem_level_features on remnant_assignment_level_features.assignment_log_id = remnant_problem_level_features.assignment_log_id 
order by remnant_assignment_level_features.student_id, remnant_assignment_level_features.assignment_start_time; 



-- Remnant Targets

drop table if exists remnant_assignment_priors;
create table remnant_assignment_priors as
select 
remnant_target_alogs.assignment_log_id, 
count(assignment_logs.start_time) as student_prior_assignments_started, 
count(assignment_logs.end_time)::real / count(assignment_logs.start_time) as student_prior_assignments_percent_completed,
ln(median(extract(epoch from assignment_logs.end_time - assignment_logs.start_time)::numeric) + 0.00001) as student_prior_median_ln_assignment_time_on_task
from remnant_target_alogs
inner join student_xrefs on student_xrefs.id = remnant_target_alogs.student_id
inner join student_data.assignment_logs on assignment_logs.user_xid = student_xrefs.xid and assignment_logs.start_time < remnant_target_alogs.start_time
group by remnant_target_alogs.assignment_log_id;


drop table if exists remnant_problem_priors;
create table remnant_problem_priors as
select 
remnant_target_alogs.assignment_log_id, 
count(problem_logs.start_time)::real / count(distinct assignment_logs.start_time) as student_prior_average_problems_per_assignment, 
ln(median(extract(epoch from (problem_logs.end_time - problem_logs.start_time))::numeric) + 0.00001) as student_prior_median_ln_problem_time_on_task, 
ln(median((problem_logs.first_response_time::float / 1000)::numeric) + 0.00001) as student_prior_median_ln_problem_first_response_time, 
avg(problem_logs.discrete_score) as student_prior_average_problem_correctness, 
avg(problem_logs.attempt_count) as student_prior_average_problem_attempt_count, 
avg(problem_logs.hint_count) as student_prior_average_problem_hint_count
from remnant_target_alogs
inner join student_xrefs on student_xrefs.id = remnant_target_alogs.student_id
inner join student_data.assignment_logs on assignment_logs.user_xid = student_xrefs.xid and assignment_logs.start_time < remnant_target_alogs.start_time
inner join student_data.problem_logs on problem_logs.assignment_log_id = assignment_logs.id
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by remnant_target_alogs.assignment_log_id;


drop table if exists remnant_target_problems_completed;
create table remnant_target_problems_completed as
select
remnant_target_alogs.assignment_log_id,
count(*) as problems_completed 
from remnant_target_alogs 
left join student_data.problem_logs on problem_logs.assignment_log_id = remnant_target_alogs.assignment_log_id 
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by remnant_target_alogs.assignment_log_id;


drop table if exists remnant_targets; 
create table remnant_targets as 
select 
remnant_target_alogs.class_id, 
remnant_target_alogs.student_id, 
extract(epoch from remnant_target_alogs.start_time) as assignment_start_time, 
encode_ceri('PS', remnant_target_alogs.sequence_id) as target_sequence, 
remnant_target_alogs.has_due_date, 
remnant_assignment_priors.student_prior_assignments_started,
remnant_assignment_priors.student_prior_assignments_percent_completed,
remnant_assignment_priors.student_prior_median_ln_assignment_time_on_task,
remnant_problem_priors.student_prior_average_problems_per_assignment,
remnant_problem_priors.student_prior_median_ln_problem_time_on_task,
remnant_problem_priors.student_prior_median_ln_problem_first_response_time,
remnant_problem_priors.student_prior_average_problem_correctness,
remnant_problem_priors.student_prior_average_problem_attempt_count,
remnant_problem_priors.student_prior_average_problem_hint_count,
(remnant_target_alogs.end_time is not null)::int as assignment_completed, 
coalesce(remnant_target_problems_completed.problems_completed, 0) as problems_completed
from remnant_target_alogs 
left join remnant_target_problems_completed on remnant_target_problems_completed.assignment_log_id = remnant_target_alogs.assignment_log_id 
left join remnant_assignment_priors on remnant_assignment_priors.assignment_log_id = remnant_target_alogs.assignment_log_id 
left join remnant_problem_priors on remnant_problem_priors.assignment_log_id = remnant_target_alogs.assignment_log_id 
order by remnant_target_alogs.student_id, remnant_target_alogs.start_time; 










-- Experiment Actions

drop table if exists experiment_actions; 
create table experiment_actions as 
select 
experiment_input_alogs.student_id, 
extract(epoch from assignment_actions.timestamp)::int - (extract(epoch from assignment_actions.timestamp)::int % 86400) as timestamp, 
sum((assignment_actions.action_defn_type_id = 1)::int) as action_1_count, 
sum((assignment_actions.action_defn_type_id = 2)::int) as action_2_count, 
sum((assignment_actions.action_defn_type_id = 3)::int) as action_3_count, 
sum((assignment_actions.action_defn_type_id = 4)::int) as action_4_count, 
sum((assignment_actions.action_defn_type_id = 5)::int) as action_5_count, 
sum((assignment_actions.action_defn_type_id = 6)::int) as action_6_count, 
sum((assignment_actions.action_defn_type_id = 7)::int) as action_7_count, 
sum((assignment_actions.action_defn_type_id = 8)::int) as action_8_count, 
sum((assignment_actions.action_defn_type_id = 9)::int) as action_9_count, 
sum((assignment_actions.action_defn_type_id = 10)::int) as action_10_count, 
sum((assignment_actions.action_defn_type_id = 11)::int) as action_11_count, 
sum((assignment_actions.action_defn_type_id = 12)::int) as action_12_count, 
sum((assignment_actions.action_defn_type_id = 13)::int) as action_13_count, 
sum((assignment_actions.action_defn_type_id = 14)::int) as action_14_count, 
sum((assignment_actions.action_defn_type_id = 15)::int) as action_15_count, 
sum((assignment_actions.action_defn_type_id = 16)::int) as action_16_count, 
sum((assignment_actions.action_defn_type_id = 17)::int) as action_17_count, 
sum((assignment_actions.action_defn_type_id = 18)::int) as action_18_count, 
sum((assignment_actions.action_defn_type_id = 19)::int) as action_19_count, 
sum((assignment_actions.action_defn_type_id = 20 and coalesce(action_responses.correct, false))::int) as action_20a_count, 
sum((assignment_actions.action_defn_type_id = 20 and not coalesce(action_responses.correct, false))::int) as action_20b_count, 
sum((assignment_actions.action_defn_type_id = 21)::int) as action_21_count, 
sum((assignment_actions.action_defn_type_id = 22)::int) as action_22_count, 
sum((assignment_actions.action_defn_type_id = 23)::int) as action_23_count, 
sum((assignment_actions.action_defn_type_id = 24)::int) as action_24_count, 
sum((assignment_actions.action_defn_type_id = 25)::int) as action_25_count, 
sum((assignment_actions.action_defn_type_id = 26)::int) as action_26_count, 
sum((assignment_actions.action_defn_type_id = 27)::int) as action_27_count, 
sum((assignment_actions.action_defn_type_id = 28)::int) as action_28_count, 
sum((assignment_actions.action_defn_type_id = 29)::int) as action_29_count, 
sum((assignment_actions.action_defn_type_id = 30)::int) as action_30_count, 
sum((assignment_actions.action_defn_type_id = 31)::int) as action_31_count, 
sum((assignment_actions.action_defn_type_id = 32)::int) as action_32_count, 
sum((assignment_actions.action_defn_type_id = 33)::int) as action_33_count, 
sum((assignment_actions.action_defn_type_id = 34)::int) as action_34_count, 
sum((assignment_actions.action_defn_type_id = 35)::int) as action_35_count 
from student_data.assignment_actions 
left join student_data.problem_actions on problem_actions.id = assignment_actions.action_id 
left join student_data.action_responses on action_responses.id = problem_actions.action_details_id 
inner join experiment_input_alogs on experiment_input_alogs.assignment_log_id = assignment_actions.assignment_log_id 
group by experiment_input_alogs.student_id, extract(epoch from assignment_actions.timestamp)::int - (extract(epoch from assignment_actions.timestamp)::int % 86400); 



-- Experiment Assignment Level Features

drop table if exists experiment_assignment_level_agg_features; 
create table experiment_assignment_level_agg_features as 
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
	inner join (select distinct sequence_id from experiment_input_alogs) good_sequences on good_sequences.sequence_id = ordered_alogs.sequence_id
) good_logs
group by assignment_log_id; 

drop table if exists assignment_level_stats; 
create table assignment_level_stats as 
select 
assignments.sequence_id, 
avg(experiment_assignment_level_agg_features.session_count) as session_count_avg, 
coalesce(stddev(experiment_assignment_level_agg_features.session_count), 0) as session_count_stddev, 
avg(experiment_assignment_level_agg_features.day_count) as day_count_avg, 
coalesce(stddev(experiment_assignment_level_agg_features.day_count), 0) as day_count_stddev, 
avg(experiment_assignment_level_agg_features.completed_problem_count) as completed_problem_count_avg, 
coalesce(stddev(experiment_assignment_level_agg_features.completed_problem_count), 0) as completed_problem_count_stddev 
from experiment_assignment_level_agg_features 
inner join assignment_xrefs on assignment_xrefs.xid = experiment_assignment_level_agg_features.assignment_log_id 
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


drop table if exists experiment_assignment_level_features; 
create table experiment_assignment_level_features as 
select 
experiment_input_alogs.assignment_log_id, 
experiment_input_alogs.student_id, 
assignment_paths.directory_1,
assignment_paths.directory_2,
assignment_paths.directory_3,
assignment_paths.sequence_id,
extract(epoch from assignment_logs.start_time) as assignment_start_time, 
(sequences.parameters like '%pseudo_skill_builder%' or sections.type='MasterySection' or sections.type='LinearMasterySection')::int as is_skill_builder, 
(assignments.due_date is not null)::int as has_due_date, 
(assignment_logs.end_time is not null)::int as assignment_completed, 
ln(coalesce(extract(epoch from (assignment_logs.start_time - lag(assignment_logs.start_time, 1) over (partition by assignment_logs.user_xid order by assignment_logs.start_time))), extract(epoch from (assignment_logs.start_time - users.created))) + 0.00001) as time_since_last_assignment_start, 
experiment_assignment_level_agg_features.session_count as session_count_raw, 
case when assignment_level_stats.session_count_stddev = 0 then 0 else (experiment_assignment_level_agg_features.session_count - assignment_level_stats.session_count_avg) / assignment_level_stats.session_count_stddev end as session_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by case when assignment_level_stats.session_count_stddev = 0 then 0 else (experiment_assignment_level_agg_features.session_count - assignment_level_stats.session_count_avg) / assignment_level_stats.session_count_stddev end) as session_count_class_percentile, 
experiment_assignment_level_agg_features.day_count as day_count_raw, 
case when assignment_level_stats.day_count_stddev = 0 then 0 else (experiment_assignment_level_agg_features.day_count - assignment_level_stats.day_count_avg) / assignment_level_stats.day_count_stddev end as day_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by case when assignment_level_stats.day_count_stddev = 0 then 0 else (experiment_assignment_level_agg_features.day_count - assignment_level_stats.day_count_avg) / assignment_level_stats.day_count_stddev end) as day_count_class_percentile, 
experiment_assignment_level_agg_features.completed_problem_count as completed_problem_count_raw, 
case when assignment_level_stats.completed_problem_count_stddev = 0 then 0 else (experiment_assignment_level_agg_features.completed_problem_count - assignment_level_stats.completed_problem_count_avg) / assignment_level_stats.completed_problem_count_stddev end as completed_problem_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by case when assignment_level_stats.completed_problem_count_stddev = 0 then 0 else (experiment_assignment_level_agg_features.completed_problem_count - assignment_level_stats.completed_problem_count_avg) / assignment_level_stats.completed_problem_count_stddev end) as completed_problem_count_class_percentile 
from student_data.assignment_logs 
inner join experiment_input_alogs on experiment_input_alogs.assignment_log_id = assignment_logs.id 
inner join users.users on users.id = experiment_input_alogs.student_id 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join legacy.sequences on sequences.id = assignments.sequence_id 
inner join legacy.sections on sections.id = sequences.head_section_id 
inner join experiment_assignment_level_agg_features on experiment_assignment_level_agg_features.assignment_log_id = assignment_logs.id 
inner join assignment_level_stats on assignment_level_stats.sequence_id = sequences.id
left join assignment_paths on assignment_paths.sequence_id = sequences.id; 


-- Experiment Problem Level Features

drop table if exists experiment_problem_level_stats;
create table experiment_problem_level_stats as 
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
inner join (select distinct sequence_id from experiment_input_alogs) good_sequences on good_sequences.sequence_id = ordered_alogs.sequence_id
inner join ln_medians on ln_medians.pid = problem_logs.problem_id 
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by problem_logs.problem_id, ln_medians.time_on_task_med, ln_medians.first_response_time_med; 


drop table if exists experiment_problem_level_features; 
create table experiment_problem_level_features as 
select 
experiment_input_alogs.assignment_log_id as assignment_log_id, 
median(ln(extract(epoch from (problem_logs.end_time - problem_logs.start_time)))::numeric) as median_ln_problem_time_on_task_raw, 
median((case when experiment_problem_level_stats.time_on_task_mad = 0 then 0 else (ln(extract(epoch from (problem_logs.end_time - problem_logs.start_time))) - experiment_problem_level_stats.time_on_task_med) / experiment_problem_level_stats.time_on_task_mad end)::numeric) as median_ln_problem_time_on_task_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by median((case when experiment_problem_level_stats.time_on_task_mad = 0 then 0 else (ln(extract(epoch from (problem_logs.end_time - problem_logs.start_time))) - experiment_problem_level_stats.time_on_task_med) / experiment_problem_level_stats.time_on_task_mad end)::numeric)) as median_ln_problem_time_on_task_class_percentile, 
median(ln(problem_logs.first_response_time::float / 1000)::numeric) as median_ln_problem_first_response_time_raw, 
median((case when experiment_problem_level_stats.first_response_time_mad = 0 then 0 else (ln(problem_logs.first_response_time::float / 1000) - experiment_problem_level_stats.first_response_time_med) / experiment_problem_level_stats.first_response_time_mad end)::numeric) as median_ln_problem_first_response_time_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by median((case when experiment_problem_level_stats.first_response_time_mad = 0 then 0 else (ln(problem_logs.first_response_time::float / 1000) - experiment_problem_level_stats.first_response_time_med) / experiment_problem_level_stats.first_response_time_mad end)::numeric)) as median_ln_problem_first_response_time_class_percentile,
avg(problem_logs.attempt_count) as average_problem_attempt_count, 
avg(case when experiment_problem_level_stats.attempt_count_stddev = 0 then 0 else (problem_logs.attempt_count - experiment_problem_level_stats.attempt_count_avg) / experiment_problem_level_stats.attempt_count_stddev end) as average_problem_attempt_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when experiment_problem_level_stats.attempt_count_stddev = 0 then 0 else (problem_logs.attempt_count - experiment_problem_level_stats.attempt_count_avg) / experiment_problem_level_stats.attempt_count_stddev end)) as average_problem_attempt_count_class_percentile, 
avg((problem_logs.first_action_type_id = 1)::int) as average_problem_answer_first, 
avg(case when experiment_problem_level_stats.answer_first_stddev = 0 then 0 else ((problem_logs.first_action_type_id = 1)::int - experiment_problem_level_stats.answer_first_avg) / experiment_problem_level_stats.answer_first_stddev end) as average_problem_answer_first_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when experiment_problem_level_stats.answer_first_stddev = 0 then 0 else ((problem_logs.first_action_type_id = 1)::int - experiment_problem_level_stats.answer_first_avg) / experiment_problem_level_stats.answer_first_stddev end)) as average_problem_answer_first_class_percentile, 
coalesce(avg((problem_logs.discrete_score = 1)::int), 1) as average_problem_correctness, 
coalesce(avg(case when experiment_problem_level_stats.correctness_stddev = 0 then 0 else ((problem_logs.discrete_score = 1)::int - experiment_problem_level_stats.correctness_avg) / experiment_problem_level_stats.correctness_stddev end), 0) as average_problem_correctness_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when experiment_problem_level_stats.correctness_stddev = 0 then 0 else ((problem_logs.discrete_score = 1)::int - experiment_problem_level_stats.correctness_avg) / experiment_problem_level_stats.correctness_stddev end)) as average_problem_correctness_class_percentile, 
avg(problem_logs.hint_count) as average_problem_hint_count, 
avg(case when experiment_problem_level_stats.hint_count_stddev = 0 then 0 else (problem_logs.hint_count - experiment_problem_level_stats.hint_count_avg) / experiment_problem_level_stats.hint_count_stddev end) as average_problem_hint_count_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when experiment_problem_level_stats.hint_count_stddev = 0 then 0 else (problem_logs.hint_count - experiment_problem_level_stats.hint_count_avg) / experiment_problem_level_stats.hint_count_stddev end)) as average_problem_hint_count_class_percentile, 
avg((problem_logs.bottom_hint)::int) as average_problem_answer_given, 
avg(case when experiment_problem_level_stats.answer_given_stddev = 0 then 0 else ((problem_logs.bottom_hint)::int - experiment_problem_level_stats.answer_given_avg) / experiment_problem_level_stats.answer_given_stddev end) as average_problem_answer_given_normalized, 
percent_rank() over (partition by assignments.group_context_xid order by avg(case when experiment_problem_level_stats.answer_given_stddev = 0 then 0 else ((problem_logs.bottom_hint)::int - experiment_problem_level_stats.answer_given_avg) / experiment_problem_level_stats.answer_given_stddev end)) as average_problem_answer_given_class_percentile 
from student_data.problem_logs 
inner join experiment_input_alogs on experiment_input_alogs.assignment_log_id = problem_logs.assignment_log_id 
inner join student_data.assignment_logs on assignment_logs.id = problem_logs.assignment_log_id 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id 
inner join experiment_problem_level_stats on experiment_problem_level_stats.problem_id = problem_logs.problem_id 
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by experiment_input_alogs.assignment_log_id, assignments.group_context_xid; 



-- Experiment Inputs

drop table if exists experiment_inputs; 
create table experiment_inputs as 
select 
experiment_assignment_level_features.student_id,
experiment_assignment_level_features.assignment_start_time,
experiment_assignment_level_features.directory_1,
experiment_assignment_level_features.directory_2,
experiment_assignment_level_features.directory_3,
experiment_assignment_level_features.sequence_id,
experiment_assignment_level_features.is_skill_builder,
experiment_assignment_level_features.has_due_date,
experiment_assignment_level_features.assignment_completed,
experiment_assignment_level_features.time_since_last_assignment_start,
experiment_assignment_level_features.session_count_raw,
experiment_assignment_level_features.session_count_normalized,
experiment_assignment_level_features.session_count_class_percentile,
experiment_assignment_level_features.day_count_raw,
experiment_assignment_level_features.day_count_normalized,
experiment_assignment_level_features.day_count_class_percentile,
experiment_assignment_level_features.completed_problem_count_raw,
experiment_assignment_level_features.completed_problem_count_normalized,
experiment_assignment_level_features.completed_problem_count_class_percentile,
experiment_problem_level_features.median_ln_problem_time_on_task_raw,
experiment_problem_level_features.median_ln_problem_time_on_task_normalized,
experiment_problem_level_features.median_ln_problem_time_on_task_class_percentile,
experiment_problem_level_features.median_ln_problem_first_response_time_raw,
experiment_problem_level_features.median_ln_problem_first_response_time_normalized,
experiment_problem_level_features.median_ln_problem_first_response_time_class_percentile, 
experiment_problem_level_features.average_problem_attempt_count, 
experiment_problem_level_features.average_problem_attempt_count_normalized, 
experiment_problem_level_features.average_problem_attempt_count_class_percentile, 
experiment_problem_level_features.average_problem_answer_first, 
experiment_problem_level_features.average_problem_answer_first_normalized, 
experiment_problem_level_features.average_problem_answer_first_class_percentile, 
experiment_problem_level_features.average_problem_correctness, 
experiment_problem_level_features.average_problem_correctness_normalized, 
experiment_problem_level_features.average_problem_correctness_class_percentile, 
experiment_problem_level_features.average_problem_hint_count, 
experiment_problem_level_features.average_problem_hint_count_normalized, 
experiment_problem_level_features.average_problem_hint_count_class_percentile, 
experiment_problem_level_features.average_problem_answer_given, 
experiment_problem_level_features.average_problem_answer_given_normalized, 
experiment_problem_level_features.average_problem_answer_given_class_percentile 
from experiment_assignment_level_features 
left join experiment_problem_level_features on experiment_assignment_level_features.assignment_log_id = experiment_problem_level_features.assignment_log_id 
order by experiment_assignment_level_features.student_id, experiment_assignment_level_features.assignment_start_time; 



-- Experiment Targets

drop table if exists experiment_assignment_priors;
create table experiment_assignment_priors as
select 
experiment_target_alogs.assignment_log_id, 
count(assignment_logs.start_time) as student_prior_assignments_started, 
count(assignment_logs.end_time)::real / count(assignment_logs.start_time) as student_prior_assignments_percent_completed,
ln(median(extract(epoch from assignment_logs.end_time - assignment_logs.start_time)::numeric) + 0.00001) as student_prior_median_ln_assignment_time_on_task
from experiment_target_alogs
inner join student_xrefs on student_xrefs.id = experiment_target_alogs.student_id
inner join student_data.assignment_logs on assignment_logs.user_xid = student_xrefs.xid and assignment_logs.start_time < experiment_target_alogs.start_time
group by experiment_target_alogs.assignment_log_id;


drop table if exists experiment_problem_priors;
create table experiment_problem_priors as
select 
experiment_target_alogs.assignment_log_id, 
count(problem_logs.start_time)::real / count(distinct assignment_logs.start_time) as student_prior_average_problems_per_assignment, 
ln(median(extract(epoch from (problem_logs.end_time - problem_logs.start_time))::numeric) + 0.00001) as student_prior_median_ln_problem_time_on_task, 
ln(median((problem_logs.first_response_time::float / 1000)::numeric) + 0.00001) as student_prior_median_ln_problem_first_response_time, 
avg(problem_logs.discrete_score) as student_prior_average_problem_correctness, 
avg(problem_logs.attempt_count) as student_prior_average_problem_attempt_count, 
avg(problem_logs.hint_count) as student_prior_average_problem_hint_count
from experiment_target_alogs
inner join student_xrefs on student_xrefs.id = experiment_target_alogs.student_id
inner join student_data.assignment_logs on assignment_logs.user_xid = student_xrefs.xid and assignment_logs.start_time < experiment_target_alogs.start_time
inner join student_data.problem_logs on problem_logs.assignment_log_id = assignment_logs.id
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by experiment_target_alogs.assignment_log_id;


drop table if exists experiment_target_problems_completed;
create table experiment_target_problems_completed as
select
experiment_target_alogs.assignment_log_id,
count(*) as problems_completed 
from experiment_target_alogs 
left join student_data.problem_logs on problem_logs.assignment_log_id = experiment_target_alogs.assignment_log_id 
where problem_logs.end_time is not null 
and problem_logs.first_response_time is not null 
and problem_logs.path_info not like '%SP%' 
group by experiment_target_alogs.assignment_log_id;


drop table if exists experiment_targets; 
create table experiment_targets as 
select 
experiment_target_alogs.class_id, 
experiment_target_alogs.student_id, 
extract(epoch from experiment_target_alogs.start_time) as assignment_start_time, 
encode_ceri('PS', experiment_target_alogs.sequence_id) as target_sequence, 
experiment_target_alogs.has_due_date, 
experiment_assignment_priors.student_prior_assignments_started,
experiment_assignment_priors.student_prior_assignments_percent_completed,
experiment_assignment_priors.student_prior_median_ln_assignment_time_on_task,
experiment_problem_priors.student_prior_average_problems_per_assignment,
experiment_problem_priors.student_prior_median_ln_problem_time_on_task,
experiment_problem_priors.student_prior_median_ln_problem_first_response_time,
experiment_problem_priors.student_prior_average_problem_correctness,
experiment_problem_priors.student_prior_average_problem_attempt_count,
experiment_problem_priors.student_prior_average_problem_hint_count,
(experiment_target_alogs.end_time is not null)::int as assignment_completed, 
coalesce(experiment_target_problems_completed.problems_completed, 0) as problems_completed
from experiment_target_alogs 
left join experiment_target_problems_completed on experiment_target_problems_completed.assignment_log_id = experiment_target_alogs.assignment_log_id 
left join experiment_assignment_priors on experiment_assignment_priors.assignment_log_id = experiment_target_alogs.assignment_log_id 
left join experiment_problem_priors on experiment_problem_priors.assignment_log_id = experiment_target_alogs.assignment_log_id 
order by experiment_target_alogs.student_id, experiment_target_alogs.start_time; 





