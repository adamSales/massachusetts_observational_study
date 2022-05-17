-- exp_norm_map.csv

create or replace view decoded_map as 
select 
decode_ceri(experiment_id) as eid, 
decode_ceri("Original Skill Builders") as osb
from exp_norm_map_with_errors_csv enmc;

select 
s1.name, 
s2.name, 
encode_ceri('PS', s1.id),
encode_ceri('PS', s2.id) 
from decoded_map dm
inner join legacy.sequences s1 on s1.id  = dm.eid
inner join legacy.sequences s2 on s2.id = dm.osb;



-- normal_skill_builders_in_folder.csv

with recursive recursed_folders(parent_id, child_id, dir) as
(
	select 
		f1.parent_id as parent_id, 
		f1.id as child_id, 
		f1.name as dir
	from legacy.folders f1
	where id in (select id from legacy.folders where parent_id in (86954))
	union all
	(
		select 
			f2.parent_id as parent_id, 
			f2.id as child_id, 
			(rf.dir || '/' || f2.name)::varchar(255) as dir
		from legacy.folders f2, recursed_folders rf
		where f2.parent_id = rf.child_id
	)
)
select sequences.name, encode_ceri('PS', sequences.id) as sequence_id
from recursed_folders
inner join legacy.folder_items on folder_items.folder_id = recursed_folders.child_id
inner join legacy.curriculum_items on curriculum_items.id = folder_items.item_id
inner join legacy.sequences on sequences.id = curriculum_items.sequence_id
where folder_items.item_type = 'CurriculumItem'
and recursed_folders.dir ilike '%Skill Builders%'
and sequences.name not ilike '% ex';


-- normal_replaced_skill_builders.csv

select 
name, 
encode_ceri('PS', id) as sequence_id
from legacy.sequences 
where encode_ceri('PS', id) in ('PSAKH7','PSABK2K','PSAPN4','PSATE7','PSABHUM','PSAZ2G4','PSAMV6','PSAK6J','PSAGKN','PSAVUMD','PSABKKM','PSAVUGR','PSAVUK8','PSAVTVJ','PSABKKW','PSAVUK5','PSACUED','PSAKKV','PSABMSW','PSAHQ6','PSABKJ5','PSAVUF5','PSAVUBA','PSAVUME','PSAVUK7','PSA2H6H','PSAMYE','PSAVUK9','PSABJP4','PSAKWG','PSAK4D','PSAVUF3','PSAG5K5','PSAHEA','PSAVUMA','PSAVUGQ','PSAVUAR','PSABJC8','PSAGGQ','PSA2H6E','PSAHKE','PSAGKY','PSAMV6','PSAVUMN','PSAVUMK','PSAVUMJ','PSAFTFM','PSAVUA6','PSAGFD','PSAKHX','PSAWU7D','PSAVUGQ','PSAPN4','PSAP8W','PSAJ78','PSAHK8','PSAHKRU','PSAHJ9J','PSAV9DV','PSAVUF8','PSAVUF5','PSAVTVX','PSAVUFX','PSAVUF3','PSAKWG','PSAVUGT','PSABK27','PSAFTFJ','PSAHS6','PSAVUMH','PSA4D9T','PSAVUGJ','PSAVUGX','PSAXQMQ','PSAGH7','PSABKJ9','PSAHK8','PSAHSD','PSAVTUA','PSAVTJ5','PSAG5N7','PSAD9C6','PSAVTUU','PSAX692','PSAVUGV','PSA2H6G','PSAVTVP','PSAVUGF','PSAVUGK','PSAGFD','PSANGG','PSAVTV6','PSAVUMC','PSAXWCT','PSAVTUH','PSAVTVR','PSAVUGH','PSAGF4','PSAVUGD','PSAX7DZ','PSAX7E9','PSAVUF6','PSAVTT9','PSAVTUH','PSAXVDX','PSAVTT8','PSABFTC','PSAJ4ZU','PSAVUGM','PSAVTVM','PSAJ4YN','PSAP35H','PSAW5Q9','PSAVUF9','PSAWU7D','PSAVUF2','NA','PSAP8V','PSADAWC','PSAVUGC','PSAVTK2','PSAV9DV','PSAGGU','PSAKKC','PSAG5PY','PSAQZTJ','PSAVUA4','PSAJY7C','PSAVUGP','PSAVUGA','PSAVUGY','PSAHGJ','PSABTKXQ','PSAVUBC','PSAMYE','PSAGEJ','PSAKKY','PSAVUF3','PSA2H6G')
and name not ilike '% ex';


-- all student conditions

drop schema if exists exp_data cascade; 
create schema exp_data; 

create or replace view exp_data.student_xrefs as 
select id as xid, target_id as id, xref 
from core.external_references 
where xref_type_id=1 
and partner_id=5 
and external_references.id not in (select user_xid from users.user_roles where user_role_definition_id != 6);

create or replace view exp_data.assignment_xrefs as 
select id as xid, target_id as id, xref 
from core.external_references 
where xref_type_id=3 
and partner_id=5;

drop table if exists exp_data.first_alogs; 
create table exp_data.first_alogs as 
with log_orders as 
( 
	select assignment_logs.*, 
	encode_ceri('PS', assignments.sequence_id) as sequence_id, 
	student_xrefs.id as user_id, 
	row_number() over(partition by assignment_logs.user_xid, assignments.sequence_id order by assignment_logs.start_time) as log_order 
	from student_data.assignment_logs 
	inner join public.assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
	inner join public.student_xrefs on student_xrefs.xid = assignment_logs.user_xid 
	inner join core.assignments on assignments.id = assignment_xrefs.id 
	where encode_ceri('PS', assignments.sequence_id) in ('PSA25TA','PSA29FC','PSA2KKV','PSA2KKZ','PSA2KMP','PSA2KMQ','PSA2KNM','PSA2KNP','PSA2KNR','PSA2KNW','PSA2KNX','PSA2KP9','PSA2KPT','PSA2KPV','PSA2KQB','PSA4MS2','PSA52JK','PSA55VC','PSA59TP','PSA59TQ','PSA59VC','PSA6DUN','PSA7GUA','PSA98J7','PSA9XWV','PSABANRN','PSABARYW','PSABCXRT','PSABF5JC','PSAGF4','PSAH9CV','PSAHQV','PSAJ2EE','PSAJ43P','PSAJ4YN','PSAJ4ZU','PSAJ4ZZ','PSAJDQG','PSAJDQJ','PSAJJXN','PSAJVP8','PSAJVPW','PSAJY7C','PSAKUSU','PSAM4NK','PSAMC2V','PSAMGHG','PSAMQJD','PSAMR8Z','PSAQJFP','PSAR5JG','PSAR9Y9','PSARZX2','PSAS25R','PSASA4B','PSASA67','PSASDZY','PSASRKH','PSATNB2','PSATNCQ','PSATP2Z','PSATZEJ','PSAU4JD','PSAU5XF','PSAU6Y4','PSAU7GZ','PSAU85Y','PSAU88D','PSAUK57','PSAUKPM','PSAUKPR','PSAUTWT','PSAUTWU','PSAUUKY','PSAV89B','PSAVDFE','PSAVK69','PSAVTMK','PSAWHF4','PSAWU6Z','PSAXBAF','PSAXD6K','PSAXJC2','PSAXP7W','PSAXTEE','PSAYCFH','PSAZ2G4','PSAZ5HX','PSAZGQM') 
) 
select * from log_orders 
where log_order = 1; 

select 
first_alogs.id as assignment_log_id, 
first_alogs.sequence_id, 
first_alogs.user_id, 
max((concat(s1.name, s2.name, s3.name, s4.name, s5.name) ilike '%[control%')::int) as in_control, 
max((concat(s1.name, s2.name, s3.name, s4.name, s5.name) ilike '%[treatment%')::int) as in_treatment 
from exp_data.first_alogs 
inner join student_data.problem_logs on problem_logs.assignment_log_id = first_alogs.id 
left join legacy.sections as s1 on s1.id = ('0' || split_part(btrim(replace(split_part(problem_logs.path_info, 'LPR', 1), 'LPS', ''), '/'), '/', 1))::int 
left join legacy.sections as s2 on s2.id = ('0' || split_part(btrim(replace(split_part(problem_logs.path_info, 'LPR', 1), 'LPS', ''), '/'), '/', 2))::int 
left join legacy.sections as s3 on s3.id = ('0' || split_part(btrim(replace(split_part(problem_logs.path_info, 'LPR', 1), 'LPS', ''), '/'), '/', 3))::int 
left join legacy.sections as s4 on s4.id = ('0' || split_part(btrim(replace(split_part(problem_logs.path_info, 'LPR', 1), 'LPS', ''), '/'), '/', 4))::int 
left join legacy.sections as s5 on s5.id = ('0' || split_part(btrim(replace(split_part(problem_logs.path_info, 'LPR', 1), 'LPS', ''), '/'), '/', 5))::int 
group by first_alogs.id, first_alogs.sequence_id, first_alogs.user_id; 
