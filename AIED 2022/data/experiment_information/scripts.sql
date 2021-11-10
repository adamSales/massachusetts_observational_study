-- exp_norm_map.csv

create or replace view decoded_map as 
select decode_ceri(experiment_id) as eid, 
decode_ceri("Original Skill Builders") as osb
from exp_norm_map_with_errors_csv enmc;

select 
s1.name, 
s2.name, 
encode_ceri('PS', s1.id),
encode_ceri('PS', s1.id) from decoded_map dm
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