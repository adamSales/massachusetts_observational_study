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


drop view if exists assignment_links; 
create or replace view assignment_links as 
select 
encode_ceri('PS', assignments.sequence_id) as encoded_sequence_id, 
student_xrefs.id as student_id, 
assignment_logs.start_time, 
assignment_logs.end_time 
from student_data.assignment_logs 
inner join student_xrefs on student_xrefs.xid = assignment_logs.user_xid 
inner join assignment_xrefs on assignment_xrefs.xid = assignment_logs.assignment_xid 
inner join core.assignments on assignments.id = assignment_xrefs.id; 

select * from assignment_links; 

select * 
from public.reloop_test
inner join core.assignments as exp_assignments 
	on encode_ceri('PS', exp_assignments.sequence_id) = reloop_test.exp_id 
inner join core.assignments as norm_assignments on encode_ceri('PS', norm_assignments.sequence_id) = reloop_test.norm_id 
select encode_ceri('PS', sequence_id)
