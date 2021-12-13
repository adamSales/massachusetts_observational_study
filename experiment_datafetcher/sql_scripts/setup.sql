-- Student and Teacher Action Tables
drop table if exists student_action_map;
create table student_action_map (raw_action int, action varchar(64));
insert into student_action_map (raw_action, action) values 
(1, 'assignment_started'), (2, 'assignment_resumed'), (3, 'assignment_finished'), (10, 'problem_started'), 
(12, 'problem_finished'), (16, 'hint_requested'), (17, 'scaffolding_requested'), (18, 'url_requested'), 
(19, 'explanation_requested'), (20, 'closed_response'), (21, 'open_response'), (22, 'work_submitted'), 
(24, 'peer_review_requested'), (25, 'answer_requested'), (26, 'continue_selected'), (30,'answer_requested'); 

-- Xref Views
create or replace view student_xrefs as 
select id as xid, target_id as id, xref 
from core.external_references 
where xref_type_id=1 
and partner_id=5 
and external_references.id not in (select user_xid from users.user_roles where user_role_definition_id != 6);

create or replace view teacher_xrefs as 
select id as xid, target_id as id, xref 
from core.external_references 
where xref_type_id=1 
and partner_id=5 
and external_references.id in (select user_xid from users.user_roles where user_role_definition_id = 7);

create or replace view assignment_xrefs as 
select id as xid, target_id as id, xref 
from core.external_references 
where xref_type_id=3 
and partner_id=5;

create or replace view class_xrefs as 
select id as xid, target_id as id, xref 
from core.external_references 
where xref_type_id=2 
and partner_id=5;
