drop schema if exists reloop cascade;
create schema reloop;


drop table if exists reloop.pre_start;
create table reloop.pre_start as
select
concat(school_details.state_school_id, '') as school_id,
count(distinct teacher_users.id) as pre_2015_05_01__teacher_count,
count(distinct student_classes.id) as pre_2015_05_01__class_count,
count(distinct assignment_logs.user_id) as pre_2015_05_01__user_count,
count(distinct assignment_logs.id) as pre_2015_05_01__assignment_log_count
from legacy.assignment_logs
inner join legacy.class_assignments on class_assignments.id = assignment_logs.assignment_id
inner join legacy.student_classes on student_classes.id = class_assignments.student_class_id
inner join legacy.teacher_classes on teacher_classes.student_class_id = student_classes.id
inner join legacy.users as teacher_users on teacher_users.id = teacher_classes.teacher_id
inner join legacy.user_roles on user_roles.user_id = teacher_users.id
inner join legacy.school_details on school_details.school_id = user_roles.location_id
where user_roles.location_type = 'School'
and assignment_logs.start_time < '2015-05-01'
and school_details.state = 'MA'
group by school_details.state_school_id;


drop table if exists reloop.post_start__pre_end;
create table reloop.post_start__pre_end as
select
concat(school_details.state_school_id, '') as school_id,
count(distinct teacher_users.id) as post_2015_05_01__pre_2018_01_01__teacher_count,
count(distinct student_classes.id) as post_2015_05_01__pre_2018_01_01__class_count,
count(distinct assignment_logs.user_id) as post_2015_05_01__pre_2018_01_01__user_count,
count(distinct assignment_logs.id) as post_2015_05_01__pre_2018_01_01__assignment_log_count
from legacy.assignment_logs
inner join legacy.class_assignments on class_assignments.id = assignment_logs.assignment_id
inner join legacy.student_classes on student_classes.id = class_assignments.student_class_id
inner join legacy.teacher_classes on teacher_classes.student_class_id = student_classes.id
inner join legacy.users as teacher_users on teacher_users.id = teacher_classes.teacher_id
inner join legacy.user_roles on user_roles.user_id = teacher_users.id
inner join legacy.school_details on school_details.school_id = user_roles.location_id
where user_roles.location_type = 'School'
and assignment_logs.start_time > '2015-05-01'
and assignment_logs.start_time < '2018-01-01'
and school_details.state = 'MA'
group by school_details.state_school_id;


select 
post_start__pre_end.school_id,
coalesce(pre_2015_05_01__teacher_count, 0) as pre_2015_05_01__teacher_count,
post_2015_05_01__pre_2018_01_01__teacher_count,
coalesce(pre_2015_05_01__class_count, 0) as pre_2015_05_01__class_count,
post_2015_05_01__pre_2018_01_01__class_count,
coalesce(pre_2015_05_01__user_count, 0) as pre_2015_05_01__user_count,
post_2015_05_01__pre_2018_01_01__user_count,
coalesce(pre_2015_05_01__assignment_log_count, 0) as pre_2015_05_01__assignment_log_count,
post_2015_05_01__pre_2018_01_01__assignment_log_count
from reloop.post_start__pre_end
left join reloop.pre_start on pre_start.school_id = post_start__pre_end.school_id
order by post_2015_05_01__pre_2018_01_01__assignment_log_count desc;


