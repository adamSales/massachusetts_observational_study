select
concat(school_details.state_school_id, '') as school_id,
count(distinct teacher_users.id) as teacher_count,
count(distinct student_classes.id) as class_count,
count(distinct assignment_logs.user_id) as user_count,
count(distinct assignment_logs.id) as assignment_log_count
from legacy.assignment_logs
inner join legacy.class_assignments on class_assignments.id = assignment_logs.assignment_id
inner join legacy.student_classes on student_classes.id = class_assignments.student_class_id
inner join legacy.teacher_classes on teacher_classes.student_class_id = student_classes.id
inner join legacy.users as teacher_users on teacher_users.id = teacher_classes.teacher_id
inner join legacy.user_roles on user_roles.user_id = teacher_users.id
inner join legacy.school_details on school_details.school_id = user_roles.location_id
where user_roles.location_type = 'School'
and assignment_logs.start_time > 'START_YEAR-09-01'
and assignment_logs.start_time < 'END_YEAR-06-01'
and school_details.state = 'MA'
group by school_details.state_school_id;
