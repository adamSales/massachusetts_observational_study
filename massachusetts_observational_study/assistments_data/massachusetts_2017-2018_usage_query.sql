select 
concat(school_details.state_school_id, '') as school_id,
concat(school_details.state_district_id, '0000') as district_id,
count(distinct users.id) as teacher_count,
count(distinct student_classes.id) as class_count,
count(distinct class_assignments.id) as assignment_count
from legacy.assignment_logs
left join legacy.class_assignments on class_assignments.id = assignment_logs.assignment_id
left join legacy.student_classes on student_classes.id = class_assignments.student_class_id
left join legacy.teacher_classes on teacher_classes.student_class_id = student_classes.id
left join legacy.users on users.id = teacher_classes.teacher_id
left join legacy.user_roles on user_roles.user_id = users.id
left join legacy.school_details on school_details.school_id = user_roles.location_id
left join legacy.district_details on district_details.id = school_details.district_detail_id
where class_assignments.release_date >= '2017-09-01'
and class_assignments.release_date < '2018-07-01'
and user_roles.location_type = 'School'
and school_details.state = 'MA'
group by school_details.state_school_id, school_details.state_district_id;
