alter table learning.candidate_iteration drop constraint candidate_iteration_target_type_check;
alter table learning.candidate_iteration add constraint candidate_iteration_target_type_check
    check (target_type in ('KNOWLEDGE_UPDATE', 'MEMORY_UPSERT', 'TEAM_EXPERIENCE_UPDATE'));
