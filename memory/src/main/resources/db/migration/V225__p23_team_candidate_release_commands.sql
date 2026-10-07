-- P23 在 Memory 本域记录候选发布与撤回幂等事实。
alter table memory.experience_command alter column action type varchar(32);
alter table memory.experience_command drop constraint experience_command_action_check;
alter table memory.experience_command add constraint experience_command_action_check
    check (action in ('CREATE', 'SAVE', 'PUBLISH', 'REVOKE', 'PUBLISH_CANDIDATE', 'REVOKE_CANDIDATE'));
