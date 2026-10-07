-- 任务只存本次选择的 Capability/Skill 精确版本及内容摘要，不建立到其他 schema 的外键。
alter table task.task
    add column capability_id uuid,
    add column capability_version varchar(40),
    add column capability_hash varchar(64),
    add column skill_id uuid,
    add column skill_version varchar(40),
    add column skill_hash varchar(64),
    add constraint task_asset_binding_all_or_none check (
        (capability_id is null and capability_version is null and capability_hash is null and skill_id is null and skill_version is null and skill_hash is null)
        or
        (capability_id is not null and capability_version is not null and length(capability_hash) = 64
            and skill_id is not null and skill_version is not null and length(skill_hash) = 64)
    );
-- 本文件负责 EAF 的 V57__task_capability_snapshot.sql 相关定义。
