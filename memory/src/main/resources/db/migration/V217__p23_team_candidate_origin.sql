-- P23 候选来源归 Memory 所有，机器修订保持可追溯且不经过可手工发布的 DRAFT。
alter table memory.experience_card_revision
    add column origin_candidate_id uuid,
    add column origin_candidate_revision integer,
    add column origin_card_version bigint,
    add column origin_base_revision integer,
    add column origin_base_memory_version varchar(40),
    add column origin_content_hash varchar(64),
    add column withdrawal_card_version bigint,
    add constraint experience_revision_candidate_origin_check check (
        (origin_candidate_id is null and origin_candidate_revision is null and origin_card_version is null
            and origin_base_revision is null and origin_base_memory_version is null and origin_content_hash is null
            and withdrawal_card_version is null)
        or (origin_candidate_id is not null and origin_candidate_revision > 0 and origin_card_version > 0
            and origin_base_revision > 0 and origin_base_memory_version is not null
            and origin_content_hash ~ '^[0-9a-f]{64}$'
            and (withdrawal_card_version is null or withdrawal_card_version > 0))
    );

create unique index experience_revision_candidate_origin_uq
    on memory.experience_card_revision(origin_candidate_id, origin_candidate_revision)
    where origin_candidate_id is not null;
