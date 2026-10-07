-- Memory 发布事实预留 Learning 来源键，并固定发布内容摘要便于恢复核对。
alter table memory.release add column release_origin varchar(32) not null default 'OWNER'
    check (release_origin in ('OWNER', 'CONTROLLED_SEED', 'LEARNING_CANDIDATE'));
alter table memory.release add column candidate_id uuid;
alter table memory.release add column candidate_revision integer;
alter table memory.release add column base_version varchar(40);
alter table memory.release add column content_hash char(64);
alter table memory.release add constraint memory_release_candidate_pair
    check ((candidate_id is null) = (candidate_revision is null)
       and (candidate_revision is null or candidate_revision > 0));
create unique index memory_candidate_release_unique
    on memory.release(tenant_id, workspace_id, candidate_id, candidate_revision)
    where candidate_id is not null and action = 'PUBLISHED';

create table memory.outbox (
    event_id uuid primary key references memory.release(release_id),
    event_type varchar(80) not null check (event_type in ('eaf.memory.version-published.v1', 'eaf.memory.version-revoked.v1')),
    payload jsonb not null check (jsonb_typeof(payload) = 'object'),
    status varchar(20) not null default 'PENDING' check (status in ('PENDING', 'DELIVERED', 'FAILED')),
    attempt_count integer not null default 0 check (attempt_count >= 0),
    next_attempt_at timestamptz,
    created_at timestamptz not null default now()
);
