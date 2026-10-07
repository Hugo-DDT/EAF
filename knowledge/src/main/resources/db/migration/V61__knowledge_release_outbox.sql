-- Knowledge 发布事实同时保留来源修订和正文摘要，供 Learning 崩溃后按来源键查询。
alter table knowledge.publication_event add column release_origin varchar(32) not null default 'OWNER'
    check (release_origin in ('OWNER', 'CONTROLLED_SEED', 'LEARNING_CANDIDATE'));
alter table knowledge.publication_event add column candidate_id uuid;
alter table knowledge.publication_event add column candidate_revision integer;
alter table knowledge.publication_event add column base_version integer;
alter table knowledge.publication_event add column content_hash char(64);

update knowledge.publication_event e
set content_hash = v.content_hash
from knowledge.document_version v
where v.tenant_id = e.tenant_id and v.workspace_id = e.workspace_id
  and v.document_id = e.document_id and v.asset_version = e.asset_version;

alter table knowledge.publication_event alter column content_hash set not null;
alter table knowledge.publication_event add constraint knowledge_publication_candidate_pair
    check ((candidate_id is null) = (candidate_revision is null)
       and (candidate_revision is null or candidate_revision > 0));
create unique index knowledge_candidate_release_unique
    on knowledge.publication_event(tenant_id, workspace_id, candidate_id, candidate_revision)
    where candidate_id is not null and action = 'PUBLISHED';

-- Outbox 与发布/撤回事实同事务写入；消费和有界重试由后续任务接入。
create table knowledge.outbox (
    event_id uuid primary key references knowledge.publication_event(id),
    event_type varchar(80) not null check (event_type in ('eaf.knowledge.version-published.v1', 'eaf.knowledge.version-revoked.v1')),
    payload jsonb not null check (jsonb_typeof(payload) = 'object'),
    status varchar(20) not null default 'PENDING' check (status in ('PENDING', 'DELIVERED', 'FAILED')),
    attempt_count integer not null default 0 check (attempt_count >= 0),
    next_attempt_at timestamptz,
    created_at timestamptz not null default now()
);
