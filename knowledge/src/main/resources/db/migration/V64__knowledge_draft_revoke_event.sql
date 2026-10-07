-- 当前版本级撤回后文档可准备新版本，发布事件需记录这一真实状态。
alter table knowledge.publication_event
    drop constraint publication_event_document_status_check;

alter table knowledge.publication_event
    add constraint knowledge_publication_event_document_status
    check (document_status in ('PUBLISHED', 'REVOKED', 'DRAFT'));
