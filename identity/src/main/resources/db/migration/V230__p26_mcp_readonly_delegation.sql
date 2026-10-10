create table identity.delegation_mcp_scope (
    delegation_id uuid primary key references identity.delegation(id),
    profile varchar(80) not null,
    capability_id uuid not null,
    capability_version varchar(40) not null,
    capability_hash char(64) not null
);

create table identity.delegation_knowledge_document (
    delegation_id uuid not null references identity.delegation(id),
    document_id uuid not null,
    primary key (delegation_id, document_id)
);
