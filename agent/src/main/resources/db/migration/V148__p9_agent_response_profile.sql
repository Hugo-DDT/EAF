alter table agent.version
    add column response_profile varchar(40) not null default 'CUSTOMER_RISK_V1'
        check (response_profile in ('CUSTOMER_RISK_V1', 'CUSTOMER_FOLLOWUP_V1', 'KNOWLEDGE_QA_V1')),
    add column retrieval_mode varchar(20) not null default 'VECTOR'
        check (retrieval_mode in ('VECTOR', 'HYBRID')),
    add column evidence_policy varchar(40) not null default 'NONE'
        check (evidence_policy in ('NONE', 'PASSAGE_CHOICE_V1'));

-- 旧发布版本保持风险输出和向量检索默认值；新版本必须显式绑定场景配置。
