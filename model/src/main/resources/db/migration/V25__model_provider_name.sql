update model.profile
set provider = 'deterministic-or-openai-compatible'
where id = '22000000-0000-4000-8000-000000000001'
  and provider = 'deterministic-or-dashscope';
-- 本文件负责 EAF 的 V25__model_provider_name.sql 相关定义。
