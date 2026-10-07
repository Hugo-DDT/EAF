-- 重放同一 Learning 来源时核对完整请求摘要，避免同一修订键被改载荷复用。
alter table memory.release add column request_hash char(64);
