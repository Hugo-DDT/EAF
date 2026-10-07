-- 有界投递保存安全错误类型，超过五次退避后保留可检查的失败记录。
alter table knowledge.outbox add column last_error varchar(120);
