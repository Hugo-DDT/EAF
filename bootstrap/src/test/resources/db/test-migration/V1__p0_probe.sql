CREATE TABLE eaf_meta.p0_probe (
    id integer PRIMARY KEY,
    note text NOT NULL
);

INSERT INTO eaf_meta.p0_probe (id, note) VALUES (1, 'migration-ran');
-- 本文件负责 EAF 的 V1__p0_probe.sql 相关定义。
