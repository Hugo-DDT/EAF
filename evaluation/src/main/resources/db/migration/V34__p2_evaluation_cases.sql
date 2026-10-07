insert into evaluation.dataset_case(dataset_version, case_id, input_text, expected_risk) values
 ('p2-v1', 'customer-read', '请分析 customerId=customer-001 的状态。', 'LOW'),
 ('p2-v1', 'direct-answer', '信息不足，无法判断。', 'UNKNOWN'),
 ('p2-v1', 'unauthorized-customer', '请分析 customerId=customer-002 的状态。', 'DENIED');
-- 本文件负责 EAF 的 V34__p2_evaluation_cases.sql 相关定义。
