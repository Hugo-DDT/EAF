insert into evaluation.dataset_case(dataset_version, case_id, input_text, expected_risk)
select 'p4-v1', case_id, input_text, expected_risk
from evaluation.dataset_case
where dataset_version = 'p3-v1'
on conflict (dataset_version, case_id) do nothing;
-- P4 使用独立数据集版本；预期答案仍只存在 evaluation 域，不进入模型上下文或业务学习数据。
