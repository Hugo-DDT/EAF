-- P7 Evaluation 分开汇总价格估算与账单实付，避免把未知费用显示成账单金额。
alter table evaluation.candidate_eval_run add column actual_cost numeric(18, 8);
alter table evaluation.candidate_eval_run add column actual_cost_currency varchar(3);
alter table evaluation.candidate_eval_run add column billing_status varchar(30) not null default 'UNBILLED';
alter table evaluation.candidate_eval_run add constraint candidate_eval_actual_cost_check
    check (actual_cost is null or actual_cost >= 0);
alter table evaluation.candidate_eval_run add constraint candidate_eval_billing_status_check
    check (billing_status in ('BILLED', 'PARTIAL', 'UNBILLED', 'UNKNOWN', 'MIXED_CURRENCY'));
alter table evaluation.candidate_eval_run drop constraint candidate_eval_run_cost_status_check;
alter table evaluation.candidate_eval_run add constraint candidate_eval_run_cost_status_check
    check (cost_status in ('KNOWN', 'ESTIMATED', 'BILLED', 'UNKNOWN_USAGE', 'UNKNOWN_PRICE', 'MIXED_CURRENCY'));
