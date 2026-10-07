alter table execution.remote_a2a_operation drop constraint if exists remote_a2a_operation_state_check;
alter table execution.remote_a2a_operation add constraint remote_a2a_operation_state_check
    check (state in ('SEND_PENDING','SEND_UNKNOWN','ACCEPTED','WORKING','CANCEL_PENDING','CANCEL_UNKNOWN',
                     'COMPLETED','FAILED','CANCELLED','UNKNOWN'));
