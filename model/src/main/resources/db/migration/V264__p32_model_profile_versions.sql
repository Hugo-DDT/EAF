insert into model.profile(id, provider, model_name, status, input_output_budget)
values ('22000000-0000-4000-8000-000000000002', 'server-configured', 'p15-bounded-output', 'ACTIVE', 8000)
on conflict (id) do nothing;

create table model.profile_version (
    profile_id uuid not null references model.profile(id),
    version varchar(40) not null,
    configuration_hash char(64) not null,
    snapshot jsonb not null,
    status varchar(20) not null check (status in ('ACTIVE', 'DISABLED')),
    created_at timestamptz not null default now(),
    primary key (profile_id, version)
);
