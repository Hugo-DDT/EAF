-- 跨 JVM Provider HTTP 槽；过期 HELD 由应用隔离为 QUARANTINED，不因 TTL 自动释放。
create table model.outbound_slot (
    slot_no integer primary key check (slot_no > 0),
    state varchar(16) not null check (state in ('FREE', 'HELD', 'QUARANTINED')),
    lease_id uuid unique,
    owner_incarnation uuid,
    lease_until timestamptz,
    updated_at timestamptz not null default now(),
    cleared_by varchar(120),
    cleared_reason varchar(500),
    cleared_at timestamptz,
    check (
        (state = 'FREE' and lease_id is null and owner_incarnation is null and lease_until is null)
        or (state <> 'FREE' and lease_id is not null and owner_incarnation is not null and lease_until is not null)
    )
);
