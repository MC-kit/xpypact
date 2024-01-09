-- sqlfluff:dialect:duckdb
-- sqlfluff:max_line_length:120


-- We don't create indices and primary keys here intentionally
-- The data will be transformed to parquet for bulk ingestion in R2S application
-- and indices are just waste of time and low performance for that operation
-- For other applications user can add indices later

create table rundata (      -- noqa: PRS
    material_id uinteger not null,
    case_id uinteger not null,
    timestamp timestamp not null,
    run_name varchar not null,
    flux_name varchar not null,
    dose_rate_type varchar not null,
    dose_rate_distance real not null,
    -- primary key (material_id, case_id)
);

-- the "total_" prefix is removed from fields
-- to have all the field names consistent
create table timestep (
    material_id uinteger not null,
    case_id uinteger not null,
    time_step_number uinteger not null,
    elapsed_time real not null,
    irradiation_time real not null,
    cooling_time real not null,
    duration real not null,
    flux real not null,
    atoms real not null,
    activity real not null,
    alpha_activity real not null,
    beta_activity real not null,
    gamma_activity real not null,
    mass real not null,
    heat real not null,
    alpha_heat real null,
    beta_heat real not null,
    gamma_heat real not null,
    ingestion real not null,
    inhalation real not null,
    dose real not null
--    primary key (material_id, case_id, time_step_number),
--    foreign key (material_id, case_id) references rundata (material_id, case_id)
);


create table nuclide (
    zai uinteger not null check (10010 <= zai) primary key,
    element varchar(2) not null,
    mass_number usmallint not null check (0 < mass_number),
    state varchar(1) not null,
    half_life real not null check (0 <= half_life)
);

create unique index nuclide_ems on nuclide (element, mass_number, state);

create table timestep_nuclide (
    material_id uinteger not null,
    case_id uinteger not null,
    time_step_number uinteger not null,
    zai uinteger not null,
    atoms real not null,
    grams real not null,

    activity real not null,
    alpha_activity real not null,
    beta_activity real not null,
    gamma_activity real not null,

    heat real not null,
    alpha_heat real not null,
    beta_heat real not null,
    gamma_heat real not null,

    dose real not null,
    ingestion real not null,
    inhalation real not null

--    primary key (material_id, case_id, time_step_number, zai),
--    foreign key (material_id, case_id, time_step_number) references timestep (material_id, case_id, time_step_number)
);

create table gbins (
    g utinyint primary key,
    boundary real not null check (0.0 <= boundary) unique
);

create table timestep_gamma (
    material_id uinteger not null,
    case_id uinteger not null,
    time_step_number uinteger not null,
    g utinyint not null, -- only upper bin boundaries in this table
    rate real not null

--    primary key (material_id, case_id, time_step_number, g),
--    foreign key (material_id, case_id, time_step_number) references timestep (material_id, case_id, time_step_number)
);
