-- sqlfluff:dialect:duckdb

create or replace table rundata (
    material_id uinteger not null,
    case_id uinteger not null,
    timestamp timestamp not null,
    run_name varchar not null,
    flux_name varchar not null,
    dose_rate_type varchar not null,
    dose_rate_distance real not null,
    primary key (material_id, case_id)
);

create or replace table timestep (
    material_id uinteger not null,
    case_id uinteger not null,
    time_step_number uinteger not null,
    elapsed_time real not null,
    irradiation_time real not null,
    cooling_time real not null,
    duration real not null,
    flux real not null,
    total_atoms real not null,
    total_activity real not null,
    alpha_activity real not null,
    beta_activity real not null,
    gamma_activity real not null,
    total_mass real not null,
    total_heat real not null,
    alpha_heat real null,
    beta_heat real not null,
    gamma_heat real not null,
    ingest1ion real not null,
    inhalation real not null,
    dose real not null,
    primary key (material_id, case_id, time_step_number),
    foreign key (material_id, case_id) references rundata (material_id, case_id)
);


create or replace table nuclide (
    element varchar(2) not null,
    mass_number usmallint not null check (0 < mass_number),
    state varchar(1) not null,
    zai uinteger not null check (10010 <= zai) unique,
    half_life real not null check (0 <= half_life),
    primary key (element, mass_number, state)
);

create or replace table timestep_nuclide (
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
    inhalation real not null,

    primary key (material_id, case_id, time_step_number, zai),
    foreign key (material_id, case_id, time_step_number) references timestep (material_id, case_id, time_step_number),
    foreign key (zai) references nuclide (zai)
);

create or replace table gbins (
    g utinyint primary key,
    boundary real not null check (0.0 <= boundary) unique
);

create or replace table timestep_gamma (
    material_id uinteger not null,
    case_id uinteger not null,
    time_step_number uinteger not null,
    g utinyint not null, -- only upper bin boundaries in this table
    rate real not null,
    primary key (material_id, case_id, time_step_number, g),
    foreign key (material_id, case_id, time_step_number) references timestep (material_id, case_id, time_step_number),
    foreign key (g) references gbins (g)
);
