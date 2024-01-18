-- sqlfluff:dialect:duckdb
-- sqlfluff:max_line_length:120


-- DuckDB doesn't recommend to use primary keys and indices
-- https://duckdb.org/docs/guides/performance/indexing
--
--Best Practices:
--
--Only use primary keys, foreign keys, or unique constraints,
--if these are necessary for enforcing constraints on your data.
--Do not define explicit indexes unless you have highly selective queries.
--If you define an ART index, do so after bulk loading the data to the table.
--
-- However, for testing and debugging the constraints  are useful.

create table if not exists rundata (
    material_id uinteger not null,
    case_id uinteger not null,
    timestamp timestamp not null,
    run_name varchar not null,
    flux_name varchar not null,
    dose_rate_type varchar not null,
    dose_rate_distance real not null
);

-- the "total_" prefix is removed from fields
-- to have all the field names consistent
create table if not exists timestep (
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
);


create table if not exists nuclide (
    zai uinteger not null check (10010 <= zai),
    element varchar(2) not null,
    mass_number usmallint not null check (0 < mass_number),
    state varchar(1) not null,
    half_life real not null check (0 <= half_life)
);

create table if not exists timestep_nuclide (
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
);

create table if not exists gbins (
    g utinyint,
    boundary real not null check (0.0 <= boundary)
);

create table if not exists timestep_gamma (
    material_id uinteger not null,
    case_id uinteger not null,
    time_step_number uinteger not null,
    g utinyint not null, -- only upper bin boundaries in this table
    rate real not null
);
