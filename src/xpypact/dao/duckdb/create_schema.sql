CREATE TABLE IF NOT EXISTS rundata (
    material_id uinteger not null,
    case_id     uinteger not null,
    timestamp timestamp not null,
    run_name varchar not null,
    flux_name varchar NOT NULL,
    dose_rate_type varchar NOT NULL,
    dose_rate_distance real NOT NULL,
    primary key(material_id, case_id)
);

CREATE TABLE IF NOT EXISTS timestep(
    material_id uinteger not null,
    case_id     uinteger not null,
    time_step_number uinteger not null,
    elapsed_time float4 not null,
    irradiation_time float4 not null,
    cooling_time float4 not null,
    duration float4 not null,
    flux float4 not null,
    atoms float4 not null,
    activity float4 not null,
    alpha_activity float4 not null,
    beta_activity float4 not null,
    gamma_activity float4 not null,
    mass float4 not null,
    heat float4 not null,
    alpha_heat float4 null,
    beta_heat float4 not null,
    gamma_heat float4 not null,
    ingest1ion_dose float4 not null,
    inhalation_dose float4 not null,
    dose_rate float4 not null,
    primary key(material_id, case_id, time_step_number),
    foreign key(material_id, case_id) references rundata(material_id, case_id)
);


CREATE TABLE IF NOT EXISTS nuclide(
    element varchar(2) not null,
    mass_number usmallint not null check(0 < mass_number),
    state varchar(1) not null,
    zai integer not null check(10010 <= zai) unique,
    half_life float4 not null check(0 <= half_life),
    primary key(element, mass_number, state)
);

CREATE TABLE IF NOT EXISTS timestep_nuclide(
    material_id uinteger not null,
    case_id     uinteger not null,
    time_step_number uinteger not null,
    element varchar(2) not null,
    mass_number usmallint not null,

    state varchar(1) not null,
    atoms float4 not null,
    grams float4 not null,
    activity float4 not null,
    alpha_activity float4 not null,

    beta_activity float4 not null,
    gamma_activity float4 not null,
    heat float4 not null,
    alpha_heat float4 not null,
    beta_heat float4 not null,

    gamma_heat float4 not null,
    dose float4 not null,
    ingestion float4 not null,
    inhalation float4 not null,

    primary key(material_id, case_id, time_step_number, element, mass_number, state),
    foreign key(material_id, case_id, time_step_number) references timestep(material_id, case_id, time_step_number),
    foreign key(element, mass_number, state) references nuclide(element, mass_number, state)
);

CREATE TABLE IF NOT EXISTS timestep_gamma(
    material_id uinteger not null,
    case_id     uinteger not null,
    time_step_number uinteger not null,
    boundary real not null check(0 <= boundary),
    rate real not null,
    primary key(material_id, case_id, time_step_number, boundary),
    foreign key(material_id, case_id, time_step_number) references timestep(material_id, case_id, time_step_number),
);
