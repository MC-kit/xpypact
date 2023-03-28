#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic("config", "Completer.use_jedi = False")


# In[2]:


import xpypact as xp

# In[3]:


xp.__version__


# In[4]:


from pathlib import Path

# In[5]:


root_dir = Path("~", "dev", "xpypact").expanduser()


# In[6]:


json_path = root_dir / "wrk/Alloy718-Co04-104_2_1_1.json"
assert json_path.exists()


# In[7]:


import xpypact.data_arrays as da

from xpypact.Inventory import from_json

# In[8]:


inventory = from_json(json_path)


# In[9]:


ds = da.create_dataset(inventory)


# In[10]:


ds


# In[12]:


import duckdb as db

# In[13]:


db_path = root_dir / "wrk/try-duckdb.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)
if db_path.exists():
    db_path.unlink()


# In[14]:


con = db.connect(str(db_path))


# In[15]:


material_id = 100
case_id = 3


# In[16]:


ds = ds.expand_dims(dim={"material_id": [material_id], "case_id": [case_id]})
ds


# How to transform xarray to pandas saving indexes as columns

# In[20]:


ds.irradiation_time.stack(
    {"idx": ["material_id", "case_id", "time_step_number"]}
).to_pandas().reset_index()


# In[21]:


def drop_tables(con):
    con.execute("drop table if exists timestep_gamma")
    con.execute("drop table if exists timestep_nuclide")
    con.execute("drop table if exists timestep")
    con.execute("drop table if exists nuclide")
    con.execute("drop table if exists rundata")


# In[22]:


def create_tables(con):
    sql = """
    CREATE TABLE IF NOT EXISTS rundata (
        material_id uinteger not null,
        case_id uinteger not null,
        timestamp timestamp not null,
        run_name varchar not null,
        flux_name varchar NOT NULL,
        dose_rate_type varchar NOT NULL,
        dose_rate_distance real NOT NULL,
        primary key(material_id, case_id)
    );

    CREATE TABLE IF NOT EXISTS timestep(
        material_id  uinteger not null,
        case_id      uinteger not null,
        time_step_number uinteger not null,
        elapsed_time float4 not null,
        irradiation_time float4 not null,
        cooling_time float4 not null,
        duration float4 not null,
        flux float4 not null,
        total_atoms float4 not null,
        total_activity float4 not null,
        total_alpha_activity float4 not null,
        total_beta_activity float4 not null,
        total_gamma_activity float4 not null,
        total_mass float4 not null,
        total_heat float4 not null,
        total_alpha_heat float4 null,
        total_beta_heat float4 not null,
        total_gamma_heat float4 not null,
        total_ingest1ion_dose float4 not null,
        total_inhalation_dose float4 not null,
        total_dose_rate float4 not null,
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
        material_id  uinteger not null,
        case_id      uinteger not null,
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
        material_id  uinteger not null,
        case_id      uinteger not null,
        time_step_number uinteger not null,
        boundary real not null check(0 <= boundary),
        intensity real not null,
        primary key(material_id, case_id, time_step_number, boundary),
        foreign key(material_id, case_id, time_step_number) references timestep(material_id, case_id, time_step_number),
    );
    """
    con.execute(sql)


# In[23]:


drop_tables(con)
create_tables(con)


# In[24]:


def save_rundata(con, ds):
    sql = """
        INSERT INTO rundata values(?, ?, ?, ?, ?, ?, ?)
    """
    con.execute(
        sql,
        (
            ds.material_id.item(),
            ds.case_id.item(),
            ds.timestamp.dt.strftime("%Y-%m-%d %H:%M:%S").item(),
            ds.attrs["run_name"],
            ds.attrs["flux_name"],
            ds.attrs["dose_rate_type"],
            ds.attrs["dose_rate_distance"],
        ),
    )
    con.commit()


# In[25]:


save_rundata(con, ds)


# In[26]:


con.execute("select * from rundata").df()


# In[27]:


def save_timesteps(con, ds):
    (
        ds[
            [
                "material_id",
                "case_id",
                "time_step_number",
                "elapsed_time",
                "irradiation_time",
                "cooling_time",
                "duration",
                "flux",
                "total_atoms",
                "total_activity",
                "total_alpha_activity",
                "total_beta_activity",
                "total_gamma_activity",
                "total_mass",
                "total_heat",
                "total_alpha_heat",
                "total_beta_heat",
                "total_gamma_heat",
                "total_ingest1ion_dose",
                "total_inhalation_dose",
                "total_dose_rate",
            ]
        ]
        .to_dataframe()
        .reset_index()
    )
    sql = "insert into timestep select * from timesteps_df"
    con.execute(sql)
    con.commit()


# In[28]:


save_timesteps(con, ds)


# In[29]:


con.execute("select * from timestep").df()


# In[55]:


columns_all = ["element", "mass_number", "state", "zai", "nuclide_half_life"]
columns = ["zai", "nuclide_half_life"]
ds[columns].to_dataframe()[columns].reset_index()[columns_all]
# .unstack("nuclide")
# .stack({"idx": ["material_id", "case_id", "element", "mass_number", "state"]}) \
# .to_dataframe()

# \
#     .reset_index(["material_id", "case_id"]) \
#     .to_dataframe() \
#     .reset_index(
#         [
#             "material_id", "case_id"
#         ],
#         drop=True
#     ) \
#     .reset_index(drop=True)


# In[56]:


def save_nuclides(con, ds):
    columns_all = ["element", "mass_number", "state", "zai", "nuclide_half_life"]
    columns = ["zai", "nuclide_half_life"]
    ds[columns].to_dataframe()[columns].reset_index()[columns_all]
    sql = "insert or ignore into nuclide select * from nuclides_df"
    con.execute(sql)
    con.commit()


# In[57]:


save_nuclides(con, ds)


# In[58]:


con.execute("select * from nuclide").df()


# In[91]:


columns = [
    # "material_id",
    # "case_id",
    # "time_step_number",
    # "element",
    # "mass_number",
    # "state",
    "nuclide_atoms",
    "nuclide_grams",
    "nuclide_activity",
    "nuclide_alpha_activity",
    "nuclide_beta_activity",
    "nuclide_gamma_activity",
    "nuclide_heat",
    "nuclide_alpha_heat",
    "nuclide_beta_heat",
    "nuclide_gamma_heat",
    "nuclide_dose",
    "nuclide_ingestion",
    "nuclide_inhalation",
]
columns_all = [
    "material_id",
    "case_id",
    "time_step_number",
    "element",
    "mass_number",
    "state",
] + columns

ds[columns].stack(idx=("material_id", "case_id", "time_step_number")).to_dataframe()[
    columns
].reset_index()[columns_all].fillna(0.0)
# ds[columns].reset_index(["material_id", "case_id"])

# ds[columns].coords.to_dataset()
# .reset_index(["elapsed_time", "zai"])
# .reset_coords()
# .drop_indexes(["material_id", "case_id", "time_step_number", "nuclide"])
# .reset_index(["material_id", "case_id", "time_step_number", "nuclide"])
# .to_dataframe().reset_index(
#     [
#         "material_id", "case_id", "time_step_number",
#     ]
# ).reset_index(drop=True)[columns].fillna(0.0)


# In[ ]:


# In[92]:


def save_timestep_nucludes(con, ds):
    columns = [
        "nuclide_atoms",
        "nuclide_grams",
        "nuclide_activity",
        "nuclide_alpha_activity",
        "nuclide_beta_activity",
        "nuclide_gamma_activity",
        "nuclide_heat",
        "nuclide_alpha_heat",
        "nuclide_beta_heat",
        "nuclide_gamma_heat",
        "nuclide_dose",
        "nuclide_ingestion",
        "nuclide_inhalation",
    ]
    columns_all = [
        "material_id",
        "case_id",
        "time_step_number",
        "element",
        "mass_number",
        "state",
    ] + columns

    (
        ds[columns]
        .stack(idx=("material_id", "case_id", "time_step_number"))
        .to_dataframe()[columns]
        .reset_index()[columns_all]
        .fillna(0.0)
    )
    sql = "insert into timestep_nuclide select * from tn"
    con.execute(sql)
    con.commit()


# In[93]:


save_timestep_nucludes(con, ds)


# In[94]:


con.execute("select * from timestep_nuclide").df()


# In[98]:


ds.gamma.to_dataframe().reset_index()[
    ["material_id", "case_id", "time_step_number", "gamma_boundaries", "gamma"]
]


# In[99]:


def save_gamma_spectra(con, ds):
    columns = [
        "material_id",
        "case_id",
        "time_step_number",
        "gamma_boundaries",
        "gamma",
    ]
    ds.gamma.to_dataframe().reset_index()[columns]
    sql = "insert into timestep_gamma select * from tg"
    con.execute(sql)
    con.commit()


# In[100]:


save_gamma_spectra(con, ds)


# In[101]:


con.execute("select * from timestep_gamma").df()


# In[103]:


con.execute("select boundary, intensity from timestep_gamma where time_step_number = 42").df()


# In[104]:


con.close()


# In[ ]:
