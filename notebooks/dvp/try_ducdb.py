#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic("config", "Completer.use_jedi = False")


# In[3]:


import xpypact as xp

# In[5]:


xp.__version__


# In[11]:


from pathlib import Path

# In[19]:


root_dir = Path("~", "dev", "xpypact").expanduser()


# In[20]:


json_path = root_dir / "tests/data/Ag-1.json"
assert json_path.exists()


# In[23]:


import xpypact.data_arrays as da

from xpypact.Inventory import Inventory, from_json

# In[24]:


inventory = from_json(json_path)


# In[25]:


ds = da.create_dataset(inventory)


# In[26]:


ds


# In[ ]:


# In[28]:


import duckdb as db

# In[46]:


db_path = root_dir / "wrk/try-duckdb.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)


# In[48]:


con = db.connect(str(db_path))


# In[153]:


# con.execute("drop table rundata")
# con.execute("drop table if exists timestep_gamma")
con.execute("drop table if exists timestep_nuclide")
# con.execute("drop table if exists timestep")
con.execute("drop table nuclide")


# In[154]:


def create_tables(con):
    sql = """

    CREATE TABLE IF NOT EXISTS rundata (
        timestamp varchar NOT NULL,
        run_name  varchar NOT NULL,
        flux_name varchar NOT NULL,
        dose_rate_type varchar NOT NULL,
        dose_rate_distance real NOT NULL
    );

    CREATE TABLE IF NOT EXISTS timestep(
        id integer PRIMARY KEY,
        elapsed_time real not null,
        irradiation_time real not null,
        cooling_time real not null,
        duration real not null,
        flux real not null,
        total_atoms real not null,
        total_activity real not null,
        total_alpha_activity real not null,
        total_beta_activity real not null,
        total_gamma_activity real not null,
        total_mass real not null,
        total_heat real not null,
        total_alpha_heat real not null,
        total_beta_heat real not null,
        total_gamma_heat real not null,
        total_ingest1ion_dose real not null,
        total_inhalation_dose real not null,
        total_dose_rate real not null
    );

    CREATE TABLE IF NOT EXISTS nuclide(
        element varchar(2) not null,
        mass_number integer not null check(0 < mass_number),     -- A
        state varchar(1) not null,
        zai integer not null check(10010 <= zai) unique,
        half_life real not null check(0 <= half_life),
        primary key(element, mass_number, state)
    );

    CREATE TABLE IF NOT EXISTS timestep_nuclide(
        timestep_id integer not null,
        element varchar(2) not null,
        mass_number integer not null,
        state varchar(1) not null,
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
        primary key(timestep_id, element, mass_number, state),
        foreign key(timestep_id) references timestep(id),
        foreign key(element, mass_number, state) references nuclide(element, mass_number, state)
    );

    CREATE TABLE IF NOT EXISTS timestep_gamma(
        timestep_id integer not null,
        boundary real not null check(0 <= boundary),
        intensity real not null,
        primary key(timestep_id, boundary),
        foreign key(timestep_id) references timestep(id),
    );
"""
    con.execute(sql)


# In[155]:


create_tables(con)


# In[85]:


# con.execute("delete from rundata")


# In[81]:


def save_rundata(con, ds):
    sql = """
        INSERT INTO rundata values(?, ?, ?, ?, ?)
    """
    con.execute(
        sql,
        (
            ds.coords["timestamp"].item(),
            ds.attrs["run_name"],
            ds.attrs["flux_name"],
            ds.attrs["dose_rate_type"],
            ds.attrs["dose_rate_distance"],
        ),
    )
    con.commit()


# In[83]:


save_rundata(con, ds)


# In[84]:


con.execute("select * from rundata").df()


# In[114]:


def save_timesteps(con, ds):
    timesteps_df = (
        ds[
            [
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
        .to_pandas()
        .reset_index()
    )
    sql = "insert into timestep select * from timesteps_df"
    con.execute(sql)
    con.commit()


# In[115]:


save_timesteps(con, ds)


# In[116]:


con.execute("select * from timestep").df()


# In[131]:


ds[
    ["element", "mass_number", "state", "zai", "nuclide_half_life"]
].to_pandas().reset_index(drop=True)


# In[156]:


def save_nuclides(con, ds):
    nuclides_df = (
        ds[["element", "mass_number", "state", "zai", "nuclide_half_life"]]
        .to_pandas()
        .reset_index(drop=True)
    )
    sql = "insert into nuclide select * from nuclides_df"
    con.execute(sql)
    con.commit()


# In[157]:


save_nuclides(con, ds)


# In[160]:


con.execute("select * from nuclide").df().head()


# In[166]:


ds


# In[203]:


ds[
    [
        """Time_step_number.""",
        """Element.""",
        """Mass_number.""",
        """State.""",
        """Nuclide_atoms.""",
        """Nuclide_grams.""",
        """Nuclide_activity.""",
        """Nuclide_alpha_activity.""",
        """Nuclide_beta_activity.""",
        """Nuclide_gamma_activity.""",
        """Nuclide_heat.""",
        """Nuclide_alpha_heat.""",
        """Nuclide_beta_heat.""",
        """Nuclide_gamma_heat.""",
        """Nuclide_dose.""",
        """Nuclide_ingestion.""",
        """Nuclide_inhalation.""",
    ]
].to_array().drop_indexes(["time_step_number", "nuclide"])
# .reset_coords(names=["time_step_number", "nuclide"])
# .stack(dimensions={"tsn": ["time_step_number", "element", "mass_number", "state"]})


# In[186]:


def save_timestep_nucludes(con, ds):
    timesteps_nuclides_df = (
        ds[
            [
                "time_step_number",
                "element",
                "mass_number",
                "state",
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
        ]
        .to_dataframe()
        .reset_index(drop=True)
    )
    sql = "insert into timestep_nuclide select * from timesteps_nuclides_df"
    con.execute(sql)
    con.commit()


# In[187]:


save_timestep_nucludes(con, ds)


# In[41]:


con.close()


# In[ ]:
