# Databricks notebook source
# %md # CCU052_02-D01-parameters

# **Description** This notebook defines a set of parameters for use across the data curation pipeline for the project. This notebook is called at the start of each subsequent notebook in the pipleine for consistency. The parameters include variables for the project, databases, study dates, and composite events; and paths to archive, reference, temporary, input, curated, and output tables. 

# **Authors** Thomas Bolton, Hannah Whittaker (based on work from CCU002 and the Health Data Science Team, BHF Data Science Centre)

# **Data input** -

# **Data output** parameters_df_datasets

# **Notes** Update `pipeline_production_date` and set `run_all_toggle` to True to rerun in full. After which set `run_all_toggle` to False to prevent subsequent notebooks that call this notebook from running the `archived_on` section. This markdown cell has been intentionally commented out to avoid this cell being displayed in the other notebooks running this notebook.

# COMMAND ----------

run_all_toggle = False

# COMMAND ----------

# %md # 0. Libraries

# COMMAND ----------

import pyspark.sql.functions as f
import pandas as pd
import re
import datetime

# COMMAND ----------

# %md # 1. Functions

# COMMAND ----------

###########################################################################
# %md ## 1.1. Common
###########################################################################

# COMMAND ----------

# MAGIC %run "../../../../shds/common/functions"

# COMMAND ----------

###########################################################################
# %md ## 1.2. Custom
###########################################################################

# COMMAND ----------

# DBTITLE 1,save_table
def save_table(df, out_name:str, save_previous=True, dbc:str=f''):
  
  # assert that df is a dataframe
  assert isinstance(df, f.DataFrame), 'df must be of type dataframe' #isinstance(df, pd.DataFrame) | 
  # if a pandas df then convert to spark
  #if(isinstance(df, pd.DataFrame)):
    #df = (spark.createDataFrame(df))
  
  # save name check
  if(any(char.isupper() for char in out_name)): 
    print(f'Warning: {out_name} converted to lowercase for saving')
    out_name = out_name.lower()
    print('out_name: ' + out_name)
    print('')
  
  # df name
  df_name = [x for x in globals() if globals()[x] is df][0]
  
  # ------------------------------------------------------------------------------------------------
  # save previous version for comparison purposes
  if(save_previous):
    tmpt = (
      spark.sql(f"""SHOW TABLES FROM {dbc}""")
      .select('tableName')
      .where(f.col('tableName') == out_name)
      .collect()
    )
    if(len(tmpt)>0):
      # save with production date appended
      _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
      out_name_pre = f'{out_name}_pre{_datetimenow}'.lower()
      print(f'saving (previous version):')
      print(f'  {dbc}.{out_name}')
      print(f'  as')
      print(f'  {dbc}.{out_name_pre}')
      spark.table(f'{dbc}.{out_name}').write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dbc}.{out_name_pre}')
      # spark.sql(f'ALTER TABLE {dbc}.{out_name_pre} OWNER TO {dbc}')
      print('saved')
      print('') 
    else:
      print(f'Warning: no previous version of {out_name} found')
      print('')
  # ------------------------------------------------------------------------------------------------  
  
  # save new version
  print(f'saving:')
  print(f'  {df_name}')
  print(f'  as')
  print(f'  {dbc}.{out_name}')
  df.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{dbc}.{out_name}')
  # spark.sql(f'ALTER TABLE {dbc}.{out_name} OWNER TO {dbc}')
  print('saved')

# COMMAND ----------

# DBTITLE 1,extract_batch_from_archive
# function to extract the batch corresponding to the pre-defined archived_on date - will be used in subsequent notebooks

from pyspark.sql import DataFrame
def extract_batch_from_archive(_df_datasets: DataFrame, _dataset: str):
  
  # get row from df_archive_tables corresponding to the specified dataset
  _row = _df_datasets[_df_datasets['dataset'] == _dataset]
  
  # check one row only
  assert _row.shape[0] != 0, f"dataset = {_dataset} not found in _df_datasets (datasets = {_df_datasets['dataset'].tolist()})"
  assert _row.shape[0] == 1, f"dataset = {_dataset} has >1 row in _df_datasets"
  
  # create path and extract archived on
  _row = _row.iloc[0]
  _path = _row['database'] + '.' + _row['table']  
  _archived_on = _row['archived_on']
  _n_rows_expected = _row['n']  
  print(_path + ' (archived_on = ' + _archived_on + ', n_rows_expected = ' + _n_rows_expected + ')')
  
  # check path exists # commented out for runtime
#   _tmp_exists = spark.sql(f"SHOW TABLES FROM {_row['database']}")\
#     .where(f.col('tableName') == _row['table'])\
#     .count()
#   assert _tmp_exists == 1, f"path = {_path} not found"

  # extract batch
  _tmp = spark.table(_path)\
    .where(f.col('archived_on') == _archived_on)  
  
  # check number of records returned
  _n_rows_observed = _tmp.count()
  print(f'  n_rows_observed = {_n_rows_observed:,}')
  assert _n_rows_observed > 0, f"_n_rows_observed == 0"
  assert f'{_n_rows_observed:,}' == _n_rows_expected, f"_n_rows_observed != _n_rows_expected ({_n_rows_observed:,} != {_n_rows_expected})"

  # return dataframe
  return _tmp

# COMMAND ----------

# %md # 2. Variables

# COMMAND ----------

# set study-specific variables

# -----------------------------------------------------------------------------
# study prefix
# -----------------------------------------------------------------------------
# project, subproject, cohort, version
project = 'ccu052'
subproject = '02'
cohort = 'c01'
version = 'v01'

proj = f'{project}_{subproject}_{cohort}_{version}'
# TODO: rename proj to study_prefix if this convention is accepted and change all occurences of proj in this and subsequent notebooks


# -----------------------------------------------------------------------------
# study dates
# -----------------------------------------------------------------------------
study_start_date = '2019-11-01' 
study_end_date   = '2023-06-30' # updated for pipeline rerun (2023-09-12) [checked fu], previsouly '2025-03-31'


# -----------------------------------------------------------------------------
# databases
# -----------------------------------------------------------------------------
db = ''
dbc = f''
dbc_old = f'{db}_collab'


# -----------------------------------------------------------------------------
# pipeline production date
# -----------------------------------------------------------------------------
# date at which pipeline was created and archived_on dates for datasets have been selected based on
pipeline_production_date = '2025-03-31'


# -----------------------------------------------------------------------------
# datasets
# -----------------------------------------------------------------------------
# data frame of datasets
datasets = [
  # -----------------------------------------------------------------------------
  # Datasets requested by the project
  # -----------------------------------------------------------------------------  
    ['gdppr',     dbc_old, f'gdppr_{db}_archive',             'NHS_NUMBER_DEID',                'DATE']  
  , ['hes_apc',   dbc_old, f'hes_apc_all_years_archive',      'PERSON_ID_DEID',                 'EPISTART']
  , ['deaths',    dbc_old, f'deaths_{db}_archive',            'DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH']
  , ['pmeds',     dbc_old, f'primary_care_meds_{db}_archive', 'Person_ID_DEID',                 'ProcessingPeriodDate']           
  , ['sgss',      dbc_old, f'sgss_{db}_archive',              'PERSON_ID_DEID',                 'Specimen_Date']    
  
  # -----------------------------------------------------------------------------
  # Additonal datasets needed for the data curation pipeline for this project
  # -----------------------------------------------------------------------------
  , ['hes_ae',    dbc_old, f'hes_ae_all_years_archive',       'PERSON_ID_DEID',                 'ARRIVALDATE']
  , ['hes_op',    dbc_old, f'hes_op_all_years_archive',       'PERSON_ID_DEID',                 'APPTDATE']
  , ['hes_cc',    dbc_old, f'hes_cc_all_years_archive',       'PERSON_ID_DEID',                 'CCSTARTDATE'] 
  , ['chess',     dbc_old, f'chess_{db}_archive',             'PERSON_ID_DEID',                 'InfectionSwabDate'] 
  , ['sus',       dbc_old, f'sus_{db}_archive',               'NHS_NUMBER_DEID',                'EPISODE_START_DATE']    
  
  # -----------------------------------------------------------------------------
  # Datasets not required for this project
  # -----------------------------------------------------------------------------  
#   , ['icnarc',    dbc_old, f'icnarc_{db}_archive',            'NHS_NUMBER_DEID',               'Date_of_admission_to_your_unit']  
#   , ['ssnap',     dbc_old, f'ssnap_{db}_archive',             'PERSON_ID_DEID',                'S1ONSETDATETIME'] 
#   , ['minap',     dbc_old, f'minap_{db}_archive',             'NHS_NUMBER_DEID',               'ARRIVAL_AT_HOSPITAL'] 
#   , ['nhfa',      dbc_old, f'nhfa_{db}_archive',              '1_03_NHS_NUMBER_DEID',          '2_00_DATE_OF_VISIT'] 
#   , ['nvra',      dbc_old, f'nvra_{db}_archive',              'NHS_NUMBER_DEID',               'DATE'] 
#   , ['vacc',      dbc_old, f'vaccine_status_{db}_archive',    'PERSON_ID_DEID',                'DATE_AND_TIME']  
]

tmp_df_datasets = pd.DataFrame(datasets, columns=['dataset', 'database', 'table', 'id', 'date']).reset_index()

if(run_all_toggle):
  print('tmp_df_datasets:\n', tmp_df_datasets.to_string())

# note: gdppr_1st has been removed - since we will move away from using this and use the gdppr_archive in full (filtered to archive_on on or before the selected archived_on date for gdppr)

# COMMAND ----------

# %md # 3. archived_on

# COMMAND ----------

###########################################################################
# %md ## 3.1. Create
###########################################################################
# for each dataset in tmp_df_datasets, 
#   tabulate all archived_on dates (for information)
#   find the latest (most recent) archived_on date before the pipeline_production_date
#   create a table containing a row with the latest archived_on date and count of the number of records for each dataset
  
# this will not run each time the Parameters notebook is run in annother notebook - will only run if the toggle is switched to True
if(run_all_toggle):

  latest_archived_on = []
  for index, row in tmp_df_datasets.iterrows():
    # initial  
    dataset = row['dataset']
    path = row['database'] + '.' + row['table']
    print(index, dataset, path); print()

    # point to table
    tmpd = spark.table(path)

    # tabulate all archived_on dates
    tmpt = tab(tmpd, 'archived_on')
    
    # extract latest (most recent) archived_on date before the pipeline_production_date
    tmpa = (
      tmpd
      .groupBy('archived_on')
      .agg(f.count(f.lit(1)).alias('n'))
      .withColumn('n', f.format_number('n', 0))
      .where(f.col('archived_on') <= pipeline_production_date)
      .orderBy(f.desc('archived_on'))
      .limit(1)
      .withColumn('dataset', f.lit(dataset))
      .select('dataset', 'archived_on', 'n')
    )
    
    # append results
    if(index == 0): latest_archived_on = tmpa
    else: latest_archived_on = latest_archived_on.unionByName(tmpa)
    print()

  # check  
  print(latest_archived_on.toPandas().to_string())

# COMMAND ----------

###########################################################################
# %md ## 3.2. Check
###########################################################################
# separate cell to check result
# this will not run each time the Parameters notebook is run in annother notebook - will only run if the toggle is switched to True
if(run_all_toggle):
  # check
  print(latest_archived_on.toPandas().to_string())

# COMMAND ----------

###########################################################################
# %md ## 3.3. Prepare
###########################################################################
# prepare the table to be saved
# merge the datasets dataframe with the latest_archived_on
# this will not run each time the Parameters notebook is run in annother notebook - will only run if the toggle is switched to True
if(run_all_toggle):
  tmp_df_datasets_sp = spark.createDataFrame(tmp_df_datasets) 
  parameters_df_datasets = (
    merge(tmp_df_datasets_sp, latest_archived_on, ['dataset'], validate='1:1', assert_results=['both'], indicator=0).orderBy('index')
    .withColumn('pipeline_production_date', f.lit(pipeline_production_date))
  ); print()  
  # check  
  print(parameters_df_datasets.toPandas().to_string())

# COMMAND ----------

###########################################################################
# %md ## 3.4. Save
###########################################################################
# save the parameters_df_datasets table
# which we can simply import below when not running all and calling this notebook in subsequent notebooks
# this will not run each time the Parameters notebook is run in annother notebook - will only run if the toggle is switched to True
if(run_all_toggle):
  
  # save name
  save_table(parameters_df_datasets, out_name=f'{proj}_parameters_df_datasets', save_previous=True, dbc=f'{dbc}')
 

# COMMAND ----------

###########################################################################
# %md ## 3.5. Import
###########################################################################
# import the parameters_df_datasets table 
# convert to a Pandas dataframe and transform archived_on to a string (to conform to the input that the extract_batch_from_archive function is expecting)

spark.sql(f'REFRESH TABLE {dbc}.{proj}_parameters_df_datasets')
parameters_df_datasets = (
  spark.table(f'{dbc}.{proj}_parameters_df_datasets')
  .orderBy('index')
  .toPandas()
)
parameters_df_datasets['archived_on'] = parameters_df_datasets['archived_on'].astype(str)

# check/print in Summary section below

# COMMAND ----------

# %md # 4. Paths

# COMMAND ----------

# -----------------------------------------------------------------------------
# These are paths to data tables curated in subsequent notebooks that may be
# needed in subsequent notebooks from which they were curated
# -----------------------------------------------------------------------------

# =============================================================================
# reference tables
# =============================================================================
param_path_ref_bhf_phenotypes  = 'bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127'
param_path_ref_geog            = 'dss_corporate.ons_chd_geo_listings'
param_path_ref_imd             = 'dss_corporate.english_indices_of_dep_v02'
param_path_ref_gp_refset       = 'dss_corporate.gpdata_snomed_refset_full'
param_path_ref_gdppr_refset    = 'dss_corporate.gdppr_cluster_refset'
param_path_ref_icd10           = 'dss_corporate.icd10_group_chapter_v01'
param_path_ref_opcs4           = 'dss_corporate.opcs_codes_v02'
param_path_ref_bnf             = 'dss_corporate.bnf_code_information'
# param_path_ref_map_ctv3_snomed = 'dss_corporate.read_codes_map_ctv3_to_snomed'
# param_path_ref_ethnic_hes      = 'dss_corporate.hesf_ethnicity'
# param_path_ref_ethnic_gdppr    = 'dss_corporate.gdppr_ethnicity'


# =============================================================================
# created tables
# =============================================================================
# -----------------------------------------------------------------------------
# codelist tables
# -----------------------------------------------------------------------------
# param_table_codelist_covid    = f'{proj}_codelist_covid'
param_table_codelist_qa       = f'{proj}_codelist_qa'
param_table_codelist_outcomes = f'{proj}_codelist_outcomes'
param_table_codelist_charlson = f'{proj}_codelist_charlson'


# -----------------------------------------------------------------------------
# curated tables
# -----------------------------------------------------------------------------
# param_table_cur_covid             = f'{proj}_cur_covid'
# param_table_cur_vacc              = f'{proj}_cur_vacc'
param_table_cur_deaths_nodup      = f'{proj}_cur_deaths_nodup'
param_table_cur_deaths_nodup_long = f'{proj}_cur_deaths_nodup_long'
param_table_cur_hes_apc_long      = f'{proj}_cur_hes_apc_long'
# param_table_cur_hes_apc_oper_long = f'{proj}_cur_hes_apc_oper_long'
param_table_cur_lsoa_region       = f'{proj}_cur_lsoa_region'
param_table_cur_lsoa_imd          = f'{proj}_cur_lsoa_imd'


# -----------------------------------------------------------------------------
# temporary tables
# -----------------------------------------------------------------------------
param_table_tmp_kpc_harmonised       = f'{proj}_tmp_kpc_harmonised'
param_table_tmp_skinny               = f'{proj}_tmp_skinny'
param_table_tmp_lsoa                 = f'{proj}_tmp_lsoa'
param_table_tmp_qa                   = f'{proj}_tmp_qa'
param_table_tmp_inc_exc              = f'{proj}_tmp_inc_exc'
param_table_tmp_inc_exc_flow         = f'{proj}_tmp_inc_exc_flow'


# -----------------------------------------------------------------------------
# output tables
# -----------------------------------------------------------------------------
param_table_out_cohort               = f'{proj}_out_cohort'

# path_tmp_covariates_hes_apc             = f'{dbc}.{proj}_tmp_covariates_hes_apc'
# path_tmp_covariates_pmeds               = f'{dbc}.{proj}_tmp_covariates_pmeds'
# path_tmp_covariates_lsoa                = f'{dbc}.{proj}_tmp_covariates_lsoa'
# path_tmp_covariates_lsoa_2              = f'{dbc}.{proj}_tmp_covariates_lsoa_2'
# path_tmp_covariates_lsoa_3              = f'{dbc}.{proj}_tmp_covariates_lsoa_3'
# path_tmp_covariates_n_consultations     = f'{dbc}.{proj}_tmp_covariates_n_consultations'
# path_tmp_covariates_unique_bnf_chapters = f'{dbc}.{proj}_tmp_covariates_unique_bnf_chapters'
# path_tmp_covariates_hx_out_1st_wide     = f'{dbc}.{proj}_tmp_covariates_hx_out_1st_wide'
# path_tmp_covariates_hx_com_1st_wide     = f'{dbc}.{proj}_tmp_covariates_hx_com_1st_wide'



# path_out_codelist_quality_assurance      = f'{dbc}.{proj}_out_codelist_quality_assurance'
# path_out_codelist_cvd                    = f'{dbc}.{proj}_out_codelist_cvd'
# path_out_codelist_comorbidity            = f'{dbc}.{proj}_out_codelist_comorbidity'
# path_out_codelist_covid                  = f'{dbc}.{proj}_out_codelist_covid'

# table_out_cohort                          = f'{proj}_out_cohort'

# path_out_covariates                 = f'{dbc}.{proj}_out_covariates'
# path_out_exposures                  = f'{dbc}.{proj}_out_exposures'
# path_out_outcomes                   = f'{dbc}.{proj}_out_outcomes'

# COMMAND ----------

# %md # 5. Summary

# COMMAND ----------

# print variables, archived_on table, and paths for reference in subsequent notebooks
print(f'study:')
print("  {0:<22}".format('project') + " = " + f'{project}') 
print("  {0:<22}".format('subproject') + " = " + f'{subproject}') 
print("  {0:<22}".format('cohort') + " = " + f'{cohort}') 
print("  {0:<22}".format('version') + " = " + f'{version}') 
print(f'')
print("  {0:<22}".format('proj') + " = " + f'{proj}') 
print(f'')
print(f'study dates:')    
print("  {0:<22}".format('study_start_date') + " = " + f'{study_start_date}') 
print("  {0:<22}".format('study_end_date') + " = " + f'{study_end_date}') 
print(f'')
print(f'databases:')
print("  {0:<22}".format('db') + " = " + f'{db}') 
print("  {0:<22}".format('dbc') + " = " + f'{dbc}') 
print(f'')
print(f'Pipeline production date:')    
print("  {0:<22}".format('pipeline_production_date') + " = " + f'{pipeline_production_date}') 
print(f'')
print(f'parameters_df_datasets:')
print(parameters_df_datasets[['dataset', 'database', 'table', 'archived_on', 'n', 'pipeline_production_date']].to_string())
print(f'')
print(f'parameters:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
# print(f'')
# print(f'  df_archive')
# print(df_archive[['dataset', 'database', 'table', 'productionDate']].to_string())
tmp = vars().copy()
for var in list(tmp.keys()):
  if(re.match('^param_.*$', var)):
    print("  {0:<22}".format(var) + " = " + tmp[var])    
 
  
#print(f'composite_events:')   
#for i, c in enumerate(composite_events):
  #print('  ', i, c, '=', composite_events[c])
#print(f'')
# print(f'Out dates:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
# print(f'')
# print(f'  df_out')
#print(df_out[['dataset', 'out_date']].to_string())
#print(f'')