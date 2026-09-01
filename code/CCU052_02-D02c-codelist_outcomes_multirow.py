# Databricks notebook source
# MAGIC %md # CCU052_02-D02c-codelist_outcomes_multirow
# MAGIC
# MAGIC **Description** This notebook creates the code list for the multiple row outcomes, which includes code lists for acute exacerbations of ashtma, chronic obstructive pulmonary disease (COPD), and interstitial lung disease (ILD).
# MAGIC
# MAGIC **Authors** Hannah Whittaker, Thomas Bolton
# MAGIC
# MAGIC **Data input** -
# MAGIC
# MAGIC **Data output** codelist_outcomes_multirow

# COMMAND ----------

# MAGIC %md # 0. Setup

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# MAGIC %run "../../../../shds/common/functions"

# COMMAND ----------

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU052_02-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data

# COMMAND ----------

codelist_hw = spark.createDataFrame([

# aecopd - snomed
("106001000119101","chronic obstructive lung disease co-occurrent with acute bronchitis (disorder)", "aecopd", "snomed"),
("196001008","chronic obstructive pulmonary disease with acute lower respiratory infection (disorder)", "aecopd", "snomed"),
("293241000119100","acute exacerbation of chronic obstructive bronchitis (disorder)", "aecopd", "snomed"),
("425748003","acute exacerbation of chronic bronchitis (disorder)", "aecopd", "snomed"),
("10509002","acute bronchitis (disorder)", "aecopd", "snomed"),
("195717003","acute purulent bronchitis (disorder)", "aecopd", "snomed"),
("195719000","acute pneumococcal bronchitis (disorder)", "aecopd", "snomed"),
("195720006","acute streptococcal bronchitis (disorder)", "aecopd", "snomed"),
("195721005","acute haemophilus influenzae bronchitis (disorder)", "aecopd", "snomed"),
("195722003","acute moraxella catarrhalis bronchitis (disorder)", "aecopd", "snomed"),
("195725001","acute coxsackievirus bronchitis (disorder)", "aecopd", "snomed"),
("195726000","acute parainfluenza virus bronchitis (disorder)", "aecopd", "snomed"),
("195727009","acute respiratory syncytial virus bronchitis (disorder)", "aecopd", "snomed"),
("195728004","acute bronchitis caused by rhinovirus (disorder)", "aecopd", "snomed"),
("195729007","acute echovirus bronchitis (disorder)", "aecopd", "snomed"),
("195737004","acute exudative bronchiolitis (disorder)", "aecopd", "snomed"),
("195739001","acute bronchiolitis caused by respiratory syncytial virus (disorder)", "aecopd", "snomed"),
("195742007","acute lower respiratory tract infection (disorder)", "aecopd", "snomed"),
("233598009","acute bacterial bronchitis (disorder)", "aecopd", "snomed"),
("233599001","acute mycoplasmal bronchitis (disorder)", "aecopd", "snomed"),
("233600003","acute chlamydial bronchitis (disorder)", "aecopd", "snomed"),
("233601004","acute viral bronchitis (disorder)", "aecopd", "snomed"),
("233602006","acute viral bronchiolitis (disorder)", "aecopd", "snomed"),
("233603001","acute bronchiolitis caused by adenovirus (disorder)", "aecopd", "snomed"),
("24662006","influenza caused by influenza b virus (disorder)", "aecopd", "snomed"),
("275499005","acute wheezy bronchitis (disorder)", "aecopd", "snomed"),
("312119006","bacterial lower respiratory infection (disorder)", "aecopd", "snomed"),
("312134000","viral lower respiratory infection (disorder)", "aecopd", "snomed"),
("312371005","acute infective bronchitis (disorder)", "aecopd", "snomed"),
("315642008","influenza-like symptoms (finding)", "aecopd", "snomed"),
("36426008","subacute bronchitis (disorder)", "aecopd", "snomed"),
("427873006","influenza caused by influenza virus type a, avian, h5n1 strain (disorder)", "aecopd", "snomed"),
("442438000","influenza caused by influenza a virus (disorder)", "aecopd", "snomed"),
("442696006","influenza caused by influenza a virus subtype h1n1 (disorder)", "aecopd", "snomed"),
("448739000","recurrent lower respiratory tract infection (disorder)", "aecopd", "snomed"),
("450715004","influenza caused by influenza a virus subtype h7 (disorder)", "aecopd", "snomed"),
("450716003","influenza caused by influenza a virus subtype h9 (disorder)", "aecopd", "snomed"),
("50417007","lower respiratory tract infection (disorder)", "aecopd", "snomed"),
("5875001","acute bronchitis with obstruction (disorder)", "aecopd", "snomed"),
("6142004","influenza (disorder)", "aecopd", "snomed"),
("707448003","influenza caused by influenza a virus subtype h7n9 (disorder)", "aecopd", "snomed"),
("711128004","influenza caused by influenza virus type a, avian, h3n2 strain (disorder)", "aecopd", "snomed"),
("713083002","influenza caused by influenza a virus subtype h5 (disorder)", "aecopd", "snomed"),
("719590007","influenza caused by seasonal influenza virus (disorder)", "aecopd", "snomed"),
("719865001","influenza caused by pandemic influenza virus (disorder)", "aecopd", "snomed"),
("772810003","influenza caused by influenza a virus subtype h3n2 (disorder)", "aecopd", "snomed"),
("772828001","influenza caused by influenza a virus subtype h5n1 (disorder)", "aecopd", "snomed"),
("785745000","acute bronchitis co-occurrent with wheeze (disorder)", "aecopd", "snomed"),
("80257001","acute bronchitis with bronchospasm (disorder)", "aecopd", "snomed"),
("81524006","influenza caused by influenza c virus (disorder)", "aecopd", "snomed"),
("95891005","influenza-like illness (finding)", "aecopd", "snomed"),
("138389411000119105","acute bronchitis caused by severe acute respiratory syndrome coronavirus 2 (disorder)", "aecopd", "snomed"),
("880529761000119102","lower respiratory infection caused by severe acute respiratory syndrome coronavirus 2 (disorder)", "aecopd", "snomed"),

# aecopd - hes 
("J44.1", "COPD with acute exacberbation", "aecopd","icd10"),
("J44.0",  "COPD with acute lower tract infection", "aecopd", "icd10"),
("J44.9", "COPD unspecified", "aecopd", "icd10"),
("J44.8", "Other COPD", "aecopd","icd10"),
("J22", "Unspecified acute lower respiratory infection", "aecopd","icd10"),

# aeasthma - snomed
("10692721000119102","chronic obstructive asthma co-occurrent with actue exacerbation of asthma (disorder)", "aeasthma", "snomed"),  
  
# aeasthma - hes 
("J45",	"Asthma", "aeasthma",	"icd10"),
("J45.0", "Predominantly allergic asthma", "aeasthma", "icd10"),
("J45.1", "Nonallergic asthma",	"aeasthma", "icd10"),
("J45.8", "Mixed asthma", "aeasthma",	"icd10"),
("J45.9", "Asthma, unspecified", "aeasthma", "icd10"),
("J46",	"Status asthmaticus", "aeasthma",	"icd10"),

# aeasthma - hes - supporting - here as a placeholder - see bespoke coding in outcomes_multirow notebook
("R06.2",	"Wheezing", "aeasthma_supporting",	"icd10"),
("B34.9",	"Viral infection, unspecified", "aeasthma_supporting",	"icd10")

], ['code', 'term', 'name', 'terminology'])

# check
tmpt = tab(codelist_hw, 'name', 'terminology'); print()


codelist_hw_2 = spark.createDataFrame([

# ild - snomed - none. 

# ild - hes
("ILD","ICD10","J70.2","acute drug-induced interstitial lung disorders","1","20230803"), # added in 20231201
("ILD","ICD10","J70.0","acute pulmonary manifestations due to radiation","1","20230803"),
("ILD","ICD10","J67.7","air-conditioner and humidifier lung","1","20230803"),
("ILD","ICD10","J66.8","airway disease due to other specific organic dusts","1","20230803"),
("ILD","ICD10","J66","airway disease due to specific organic dust","1","20230803"),
("ILD","ICD10","J63.0","aluminosis (of lung)","1","20230803"),
("ILD","ICD10","J84.0","alveolar and parietoalveolar conditions","1","20230803"),
("ILD","ICD10","B44","aspergillosis","1","20230803"),
("ILD","ICD10","B44.9","aspergillosis, unspecified","1","20230803"),
("ILD","ICD10","J67.1","bagassosis","1","20230803"),
("ILD","ICD10","J63.1","bauxite fibrosis (of lung)","1","20230803"),
("ILD","ICD10","J63.2","berylliosis","1","20230803"),
("ILD","ICD10","J67.2","bird fancier lung","1","20230803"),
("ILD","ICD10","J68.0","bronchitis and pneumonitis due to chemicals, gases, fumes and vapours","1","20230803"),
("ILD","ICD10","J66.0","byssinosis (airway disease due to cotton dust)","1","20230803"),
("ILD","ICD10","J66.2","cannabinosis","1","20230803"),
("ILD","ICD10","J70.1","chronic and other pulmonary manifestations due to radiation","1","20230803"),
("ILD","ICD10","J70.3","chronic drug-induced interstitial lung disorders","1","20230803"),
("ILD","ICD10","J68.4","chronic respiratory conditions due to chemicals, gases, fumes and vapours","1","20230803"),
("ILD","ICD10","J60","coalworker pneumoconiosis","1","20230803"),
("ILD","ICD10","B44.7","disseminated aspergillosis","1","20230803"),
("ILD","ICD10","J70.4","drug-induced interstitial lung disorders, unspecified","1","20230803"),
("ILD","ICD10","M32.0","drug-induced systemic lupus erythematosus","1","20230803"),
("ILD","ICD10","J67.0","farmer lung","1","20230803"),
("ILD","ICD10","J66.1","flax-dresser disease","1","20230803"),
("ILD","ICD10","J63.3","graphite fibrosis (of lung)","1","20230803"),
("ILD","ICD10","B22.1","hiv disease resulting in lymphoid interstitial pneumonitis","1","20230803"),
("ILD","ICD10","J67","hypersensitivity pneumonitis due to organic dust","1","20230803"),
("ILD","ICD10","J67.8","hypersensitivity pneumonitis due to other organic dusts","1","20230803"),
("ILD","ICD10","J67.9","hypersensitivity pneumonitis due to unspecified organic dust","1","20230803"),
("ILD","ICD10","M60.0","infective myositis","1","20230803"),
("ILD","ICD10","J98.2","interstitial emphysema","1","20230803"),
("ILD","ICD10","M60.1","interstitial myositis","1","20230803"),
("ILD","ICD10","J84.9","interstitial pulmonary disease unspecified","1","20230803"),
("ILD","ICD10","B44.0","invasive pulmonary aspergillosis","1","20230803")

], ['name', 'terminology', 'code', 'term', 'code_type', 'date'])  

codelist_hw_2 = (
  codelist_hw_2
  .withColumn('name', f.lit('aeild'))
)

# check
tmpt = tab(codelist_hw_2, 'name', 'terminology'); print()
tmpt = tab(codelist_hw_2, 'code_type', 'date'); print()

# combine
codelist = (
  codelist_hw
  .unionByName(codelist_hw_2.drop('code_type', 'date'))
  .withColumn('terminology', f.upper(f.col('terminology')))
  .select('name', 'terminology', 'code', 'term')
)

# check
tmpt = tab(codelist, 'name', 'terminology'); print()

# COMMAND ----------

# MAGIC %md # 3. Check

# COMMAND ----------

# check
tmpt = tab(codelist, 'name'); print()
tmpt = tab(codelist, 'terminology'); print()
tmpt = tab(codelist, 'name', 'terminology'); print()
count_varlist(codelist, ['name', 'terminology', 'code'])
print(codelist.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()
# tmpt = tab(codelist, 'name', 'code_type'); print()
# assert codelist.where(f.col('code_type') == 0).count() == 0
display(codelist.orderBy('name', 'terminology', 'code'))

# COMMAND ----------

# MAGIC %md # 4. Reformat

# COMMAND ----------

# remove trailing X's, decimal points, dashes, and spaces
codelist = (
  codelist
  .withColumn('_code_old', f.col('code'))
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))
  .withColumn('_code_diff', f.when(f.col('code') != f.col('_code_old'), 1).otherwise(0))
)

# check
tmpt = tab(codelist, '_code_diff'); print()
print(codelist.where(f.col('_code_diff') == 1).orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# tidy
codelist = codelist.drop('_code_old', '_code_diff')

# COMMAND ----------

# MAGIC %md # 5. Check

# COMMAND ----------

# check 
tmpt = tab(codelist, 'name', 'terminology')
display(codelist)
display(codelist.where(f.col('terminology') == 'ICD10'))

# COMMAND ----------

# MAGIC %md # 6. Save

# COMMAND ----------

# save
save_table(df=codelist, out_name=f'{proj}_codelist_outcomes_multirow', save_previous=True, dbc=dbc)