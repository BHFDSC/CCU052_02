# Databricks notebook source
# MAGIC %md # CCU052_02-D02d-codelist_charlso
# MAGIC
# MAGIC **Description** This notebook creates the code list for the Charlson Comorbidity Index
# MAGIC
# MAGIC **Authors** Thomas Bolton, Hannah Whittaker (based on work from CCU002 and the Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data input** -
# MAGIC
# MAGIC **Data output** codelist_charlson

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

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU052_02-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

# create ICD10 lookup spark table
icd10 = spark.table(param_path_ref_icd10)

# restrict to latest available version of ICD10
icd10 = icd10\
  .where(f.col('VERSION') == "5th ed")\
  .select('ALT_CODE', 'ICD10_DESCRIPTION')\
  .withColumnRenamed('ALT_CODE', 'code')\
  .withColumnRenamed('ICD10_DESCRIPTION', 'term')

# remove trailing X's, decimal points, dashes, and spaces
icd10 = icd10\
  .withColumn('code', f.regexp_replace('code', r'X$', ''))\
  .withColumn('code', f.regexp_replace('code', r'[\.\-\s]', ''))

# check for duplicates
count_var(icd10, 'code')

# COMMAND ----------

# MAGIC %md # 2 Codelists - Charlson

# COMMAND ----------

codelist_charlson = """
code,code_desc,name
C00,Malignant neoplasm of lip,Cancer
C000,Malignant neoplasm of external upper lip,Cancer
C001,Malignant neoplasm of external lower lip,Cancer
C002,"Malignant neoplasm of external lip, unspecified",Cancer
C003,"Malignant neoplasm of upper lip, inner aspect",Cancer
C004,"Malignant neoplasm of lower lip, inner aspect",Cancer
C005,"Malignant neoplasm of lip, unspecified, inner aspect",Cancer
C006,"Malignant neoplasm of commissure of lip, unspecified",Cancer
C008,Malignant neoplasm of overlapping sites of lip,Cancer
C009,"Malignant neoplasm of lip, unspecified",Cancer
C00X,Malignant neoplasm of lip,Cancer
C01,Malignant neoplasm of base of tongue,Cancer
C01X,Malignant neoplasm of base of tongue,Cancer
C02,Malignant neoplasm of other and unspecified parts of tongue,Cancer
C020,Malignant neoplasm of dorsal surface of tongue,Cancer
C021,Malignant neoplasm of border of tongue,Cancer
C022,Malignant neoplasm of ventral surface of tongue,Cancer
C023,"Malignant neoplasm of anterior two-thirds of tongue, part unspecified",Cancer
C024,Malignant neoplasm of lingual tonsil,Cancer
C028,Malignant neoplasm of overlapping sites of tongue,Cancer
C029,"Malignant neoplasm of tongue, unspecified",Cancer
C02X,Malignant neoplasm of other and unspecified parts of tongue,Cancer
C03,Malignant neoplasm of gum,Cancer
C030,Malignant neoplasm of upper gum,Cancer
C031,Malignant neoplasm of lower gum,Cancer
C039,"Malignant neoplasm of gum, unspecified",Cancer
C03X,Malignant neoplasm of gum,Cancer
C04,Malignant neoplasm of floor of mouth,Cancer
C040,Malignant neoplasm of anterior floor of mouth,Cancer
C041,Malignant neoplasm of lateral floor of mouth,Cancer
C048,Malignant neoplasm of overlapping sites of floor of mouth,Cancer
C049,"Malignant neoplasm of floor of mouth, unspecified",Cancer
C04X,Malignant neoplasm of floor of mouth,Cancer
C05,Malignant neoplasm of palate,Cancer
C050,Malignant neoplasm of hard palate,Cancer
C051,Malignant neoplasm of soft palate,Cancer
C052,Malignant neoplasm of uvula,Cancer
C058,Malignant neoplasm of overlapping sites of palate,Cancer
C059,"Malignant neoplasm of palate, unspecified",Cancer
C05X,Malignant neoplasm of palate,Cancer
C06,Malignant neoplasm of other and unspecified parts of mouth,Cancer
C060,Malignant neoplasm of cheek mucosa,Cancer
C061,Malignant neoplasm of vestibule of mouth,Cancer
C062,Malignant neoplasm of retromolar area,Cancer
C068,Malignant neoplasm of overlapping sites of other and unspecified parts of mouth,Cancer
C069,"Malignant neoplasm of mouth, unspecified",Cancer
C06X,Malignant neoplasm of other and unspecified parts of mouth,Cancer
C07,Malignant neoplasm of parotid gland,Cancer
C07X,Malignant neoplasm of parotid gland,Cancer
C08,Malignant neoplasm of other and unspecified major salivary glands,Cancer
C080,Malignant neoplasm of submandibular gland,Cancer
C081,Malignant neoplasm of sublingual gland,Cancer
C088,Malignant neoplasm of overlapping lesion of major salivary glands,Cancer
C089,"Malignant neoplasm of major salivary gland, unspecified",Cancer
C08X,Malignant neoplasm of other and unspecified major salivary glands,Cancer
C09,Malignant neoplasm of tonsil,Cancer
C090,Malignant neoplasm of tonsillar fossa,Cancer
C091,Malignant neoplasm of tonsillar pillar (anterior) (posterior),Cancer
C098,Malignant neoplasm of overlapping sites of tonsil,Cancer
C099,"Malignant neoplasm of tonsil, unspecified",Cancer
C09X,Malignant neoplasm of tonsil,Cancer
C10,Malignant neoplasm of oropharynx,Cancer
C100,Malignant neoplasm of vallecula,Cancer
C101,Malignant neoplasm of anterior surface of epiglottis,Cancer
C102,Malignant neoplasm of lateral wall of oropharynx,Cancer
C103,Malignant neoplasm of posterior wall of oropharynx,Cancer
C104,Malignant neoplasm of branchial cleft,Cancer
C108,Malignant neoplasm of overlapping sites of oropharynx,Cancer
C109,"Malignant neoplasm of oropharynx, unspecified",Cancer
C10X,Malignant neoplasm of oropharynx,Cancer
C11,Malignant neoplasm of nasopharynx,Cancer
C110,Malignant neoplasm of superior wall of nasopharynx,Cancer
C111,Malignant neoplasm of posterior wall of nasopharynx,Cancer
C112,Malignant neoplasm of lateral wall of nasopharynx,Cancer
C113,Malignant neoplasm of anterior wall of nasopharynx,Cancer
C118,Malignant neoplasm of overlapping sites of nasopharynx,Cancer
C119,"Malignant neoplasm of nasopharynx, unspecified",Cancer
C11X,Malignant neoplasm of nasopharynx,Cancer
C12,Malignant neoplasm of pyriform sinus,Cancer
C12X,Malignant neoplasm of pyriform sinus,Cancer
C13,Malignant neoplasm of hypopharynx,Cancer
C130,Malignant neoplasm of postcricoid region,Cancer
C131,"Malignant neoplasm of aryepiglottic fold, hypopharyngeal aspect",Cancer
C132,Malignant neoplasm of posterior wall of hypopharynx,Cancer
C138,Malignant neoplasm of overlapping sites of hypopharynx,Cancer
C139,"Malignant neoplasm of hypopharynx, unspecified",Cancer
C13X,Malignant neoplasm of hypopharynx,Cancer
C14,"Malignant neoplasm of other and ill-defined sites in the lip, oral cavity and pharynx",Cancer
C140,"Malignant neoplasm of pharynx, unspecified",Cancer
C142,Malignant neoplasm of Waldeyer's ring,Cancer
C148,"Malignant neoplasm of overlapping sites of lip, oral cavity and pharynx",Cancer
C14X,"Malignant neoplasm of other and ill-defined sites in the lip, oral cavity and pharynx",Cancer
C15,Malignant neoplasm of esophagus,Cancer
C150,Malignant neoplasm of cervical part of esophagus,Cancer
C151,Malignant neoplasm of thoracic part of esophagus,Cancer
C152,Malignant neoplasm of abdominal part of esophagus,Cancer
C153,Malignant neoplasm of upper third of esophagus,Cancer
C154,Malignant neoplasm of middle third of esophagus,Cancer
C155,Malignant neoplasm of lower third of esophagus,Cancer
C158,Malignant neoplasm of overlapping sites of esophagus,Cancer
C159,"Malignant neoplasm of esophagus, unspecified",Cancer
C15X,Malignant neoplasm of esophagus,Cancer
C16,Malignant neoplasm of stomach,Cancer
C160,Malignant neoplasm of cardia,Cancer
C161,Malignant neoplasm of fundus of stomach,Cancer
C162,Malignant neoplasm of body of stomach,Cancer
C163,Malignant neoplasm of pyloric antrum,Cancer
C164,Malignant neoplasm of pylorus,Cancer
C165,"Malignant neoplasm of lesser curvature of stomach, unspecified",Cancer
C166,"Malignant neoplasm of greater curvature of stomach, unspecified",Cancer
C168,Malignant neoplasm of overlapping sites of stomach,Cancer
C169,"Malignant neoplasm of stomach, unspecified",Cancer
C16X,Malignant neoplasm of stomach,Cancer
C17,Malignant neoplasm of small intestine,Cancer
C170,Malignant neoplasm of duodenum,Cancer
C171,Malignant neoplasm of jejunum,Cancer
C172,Malignant neoplasm of ileum,Cancer
C173,"Meckel's diverticulum, malignant",Cancer
C178,Malignant neoplasm of overlapping sites of small intestine,Cancer
C179,"Malignant neoplasm of small intestine, unspecified",Cancer
C17X,Malignant neoplasm of small intestine,Cancer
C18,Malignant neoplasm of colon,Cancer
C180,Malignant neoplasm of cecum,Cancer
C181,Malignant neoplasm of appendix,Cancer
C182,Malignant neoplasm of ascending colon,Cancer
C183,Malignant neoplasm of hepatic flexure,Cancer
C184,Malignant neoplasm of transverse colon,Cancer
C185,Malignant neoplasm of splenic flexure,Cancer
C186,Malignant neoplasm of descending colon,Cancer
C187,Malignant neoplasm of sigmoid colon,Cancer
C188,Malignant neoplasm of overlapping sites of colon,Cancer
C189,"Malignant neoplasm of colon, unspecified",Cancer
C18X,Malignant neoplasm of colon,Cancer
C19,Malignant neoplasm of rectosigmoid junction,Cancer
C19X,Malignant neoplasm of rectosigmoid junction,Cancer
C20,Malignant neoplasm of rectum,Cancer
C20X,Malignant neoplasm of rectum,Cancer
C21,Malignant neoplasm of anus and anal canal,Cancer
C210,"Malignant neoplasm of anus, unspecified",Cancer
C211,Malignant neoplasm of anal canal,Cancer
C212,Malignant neoplasm of cloacogenic zone,Cancer
C218,"Malignant neoplasm of overlapping sites of rectum, anus and anal canal",Cancer
C21X,Malignant neoplasm of anus and anal canal,Cancer
C22,Malignant neoplasm of liver and intrahepatic bile ducts,Cancer
C220,Liver cell carcinoma,Cancer
C221,Intrahepatic bile duct carcinoma,Cancer
C222,Hepatoblastoma,Cancer
C223,Angiosarcoma of liver,Cancer
C224,Other sarcomas of liver,Cancer
C227,Other specified carcinomas of liver,Cancer
C228,"Malignant neoplasm of liver, primary, unspecified as to type",Cancer
C229,"Malignant neoplasm of liver, not specified as primary or secondary",Cancer
C22X,Malignant neoplasm of liver and intrahepatic bile ducts,Cancer
C23,Malignant neoplasm of gallbladder,Cancer
C23X,Malignant neoplasm of gallbladder,Cancer
C24,Malignant neoplasm of other and unspecified parts of biliary tract,Cancer
C240,Malignant neoplasm of extrahepatic bile duct,Cancer
C241,Malignant neoplasm of ampulla of Vater,Cancer
C248,Malignant neoplasm of overlapping sites of biliary tract,Cancer
C249,"Malignant neoplasm of biliary tract, unspecified",Cancer
C24X,Malignant neoplasm of other and unspecified parts of biliary tract,Cancer
C25,Malignant neoplasm of pancreas,Cancer
C250,Malignant neoplasm of head of pancreas,Cancer
C251,Malignant neoplasm of body of pancreas,Cancer
C252,Malignant neoplasm of tail of pancreas,Cancer
C253,Malignant neoplasm of pancreatic duct,Cancer
C254,Malignant neoplasm of endocrine pancreas,Cancer
C257,Malignant neoplasm of other parts of pancreas,Cancer
C258,Malignant neoplasm of overlapping sites of pancreas,Cancer
C259,"Malignant neoplasm of pancreas, unspecified",Cancer
C25X,Malignant neoplasm of pancreas,Cancer
C26,Malignant neoplasm of other and ill-defined digestive organs,Cancer
C260,"Malignant neoplasm of intestinal tract, part unspecified",Cancer
C261,Malignant neoplasm of spleen,Cancer
C269,Malignant neoplasm of ill-defined sites within the digestive system,Cancer
C26X,Malignant neoplasm of other and ill-defined digestive organs,Cancer
C30,Malignant neoplasm of nasal cavity and middle ear,Cancer
C300,Malignant neoplasm of nasal cavity,Cancer
C301,Malignant neoplasm of middle ear,Cancer
C30X,Malignant neoplasm of nasal cavity and middle ear,Cancer
C31,Malignant neoplasm of accessory sinuses,Cancer
C310,Malignant neoplasm of maxillary sinus,Cancer
C311,Malignant neoplasm of ethmoidal sinus,Cancer
C312,Malignant neoplasm of frontal sinus,Cancer
C313,Malignant neoplasm of sphenoid sinus,Cancer
C318,Malignant neoplasm of overlapping sites of accessory sinuses,Cancer
C319,"Malignant neoplasm of accessory sinus, unspecified",Cancer
C31X,Malignant neoplasm of accessory sinuses,Cancer
C32,Malignant neoplasm of larynx,Cancer
C320,Malignant neoplasm of glottis,Cancer
C321,Malignant neoplasm of supraglottis,Cancer
C322,Malignant neoplasm of subglottis,Cancer
C323,Malignant neoplasm of laryngeal cartilage,Cancer
C328,Malignant neoplasm of overlapping sites of larynx,Cancer
C329,"Malignant neoplasm of larynx, unspecified",Cancer
C32X,Malignant neoplasm of larynx,Cancer
C33,Malignant neoplasm of trachea,Cancer
C33X,Malignant neoplasm of trachea,Cancer
C34,Malignant neoplasm of bronchus and lung,Cancer
C340,Malignant neoplasm of main bronchus,Cancer
C341,"Malignant neoplasm of upper lobe, bronchus or lung",Cancer
C342,"Malignant neoplasm of middle lobe, bronchus or lung",Cancer
C343,"Malignant neoplasm of lower lobe, bronchus or lung",Cancer
C348,Malignant neoplasm of overlapping sites of bronchus and lung,Cancer
C349,Malignant neoplasm of unspecified part of bronchus or lung,Cancer
C34X,Malignant neoplasm of bronchus and lung,Cancer
C37,Malignant neoplasm of thymus,Cancer
C37X,Malignant neoplasm of thymus,Cancer
C38,"Malignant neoplasm of heart, mediastinum and pleura",Cancer
C380,Malignant neoplasm of heart,Cancer
C381,Malignant neoplasm of anterior mediastinum,Cancer
C382,Malignant neoplasm of posterior mediastinum,Cancer
C383,"Malignant neoplasm of mediastinum, part unspecified",Cancer
C384,Malignant neoplasm of pleura,Cancer
C388,"Malignant neoplasm of overlapping sites of heart, mediastinum and pleura",Cancer
C38X,Malignant neoplasm of heart,Cancer
C39,Malignant neoplasm of other and ill-defined sites in the respiratory system and intrathoracic organs,Cancer
C390,"Malignant neoplasm of upper respiratory tract, part unspecified",Cancer
C398,Malignant neoplasm of overlapping lesion of respiratory and intrathoracic organs,Cancer
C399,"Malignant neoplasm of lower respiratory tract, part unspecified",Cancer
C39X,Malignant neoplasm of other and ill-defined sites in the respiratory system and intrathoracic organs,Cancer
C40,Malignant neoplasm of bone and articular cartilage of limbs,Cancer
C400,Malignant neoplasm of scapula and long bones of upper limb,Cancer
C401,Malignant neoplasm of short bones of upper limb,Cancer
C402,Malignant neoplasm of long bones of lower limb,Cancer
C403,Malignant neoplasm of short bones of lower limb,Cancer
C408,Malignant neoplasm of overlapping sites of bone and articular cartilage of limb,Cancer
C409,Malignant neoplasm of unspecified bones and articular cartilage of limb,Cancer
C40X,Malignant neoplasm of bone and articular cartilage of limbs,Cancer
C41,Malignant neoplasm of bone and articular cartilage of other and unspecified sites,Cancer
C410,Malignant neoplasm of bones of skull and face,Cancer
C411,Malignant neoplasm of mandible,Cancer
C412,Malignant neoplasm of vertebral column,Cancer
C413,"Malignant neoplasm of ribs, sternum and clavicle",Cancer
C414,"Malignant neoplasm of pelvic bones, sacrum and coccyx",Cancer
C418,Malignant neoplasm of overlapping lesion of bone and articular cartilage,Cancer
C419,"Malignant neoplasm of bone and articular cartilage, unspecified",Cancer
C41X,Malignant neoplasm of bone and articular cartilage of other and unspecified sites,Cancer
C43,Malignant melanoma of skin,Cancer
C430,Malignant melanoma of lip,Cancer
C431,"Malignant melanoma of eyelid, including canthus",Cancer
C432,Malignant melanoma of ear and external auricular canal,Cancer
C433,Malignant melanoma of other and unspecified parts of face,Cancer
C434,Malignant melanoma of scalp and neck,Cancer
C435,Malignant melanoma of trunk,Cancer
C436,"Malignant melanoma of upper limb, including shoulder",Cancer
C437,"Malignant melanoma of lower limb, including hip",Cancer
C438,Malignant melanoma of overlapping sites of skin,Cancer
C439,"Malignant melanoma of skin, unspecified",Cancer
C43X,Malignant melanoma of skin,Cancer
C45,Mesothelioma,Cancer
C450,Mesothelioma of pleura,Cancer
C451,Mesothelioma of peritoneum,Cancer
C452,Mesothelioma of pericardium,Cancer
C457,Mesothelioma of other sites,Cancer
C459,"Mesothelioma, unspecified",Cancer
C45X,Mesothelioma,Cancer
C46,Kaposi's sarcoma,Cancer
C460,Kaposi's sarcoma of skin,Cancer
C461,Kaposi's sarcoma of soft tissue,Cancer
C462,Kaposi's sarcoma of palate,Cancer
C463,Kaposi's sarcoma of lymph nodes,Cancer
C464,Kaposi's sarcoma of gastrointestinal sites,Cancer
C465,Kaposi's sarcoma of lung,Cancer
C467,Kaposi's sarcoma of other sites,Cancer
C468,Kaposi sarcoma of multiple organs,Cancer
C469,"Kaposi's sarcoma, unspecified",Cancer
C46X,Kaposi's sarcoma,Cancer
C47,Malignant neoplasm of peripheral nerves and autonomic nervous system,Cancer
C470,"Malignant neoplasm of peripheral nerves of head, face and neck",Cancer
C471,"Malignant neoplasm of peripheral nerves of upper limb, including shoulder",Cancer
C472,"Malignant neoplasm of peripheral nerves of lower limb, including hip",Cancer
C473,Malignant neoplasm of peripheral nerves of thorax,Cancer
C474,Malignant neoplasm of peripheral nerves of abdomen,Cancer
C475,Malignant neoplasm of peripheral nerves of pelvis,Cancer
C476,"Malignant neoplasm of peripheral nerves of trunk, unspecified",Cancer
C478,Malignant neoplasm of overlapping sites of peripheral nerves and autonomic nervous system,Cancer
C479,"Malignant neoplasm of peripheral nerves and autonomic nervous system, unspecified",Cancer
C47X,Malignant neoplasm of peripheral nerves and autonomic nervous system,Cancer
C48,Malignant neoplasm of retroperitoneum and peritoneum,Cancer
C480,Malignant neoplasm of retroperitoneum,Cancer
C481,Malignant neoplasm of specified parts of peritoneum,Cancer
C482,"Malignant neoplasm of peritoneum, unspecified",Cancer
C488,Malignant neoplasm of overlapping sites of retroperitoneum and peritoneum,Cancer
C48X,Malignant neoplasm of retroperitoneum and peritoneum,Cancer
C49,Malignant neoplasm of other connective and soft tissue,Cancer
C490,"Malignant neoplasm of connective and soft tissue of head, face and neck",Cancer
C491,"Malignant neoplasm of connective and soft tissue of upper limb, including shoulder",Cancer
C492,"Malignant neoplasm of connective and soft tissue of lower limb, including hip",Cancer
C493,Malignant neoplasm of connective and soft tissue of thorax,Cancer
C494,Malignant neoplasm of connective and soft tissue of abdomen,Cancer
C495,Malignant neoplasm of connective and soft tissue of pelvis,Cancer
C496,"Malignant neoplasm of connective and soft tissue of trunk, unspecified",Cancer
C498,Malignant neoplasm of overlapping sites of connective and soft tissue,Cancer
C499,"Malignant neoplasm of connective and soft tissue, unspecified",Cancer
C49X,Malignant neoplasm of other connective and soft tissue,Cancer
C50,Malignant neoplasm of breast,Cancer
C500,Malignant neoplasm of nipple and areola,Cancer
C501,Malignant neoplasm of central portion of breast,Cancer
C502,Malignant neoplasm of upper-inner quadrant of breast,Cancer
C503,Malignant neoplasm of lower-inner quadrant of breast,Cancer
C504,Malignant neoplasm of upper-outer quadrant of breast,Cancer
C505,Malignant neoplasm of lower-outer quadrant of breast,Cancer
C506,Malignant neoplasm of axillary tail of breast,Cancer
C508,Malignant neoplasm of overlapping sites of breast,Cancer
C509,Malignant neoplasm of breast of unspecified site,Cancer
C50X,Malignant neoplasm of breast,Cancer
C51,Malignant neoplasm of vulva,Cancer
C510,Malignant neoplasm of labium majus,Cancer
C511,Malignant neoplasm of labium minus,Cancer
C512,Malignant neoplasm of clitoris,Cancer
C518,Malignant neoplasm of overlapping sites of vulva,Cancer
C519,"Malignant neoplasm of vulva, unspecified",Cancer
C51X,Malignant neoplasm of vulva,Cancer
C52,Malignant neoplasm of vagina,Cancer
C52X,Malignant neoplasm of vagina,Cancer
C53,Malignant neoplasm of cervix uteri,Cancer
C530,Malignant neoplasm of endocervix,Cancer
C531,Malignant neoplasm of exocervix,Cancer
C538,Malignant neoplasm of overlapping sites of cervix uteri,Cancer
C539,"Malignant neoplasm of cervix uteri, unspecified",Cancer
C53X,Malignant neoplasm of cervix uteri,Cancer
C54,Malignant neoplasm of corpus uteri,Cancer
C540,Malignant neoplasm of isthmus uteri,Cancer
C541,Malignant neoplasm of endometrium,Cancer
C542,Malignant neoplasm of myometrium,Cancer
C543,Malignant neoplasm of fundus uteri,Cancer
C548,Malignant neoplasm of overlapping sites of corpus uteri,Cancer
C549,"Malignant neoplasm of corpus uteri, unspecified",Cancer
C54X,Malignant neoplasm of corpus uteri,Cancer
C55,"Malignant neoplasm of uterus, part unspecified",Cancer
C55X,"Malignant neoplasm of uterus, part unspecified",Cancer
C56,Malignant neoplasm of ovary,Cancer
C561,Malignant neoplasm of right ovary,Cancer
C562,Malignant neoplasm of left ovary,Cancer
C569,Malignant neoplasm of unspecified ovary,Cancer
C56X,Malignant neoplasm of left ovary,Cancer
C57,Malignant neoplasm of other and unspecified female genital organs,Cancer
C570,Malignant neoplasm of fallopian tube,Cancer
C571,Malignant neoplasm of broad ligament,Cancer
C572,Malignant neoplasm of round ligament,Cancer
C573,Malignant neoplasm of parametrium,Cancer
C574,"Malignant neoplasm of uterine adnexa, unspecified",Cancer
C577,Malignant neoplasm of other specified female genital organs,Cancer
C578,Malignant neoplasm of overlapping sites of female genital organs,Cancer
C579,"Malignant neoplasm of female genital organ, unspecified",Cancer
C57X,Malignant neoplasm of other and unspecified female genital organs,Cancer
C58,Malignant neoplasm of placenta,Cancer
C58X,Malignant neoplasm of placenta,Cancer
C60,Malignant neoplasm of penis,Cancer
C600,Malignant neoplasm of prepuce,Cancer
C601,Malignant neoplasm of glans penis,Cancer
C602,Malignant neoplasm of body of penis,Cancer
C608,Malignant neoplasm of overlapping sites of penis,Cancer
C609,"Malignant neoplasm of penis, unspecified",Cancer
C60X,Malignant neoplasm of penis,Cancer
C61,Malignant neoplasm of prostate,Cancer
C61X,Malignant neoplasm of prostate,Cancer
C62,Malignant neoplasm of testis,Cancer
C620,Malignant neoplasm of undescended testis,Cancer
C621,Malignant neoplasm of descended testis,Cancer
C629,"Malignant neoplasm of testis, unspecified whether descended or undescended",Cancer
C62X,Malignant neoplasm of prostate,Cancer
C63,Malignant neoplasm of other and unspecified male genital organs,Cancer
C630,Malignant neoplasm of epididymis,Cancer
C631,Malignant neoplasm of spermatic cord,Cancer
C632,Malignant neoplasm of scrotum,Cancer
C637,Malignant neoplasm of other specified male genital organs,Cancer
C638,Malignant neoplasm of overlapping sites of male genital organs,Cancer
C639,"Malignant neoplasm of male genital organ, unspecified",Cancer
C63X,Malignant neoplasm of other and unspecified male genital organs,Cancer
C64,"Malignant neoplasm of kidney, except renal pelvis",Cancer
C641,"Malignant neoplasm of right kidney, except renal pelvis",Cancer
C642,"Malignant neoplasm of left kidney, except renal pelvis",Cancer
C649,"Malignant neoplasm of unspecified kidney, except renal pelvis",Cancer
C64X,Malignant neoplasm of other and unspecified male genital organs,Cancer
C65,Malignant neoplasm of renal pelvis,Cancer
C651,Malignant neoplasm of right renal pelvis,Cancer
C652,Malignant neoplasm of left renal pelvis,Cancer
C659,Malignant neoplasm of unspecified renal pelvis,Cancer
C65X,Malignant neoplasm of renal pelvis,Cancer
C66,Malignant neoplasm of ureter,Cancer
C661,Malignant neoplasm of right ureter,Cancer
C662,Malignant neoplasm of left ureter,Cancer
C669,Malignant neoplasm of unspecified ureter,Cancer
C66X,Malignant neoplasm of ureter,Cancer
C67,Malignant neoplasm of bladder,Cancer
C670,Malignant neoplasm of trigone of bladder,Cancer
C671,Malignant neoplasm of dome of bladder,Cancer
C672,Malignant neoplasm of lateral wall of bladder,Cancer
C673,Malignant neoplasm of anterior wall of bladder,Cancer
C674,Malignant neoplasm of posterior wall of bladder,Cancer
C675,Malignant neoplasm of bladder neck,Cancer
C676,Malignant neoplasm of ureteric orifice,Cancer
C677,Malignant neoplasm of urachus,Cancer
C678,Malignant neoplasm of overlapping sites of bladder,Cancer
C679,"Malignant neoplasm of bladder, unspecified",Cancer
C67X,Malignant neoplasm of bladder,Cancer
C68,Malignant neoplasm of other and unspecified urinary organs,Cancer
C680,Malignant neoplasm of urethra,Cancer
C681,Malignant neoplasm of paraurethral glands,Cancer
C688,Malignant neoplasm of overlapping sites of urinary organs,Cancer
C689,"Malignant neoplasm of urinary organ, unspecified",Cancer
C68X,Malignant neoplasm of other and unspecified urinary organs,Cancer
C69,Malignant neoplasm of eye and adnexa,Cancer
C690,Malignant neoplasm of conjunctiva,Cancer
C691,Malignant neoplasm of cornea,Cancer
C692,Malignant neoplasm of retina,Cancer
C693,Malignant neoplasm of choroid,Cancer
C694,Malignant neoplasm of ciliary body,Cancer
C695,Malignant neoplasm of lacrimal gland and duct,Cancer
C696,Malignant neoplasm of orbit,Cancer
C698,Malignant neoplasm of overlapping sites of eye and adnexa,Cancer
C699,Malignant neoplasm of unspecified site of eye,Cancer
C69X,Malignant neoplasm of eye and adnexa,Cancer
C70,Malignant neoplasm of meninges,Cancer
C700,Malignant neoplasm of cerebral meninges,Cancer
C701,Malignant neoplasm of spinal meninges,Cancer
C709,"Malignant neoplasm of meninges, unspecified",Cancer
C70X,Malignant neoplasm of meninges,Cancer
C71,Malignant neoplasm of brain,Cancer
C710,"Malignant neoplasm of cerebrum, except lobes and ventricles",Cancer
C711,Malignant neoplasm of frontal lobe,Cancer
C712,Malignant neoplasm of temporal lobe,Cancer
C713,Malignant neoplasm of parietal lobe,Cancer
C714,Malignant neoplasm of occipital lobe,Cancer
C715,Malignant neoplasm of cerebral ventricle,Cancer
C716,Malignant neoplasm of cerebellum,Cancer
C717,Malignant neoplasm of brain stem,Cancer
C718,Malignant neoplasm of overlapping sites of brain,Cancer
C719,"Malignant neoplasm of brain, unspecified",Cancer
C71X,Malignant neoplasm of brain,Cancer
C72,"Malignant neoplasm of spinal cord, cranial nerves and other parts of central nervous system",Cancer
C720,Malignant neoplasm of spinal cord,Cancer
C721,Malignant neoplasm of cauda equina,Cancer
C722,Malignant neoplasm of olfactory nerve,Cancer
C723,Malignant neoplasm of optic nerve,Cancer
C724,Malignant neoplasm of acoustic nerve,Cancer
C725,Malignant neoplasm of other and unspecified cranial nerves,Cancer
C729,"Malignant neoplasm of central nervous system, unspecified",Cancer
C72X,"Malignant neoplasm of spinal cord, cranial nerves and other parts of central nervous system",Cancer
C73,Malignant neoplasm of thyroid gland,Cancer
C73X,Malignant neoplasm of thyroid gland,Cancer
C74,Malignant neoplasm of adrenal gland,Cancer
C740,Malignant neoplasm of cortex of adrenal gland,Cancer
C741,Malignant neoplasm of medulla of adrenal gland,Cancer
C749,Malignant neoplasm of unspecified part of adrenal gland,Cancer
C74X,Malignant neoplasm of adrenal gland,Cancer
C75,Malignant neoplasm of other endocrine glands and related structures,Cancer
C750,Malignant neoplasm of parathyroid gland,Cancer
C751,Malignant neoplasm of pituitary gland,Cancer
C752,Malignant neoplasm of craniopharyngeal duct,Cancer
C753,Malignant neoplasm of pineal gland,Cancer
C754,Malignant neoplasm of carotid body,Cancer
C755,Malignant neoplasm of aortic body and other paraganglia,Cancer
C758,"Malignant neoplasm with pluriglandular involvement, unspecified",Cancer
C759,"Malignant neoplasm of endocrine gland, unspecified",Cancer
C75X,Malignant neoplasm of other endocrine glands and related structures,Cancer
C76,Malignant neoplasm of other and ill-defined sites,Cancer
C760,"Malignant neoplasm of head, face and neck",Cancer
C761,Malignant neoplasm of thorax,Cancer
C762,Malignant neoplasm of abdomen,Cancer
C763,Malignant neoplasm of pelvis,Cancer
C764,Malignant neoplasm of upper limb,Cancer
C765,Malignant neoplasm of lower limb,Cancer
C768,Malignant neoplasm of other specified ill-defined sites,Cancer
C76X,Malignant neoplasm of abdomen,Cancer
C81,Hodgkin lymphoma,Cancer
C810,Nodular lymphocyte predominant Hodgkin lymphoma,Cancer
C811,Nodular sclerosis classical Hodgkin lymphoma,Cancer
C812,Mixed cellularity classical Hodgkin lymphoma,Cancer
C813,Lymphocyte depleted classical Hodgkin lymphoma,Cancer
C814,Lymphocyte-rich classical Hodgkin lymphoma,Cancer
C817,Other classical Hodgkin lymphoma,Cancer
C819,"Hodgkin lymphoma, unspecified",Cancer
C81X,Hodgkin lymphoma,Cancer
C82,Follicular lymphoma,Cancer
C820,Follicular lymphoma grade I,Cancer
C821,Follicular lymphoma grade II,Cancer
C822,"Follicular lymphoma grade III, unspecified",Cancer
C823,Follicular lymphoma grade IIIa,Cancer
C824,Follicular lymphoma grade IIIb,Cancer
C825,Diffuse follicle center lymphoma,Cancer
C826,Cutaneous follicle center lymphoma,Cancer
C827,Other types of follicular lymphoma,Cancer
C828,Other types of follicular lymphoma,Cancer
C829,"Follicular lymphoma, unspecified",Cancer
C82X,Follicular lymphoma,Cancer
C83,Non-follicular lymphoma,Cancer
C830,Small cell B-cell lymphoma,Cancer
C831,Mantle cell lymphoma,Cancer
C833,Diffuse large B-cell lymphoma,Cancer
C835,Lymphoblastic (diffuse) lymphoma,Cancer
C837,Burkitt lymphoma,Cancer
C838,Other non-follicular lymphoma,Cancer
C839,"Non-follicular (diffuse) lymphoma, unspecified",Cancer
C83X,Non-follicular lymphoma,Cancer
C84,Mature T/NK-cell lymphomas,Cancer
C840,Mycosis fungoides,Cancer
C841,Sezary disease,Cancer
C844,"Peripheral T-cell lymphoma, not classified",Cancer
C845,Other mature t/NK-cell lymphomas,Cancer
C846,"Anaplastic large cell lymphoma, ALK-positive",Cancer
C847,"Anaplastic large cell lymphoma, ALK-negative",Cancer
C848,Cutaneous t-cell lymphoma unspecified,Cancer
C849,"Mature T/NK-cell lymphomas, unspecified",Cancer
C84X,Mature T/NK-cell lymphomas,Cancer
C85,Other specified and unspecified types of non-Hodgkin lymphoma,Cancer
C851,Unspecified B-cell lymphoma,Cancer
C852,Mediastinal (thymic) large B-cell lymphoma,Cancer
C857,Other specified types of non-Hodgkin lymphoma,Cancer
C858,Other specified types of non-Hodgkin lymphoma,Cancer
C859,"Non-Hodgkin lymphoma, unspecified",Cancer
C85X,Other specified and unspecified types of non-Hodgkin lymphoma,Cancer
C88,Malignant immunoproliferative diseases and certain other B-cell lymphomas,Cancer
C880,Waldenstrom macroglobulinemia,Cancer
C882,Heavy chain disease,Cancer
C883,Immunoproliferative small intestinal disease,Cancer
C884,Extranodal marginal zone B-cell lymphoma of mucosa-associated lymphoid tissue [MALT-lymphoma],Cancer
C887,Other malignant immunoproliferative diseases,Cancer
C888,Other malignant immunoproliferative diseases,Cancer
C889,"Malignant immunoproliferative disease, unspecified",Cancer
C88X,Malignant immunoproliferative diseases and certain other B-cell lymphomas,Cancer
C90,Multiple myeloma and malignant plasma cell neoplasms,Cancer
C900,Multiple myeloma,Cancer
C901,Plasma cell leukemia,Cancer
C902,Extramedullary plasmacytoma,Cancer
C903,Solitary plasmacytoma,Cancer
C90X,Multiple myeloma,Cancer
C91,Lymphoid leukemia,Cancer
C910,Acute lymphoblastic leukemia [ALL],Cancer
C911,Chronic lymphocytic leukemia of B-cell type,Cancer
C913,Prolymphocytic leukemia of B-cell type,Cancer
C914,Hairy cell leukemia,Cancer
C915,Adult T-cell lymphoma/leukemia (HTLV-1-associated),Cancer
C916,Prolymphocytic leukemia of T-cell type,Cancer
C917,Other lymphoid leukaemia,Cancer
C918,Mature B-cell leukaemia Burkitt-type,Cancer
C919,"Lymphoid leukemia, unspecified",Cancer
C91X,Lymphoid leukemia,Cancer
C92,Myeloid leukemia,Cancer
C920,Acute myeloblastic leukemia,Cancer
C921,"Chronic myeloid leukemia, BCR/ABL-positive",Cancer
C922,"Atypical chronic myeloid leukemia, BCR/ABL-negative",Cancer
C923,Myeloid sarcoma,Cancer
C924,Acute promyelocytic leukemia,Cancer
C925,Acute myelomonocytic leukemia,Cancer
C926,Acute myeloid leukemia with 11q23-abnormality,Cancer
C927,Other myeloid leukaemia,Cancer
C928,Acute myeloid leukaemia with multilineage dysplasia,Cancer
C929,"Myeloid leukemia, unspecified",Cancer
C92X,Acute myeloblastic leukemia,Cancer
C93,Monocytic leukemia,Cancer
C930,Acute monoblastic/monocytic leukemia,Cancer
C931,Chronic myelomonocytic leukemia,Cancer
C933,Juvenile myelomonocytic leukemia,Cancer
C937,Other monocytic leukaemia,Cancer
C939,"Monocytic leukemia, unspecified",Cancer
C93X,Monocytic leukemia,Cancer
C94,Other leukemias of specified cell type,Cancer
C940,Acute erythroid leukemia,Cancer
C942,Acute megakaryoblastic leukemia,Cancer
C943,Mast cell leukemia,Cancer
C944,Acute panmyelosis with myelofibrosis,Cancer
C946,"Myelodysplastic disease, not classified",Cancer
C947,Other specified leukaemias,Cancer
C948,Other specified leukemias,Cancer
C94X,Other leukemias of specified cell type,Cancer
C95,Leukemia of unspecified cell type,Cancer
C950,Acute leukemia of unspecified cell type,Cancer
C951,Chronic leukemia of unspecified cell type,Cancer
C957,Other leukaemia of unspecified cell type,Cancer
C959,"Leukemia, unspecified",Cancer
C95X,Leukemia of unspecified cell type,Cancer
C96,"Other and unspecified malignant neoplasms of lymphoid, hematopoietic and related tissue",Cancer
C960,Multifocal and multisystemic (disseminated) Langerhans-cell histiocytosis,Cancer
C962,Malignant mast cell tumor,Cancer
C964,Sarcoma of dendritic cells (accessory cells),Cancer
C965,Multifocal and unisystemic Langerhans-cell histiocytosis,Cancer
C966,Unifocal Langerhans-cell histiocytosis,Cancer
C967,"Other specified malignant neoplasms of lymphoid, haematopoietic and related tissue",Cancer
C968,Histiocytic sarcoma,Cancer
C969,"Malignant neoplasm of lymphoid, hematopoietic and related tissue, unspecified",Cancer
C96X,"Other and unspecified malignant neoplasms of lymphoid, hematopoietic and related tissue",Cancer
C97,Malignant neoplasms of independent (primary) multiple sites,Cancer
C97X,Malignant neoplasms of independent (primary) multiple sites,Cancer
G45,Transient cerebral ischemic attacks and related syndromes,Cerebrovascular Disease
G450,Vertebro-basilar artery syndrome,Cerebrovascular Disease
G451,Carotid artery syndrome (hemispheric),Cerebrovascular Disease
G452,Multiple and bilateral precerebral artery syndromes,Cerebrovascular Disease
G453,Amaurosis fugax,Cerebrovascular Disease
G454,Transient global amnesia,Cerebrovascular Disease
G458,Other transient cerebral ischemic attacks and related syndromes,Cerebrovascular Disease
G459,"Transient cerebral ischemic attack, unspecified",Cerebrovascular Disease
G45X,Transient cerebral ischemic attacks and related syndromes,Cerebrovascular Disease
G46,Vascular syndromes of brain in cerebrovascular diseases,Cerebrovascular Disease
G460,Middle cerebral artery syndrome,Cerebrovascular Disease
G461,Anterior cerebral artery syndrome,Cerebrovascular Disease
G462,Posterior cerebral artery syndrome,Cerebrovascular Disease
G463,Brain stem stroke syndrome,Cerebrovascular Disease
G464,Cerebellar stroke syndrome,Cerebrovascular Disease
G465,Pure motor lacunar syndrome,Cerebrovascular Disease
G466,Pure sensory lacunar syndrome,Cerebrovascular Disease
G467,Other lacunar syndromes,Cerebrovascular Disease
G468,Other vascular syndromes of brain in cerebrovascular diseases,Cerebrovascular Disease
G46X,Vascular syndromes of brain in cerebrovascular diseases,Cerebrovascular Disease
H340,Transient retinal artery occlusion,Cerebrovascular Disease
I60,Nontraumatic subarachnoid hemorrhage,Cerebrovascular Disease
I600,Nontraumatic subarachnoid hemorrhage from carotid siphon and bifurcation,Cerebrovascular Disease
I601,Nontraumatic subarachnoid hemorrhage from middle cerebral artery,Cerebrovascular Disease
I602,Nontraumatic subarachnoid hemorrhage from anterior communicating artery,Cerebrovascular Disease
I603,Nontraumatic subarachnoid hemorrhage from posterior communicating artery,Cerebrovascular Disease
I604,Nontraumatic subarachnoid hemorrhage from basilar artery,Cerebrovascular Disease
I605,Nontraumatic subarachnoid hemorrhage from vertebral artery,Cerebrovascular Disease
I606,Nontraumatic subarachnoid hemorrhage from other intracranial arteries,Cerebrovascular Disease
I607,Nontraumatic subarachnoid hemorrhage from unspecified intracranial artery,Cerebrovascular Disease
I608,Other nontraumatic subarachnoid hemorrhage,Cerebrovascular Disease
I609,"Nontraumatic subarachnoid hemorrhage, unspecified",Cerebrovascular Disease
I60X,Nontraumatic subarachnoid hemorrhage,Cerebrovascular Disease
I61,Nontraumatic intracerebral hemorrhage,Cerebrovascular Disease
I610,"Nontraumatic intracerebral hemorrhage in hemisphere, subcortical",Cerebrovascular Disease
I611,"Nontraumatic intracerebral hemorrhage in hemisphere, cortical",Cerebrovascular Disease
I612,"Nontraumatic intracerebral hemorrhage in hemisphere, unspecified",Cerebrovascular Disease
I613,Nontraumatic intracerebral hemorrhage in brain stem,Cerebrovascular Disease
I614,Nontraumatic intracerebral hemorrhage in cerebellum,Cerebrovascular Disease
I615,"Nontraumatic intracerebral hemorrhage, intraventricular",Cerebrovascular Disease
I616,"Nontraumatic intracerebral hemorrhage, multiple localized",Cerebrovascular Disease
I618,Other nontraumatic intracerebral hemorrhage,Cerebrovascular Disease
I619,"Nontraumatic intracerebral hemorrhage, unspecified",Cerebrovascular Disease
I61X,Nontraumatic intracerebral hemorrhage,Cerebrovascular Disease
I62,Other and unspecified nontraumatic intracranial hemorrhage,Cerebrovascular Disease
I620,Nontraumatic subdural hemorrhage,Cerebrovascular Disease
I621,Nontraumatic extradural hemorrhage,Cerebrovascular Disease
I629,"Nontraumatic intracranial hemorrhage, unspecified",Cerebrovascular Disease
I62X,Other and unspecified nontraumatic intracranial hemorrhage,Cerebrovascular Disease
I63,Cerebral infarction,Cerebrovascular Disease
I630,Cerebral infarction due to thrombosis of precerebral arteries,Cerebrovascular Disease
I631,Cerebral infarction due to embolism of precerebral arteries,Cerebrovascular Disease
I632,Cerebral infarction due to unspecified occlusion or stenosis of precerebral arteries,Cerebrovascular Disease
I633,Cerebral infarction due to thrombosis of cerebral arteries,Cerebrovascular Disease
I634,Cerebral infarction due to embolism of cerebral arteries,Cerebrovascular Disease
I635,Cerebral infarction due to unspecified occlusion or stenosis of cerebral arteries,Cerebrovascular Disease
I636,"Cerebral infarction due to cerebral venous thrombosis, nonpyogenic",Cerebrovascular Disease
I638,Other cerebral infarction,Cerebrovascular Disease
I639,"Cerebral infarction, unspecified",Cerebrovascular Disease
I63X,Cerebral infarction,Cerebrovascular Disease
I64,"Stroke, not specified as haemorrhage or infarction",Cerebrovascular Disease
I64X,"Stroke, not specified as haemorrhage or infarction",Cerebrovascular Disease
I65,"Occlusion and stenosis of precerebral arteries, not resulting in cerebral infarction",Cerebrovascular Disease
I650,Occlusion and stenosis of vertebral artery,Cerebrovascular Disease
I651,Occlusion and stenosis of basilar artery,Cerebrovascular Disease
I652,Occlusion and stenosis of carotid artery,Cerebrovascular Disease
I653,Occlusion and stenosis of multiple and bilateral precerebral arteries,Cerebrovascular Disease
I658,Occlusion and stenosis of other precerebral arteries,Cerebrovascular Disease
I659,Occlusion and stenosis of unspecified precerebral artery,Cerebrovascular Disease
I65X,"Occlusion and stenosis of precerebral arteries, not resulting in cerebral infarction",Cerebrovascular Disease
I66,"Occlusion and stenosis of cerebral arteries, not resulting in cerebral infarction",Cerebrovascular Disease
I660,Occlusion and stenosis of middle cerebral artery,Cerebrovascular Disease
I661,Occlusion and stenosis of anterior cerebral artery,Cerebrovascular Disease
I662,Occlusion and stenosis of posterior cerebral artery,Cerebrovascular Disease
I663,Occlusion and stenosis of cerebellar arteries,Cerebrovascular Disease
I664,Occlusion and stenosis of multiple and bilateral cerebral arteries,Cerebrovascular Disease
I668,Occlusion and stenosis of other cerebral arteries,Cerebrovascular Disease
I669,Occlusion and stenosis of unspecified cerebral artery,Cerebrovascular Disease
I66X,"Occlusion and stenosis of cerebral arteries, not resulting in cerebral infarction",Cerebrovascular Disease
I67,Other cerebrovascular diseases,Cerebrovascular Disease
I670,"Dissection of cerebral arteries, nonruptured",Cerebrovascular Disease
I671,"Cerebral aneurysm, nonruptured",Cerebrovascular Disease
I672,Cerebral atherosclerosis,Cerebrovascular Disease
I673,Progressive vascular leukoencephalopathy,Cerebrovascular Disease
I674,Hypertensive encephalopathy,Cerebrovascular Disease
I675,Moyamoya disease,Cerebrovascular Disease
I676,Nonpyogenic thrombosis of intracranial venous system,Cerebrovascular Disease
I677,"Cerebral arteritis, not elsewhere classified",Cerebrovascular Disease
I678,Other specified cerebrovascular diseases,Cerebrovascular Disease
I679,"Cerebrovascular disease, unspecified",Cerebrovascular Disease
I67X,Other cerebrovascular diseases,Cerebrovascular Disease
I68,Cerebrovascular disorders in diseases classified elsewhere,Cerebrovascular Disease
I680,Cerebral amyloid angiopathy,Cerebrovascular Disease
I681,Cerebral arteritis in infectious and parasitic diseases classified elsewhere,Cerebrovascular Disease
I682,Cerebral arteritis in other diseases classified elsewhere,Cerebrovascular Disease
I688,Other cerebrovascular disorders in diseases classified elsewhere,Cerebrovascular Disease
I68X,Cerebrovascular disorders in diseases classified elsewhere,Cerebrovascular Disease
I69,Sequelae of cerebrovascular disease,Cerebrovascular Disease
I690,Sequelae of nontraumatic subarachnoid hemorrhage,Cerebrovascular Disease
I691,Sequelae of nontraumatic intracerebral hemorrhage,Cerebrovascular Disease
I692,Sequelae of other nontraumatic intracranial hemorrhage,Cerebrovascular Disease
I693,Sequelae of cerebral infarction,Cerebrovascular Disease
I694,"Sequelae of stroke, not specified as haemorrhage or infarction",Cerebrovascular Disease
I698,Sequelae of other cerebrovascular diseases,Cerebrovascular Disease
I699,Sequelae of unspecified cerebrovascular diseases,Cerebrovascular Disease
I69X,Sequelae of cerebrovascular disease,Cerebrovascular Disease
I278,Other specified pulmonary heart diseases,Chronic pulmonary Disease
I279,"Pulmonary heart disease, unspecified",Chronic pulmonary Disease
J40,"Bronchitis, not specified as acute or chronic",Chronic pulmonary disease
J40X,"Bronchitis, not specified as acute or chronic",Chronic pulmonary disease
J41,Simple and mucopurulent chronic bronchitis,Chronic pulmonary disease
J410,Simple chronic bronchitis,Chronic pulmonary disease
J411,Mucopurulent chronic bronchitis,Chronic pulmonary disease
J418,Mixed simple and mucopurulent chronic bronchitis,Chronic pulmonary disease
J41X,Simple and mucopurulent chronic bronchitis,Chronic pulmonary disease
J42,Unspecified chronic bronchitis,Chronic pulmonary disease
J42X,Unspecified chronic bronchitis,Chronic pulmonary disease
J43,Emphysema,Chronic pulmonary disease
J430,Unilateral pulmonary emphysema [MacLeod's syndrome],Chronic pulmonary disease
J431,Panlobular emphysema,Chronic pulmonary disease
J432,Centrilobular emphysema,Chronic pulmonary disease
J438,Other emphysema,Chronic pulmonary disease
J439,"Emphysema, unspecified",Chronic pulmonary disease
J43X,Emphysema,Chronic pulmonary disease
J44,Other chronic obstructive pulmonary disease,Chronic pulmonary disease
J440,Chronic obstructive pulmonary disease with acute lower respiratory infection,Chronic pulmonary disease
J441,Chronic obstructive pulmonary disease with (acute) exacerbation,Chronic pulmonary disease
J449,"Chronic obstructive pulmonary disease, unspecified",Chronic pulmonary disease
J44X,Other chronic obstructive pulmonary disease,Chronic pulmonary disease
J45,Asthma,Chronic pulmonary disease
J450,Predominantly allergic asthma,Chronic pulmonary disease
J451,Nonallergic asthma,Chronic pulmonary disease
J452,Mild intermittent asthma,Chronic pulmonary disease
J453,Mild persistent asthma,Chronic pulmonary disease
J454,Moderate persistent asthma,Chronic pulmonary disease
J455,Severe persistent asthma,Chronic pulmonary disease
J458,Mixed asthma,Chronic pulmonary disease
J459,Other and unspecified asthma,Chronic pulmonary disease
J45X,Asthma,Chronic pulmonary disease
J46,Status asthmaticus,Chronic pulmonary disease
J46X,Status asthmaticus,Chronic pulmonary disease
J47,Bronchiectasis,Chronic pulmonary disease
J470,Bronchiectasis with acute lower respiratory infection,Chronic pulmonary disease
J471,Bronchiectasis with (acute) exacerbation,Chronic pulmonary disease
J479,"Bronchiectasis, uncomplicated",Chronic pulmonary disease
J47X,Bronchiectasis,Chronic pulmonary disease
J60,Coalworker's pneumoconiosis,Chronic pulmonary disease
J60X,Coalworker's pneumoconiosis,Chronic pulmonary disease
J61,Pneumoconiosis due to asbestos and other mineral fibers,Chronic pulmonary disease
J61X,Pneumoconiosis due to asbestos and other mineral fibers,Chronic pulmonary disease
J62,Pneumoconiosis due to dust containing silica,Chronic pulmonary disease
J620,Pneumoconiosis due to talc dust,Chronic pulmonary disease
J628,Pneumoconiosis due to other dust containing silica,Chronic pulmonary disease
J63,Pneumoconiosis due to other inorganic dusts,Chronic pulmonary disease
J630,Aluminosis (of lung),Chronic pulmonary disease
J631,Bauxite fibrosis (of lung),Chronic pulmonary disease
J632,Berylliosis,Chronic pulmonary disease
J633,Graphite fibrosis (of lung),Chronic pulmonary disease
J634,Siderosis,Chronic pulmonary disease
J635,Stannosis,Chronic pulmonary disease
J636,Pneumoconiosis due to other specified inorganic dusts,Chronic pulmonary disease
J638,Pneumoconiosis due to specified inorganic dusts,Chronic pulmonary disease
J63X,Pneumoconiosis due to other inorganic dusts,Chronic pulmonary disease
J64,Unspecified pneumoconiosis,Chronic pulmonary disease
J64X,Unspecified pneumoconiosis,Chronic pulmonary disease
J65,Pneumoconiosis associated with tuberculosis,Chronic pulmonary disease
J65X,Pneumoconiosis associated with tuberculosis,Chronic pulmonary disease
J66,Airway disease due to specific organic dust,Chronic pulmonary disease
J660,Byssinosis,Chronic pulmonary disease
J661,Flax-dressers' disease,Chronic pulmonary disease
J662,Cannabinosis,Chronic pulmonary disease
J668,Airway disease due to other specific organic dusts,Chronic pulmonary disease
J66X,Cannabinosis,Chronic pulmonary disease
J67,Hypersensitivity pneumonitis due to organic dust,Chronic pulmonary disease
J670,Farmer's lung,Chronic pulmonary disease
J671,Bagassosis,Chronic pulmonary disease
J672,Bird fancier's lung,Chronic pulmonary disease
J673,Suberosis,Chronic pulmonary disease
J674,Maltworker's lung,Chronic pulmonary disease
J675,Mushroom-worker's lung,Chronic pulmonary disease
J676,Maple-bark-stripper's lung,Chronic pulmonary disease
J677,Air conditioner and humidifier lung,Chronic pulmonary disease
J678,Hypersensitivity pneumonitis due to other organic dusts,Chronic pulmonary disease
J679,Hypersensitivity pneumonitis due to unspecified organic dust,Chronic pulmonary disease
J67X,Hypersensitivity pneumonitis due to organic dust,Chronic pulmonary disease
J684,"Chronic respiratory conditions due to chemicals, gases, fumes and vapors",Chronic pulmonary disease
J701,Chronic and other pulmonary manifestations due to radiation,Chronic pulmonary disease
J703,Chronic drug-induced interstitial lung disorders,Chronic pulmonary disease
I099,"Rheumatic heart disease, unspecified",Congestive Heart Failure
I110,Hypertensive heart disease with heart failure,Congestive Heart Failure
I110,Hypertensive heart disease with heart failure,Congestive Heart Failure
I130,Hypertensive heart and chronic kidney disease with heart failure and stage 1 through stage 4 chronic kidney,Congestive Heart Failure
I132,"Hypertensive heart and chronic kidney disease with heart failure and with stage 5 chronic kidney disease, or",Congestive Heart Failure
I255,Ischemic cardiomyopathy,Congestive Heart Failure
I420,Dilated cardiomyopathy,Congestive Heart Failure
I425,Other restrictive cardiomyopathy,Congestive Heart Failure
I426,Alcoholic cardiomyopathy,Congestive Heart Failure
I427,Cardiomyopathy due to drug and external agent,Congestive Heart Failure
I428,Other cardiomyopathies,Congestive Heart Failure
I429,"Cardiomyopathy, unspecified",Congestive Heart Failure
I43,Cardiomyopathy in diseases classified elsewhere,Congestive Heart Failure
I430,Cardiomyopathy in infectious and parasitic diseases classified elsewhere,Congestive Heart Failure
I431,Cardiomyopathy in metabolic diseases,Congestive Heart Failure
I432,Cardiomyopathy in nutritional diseases,Congestive Heart Failure
I438,Cardiomyopathy in other diseases classified elsewhere,Congestive Heart Failure
I43X,Cardiomyopathy in diseases classified elsewhere,Congestive Heart Failure
I50,Heart failure,Congestive Heart Failure
I500,Congestive heart failure,Congestive Heart Failure
I501,Left ventricular failure,Congestive Heart Failure
I502,Systolic (congestive) heart failure,Congestive Heart Failure
I503,Diastolic (congestive) heart failure,Congestive Heart Failure
I504,Combined systolic (congestive) and diastolic (congestive) heart failure,Congestive Heart Failure
I509,"Heart failure, unspecified",Congestive Heart Failure
I50X,Congestive heart failure,Congestive Heart Failure
P290,Neonatal cardiac failure,Congestive Heart Failure
F00,Dementia in Alzheimer disease,Dementia
F000,Dementia in Alzheimer disease with early onset,Dementia
F001,Dementia in Alzheimer disease with late onset,Dementia
F002,Dementia in Alzheimer disease atypical or mixed type,Dementia
F009,Dementia in Alzheimer disease unspecified,Dementia
F00X,Dementia in Alzheimer disease,Dementia
F01,Vascular dementia,Dementia
F010,Vascular dementia of acute onset,Dementia
F011,Multi-infarct dementia,Dementia
F012,Subcortical dementia,Dementia
F013,Mixed cortical and subcorctical vascular dementia,Dementia
F018,Other vascular dementia,Dementia
F019,Vascular dementia unspecified,Dementia
F01X,Vascular dementia,Dementia
F02,Dementia in other diseases classified elsewhere,Dementia
F020,Dementia in Pick disease,Dementia
F021,Dementia in Creutzfeld-Jakob disease,Dementia
F022,Dementia in Huntington disease,Dementia
F023,Dementia in Parkinson disease,Dementia
F024,Dementia in HIV disease,Dementia
F028,Dementia in other diseases classified elsewhere,Dementia
F02X,Dementia in other diseases classified elsewhere,Dementia
F03,Unspecified dementia,Dementia
F039,Unspecified dementia,Dementia
F03X,Unspecified dementia,Dementia
F051,Delirium superimposed on dementia,Dementia
G30,Alzheimer's disease,Dementia
G300,Alzheimer's disease with early onset,Dementia
G301,Alzheimer's disease with late onset,Dementia
G308,Other Alzheimer's disease,Dementia
G309,"Alzheimer's disease, unspecified",Dementia
G30X,Alzheimer's disease,Dementia
G311,"Senile degeneration of brain, not elsewhere classified",Dementia
E102,Type 1 diabetes mellitus with kidney complications,Diabetes with chronic complication
E103,Type 1 diabetes mellitus with ophthalmic complications,Diabetes with chronic complication
E104,Type 1 diabetes mellitus with neurological complications,Diabetes with chronic complication
E105,Type 1 diabetes mellitus with circulatory complications,Diabetes with chronic complication
E107,Type 1 diabetes mellitus with multiple complications,Diabetes with chronic complication
E112,Type 2 diabetes mellitus with kidney complications,Diabetes with chronic complication
E113,Type 2 diabetes mellitus with ophthalmic complications,Diabetes with chronic complication
E114,Type 2 diabetes mellitus with neurological complications,Diabetes with chronic complication
E115,Type 2 diabetes mellitus with circulatory complications,Diabetes with chronic complication
E117,Type 2 diabetes mellitus with multiple complications,Diabetes with chronic complication
E122,Malnutrition-related diabetes mellitus with renal complications,Diabetes with chronic complication
E123,Malnutrition-related diabetes mellitus with  opthalmic complications,Diabetes with chronic complication
E124,Malnutrition-related diabetes mellitus with neurological complications,Diabetes with chronic complication
E125,Malnutrition-related diabetes mellitus with  peripheral circulatory complications,Diabetes with chronic complication
E127,Malnutrition-related diabetes mellitus with multiple complications,Diabetes with chronic complication
E132,Other specified diabetes mellitus with kidney complications,Diabetes with chronic complication
E133,Other specified diabetes mellitus with ophthalmic complications,Diabetes with chronic complication
E134,Other specified diabetes mellitus with neurological complications,Diabetes with chronic complication
E135,Other specified diabetes mellitus with circulatory complications,Diabetes with chronic complication
E137,Other specified diabetes mellitus with multiple complications,Diabetes with chronic complication
E142,Unspecified diabetes mellitus with kideny complications,Diabetes with chronic complication
E143,Unspecified diabetes mellitus with ophthalmic complications,Diabetes with chronic complication
E144,Unspecified diabetes mellitus with neurological complications,Diabetes with chronic complication
E145,Unspecified diabetes mellitus with circulatory complications,Diabetes with chronic complication
E147,Unspecified diabetes mellitus with multiple complications,Diabetes with chronic complication
E100,Type 1 diabetes mellitus with coma,Diabetes without chronic complication
E101,Type 1 diabetes mellitus with ketoacidosis,Diabetes without chronic complication
E106,Type 1 diabetes mellitus with other specified complications,Diabetes without chronic complication
E108,Type 1 diabetes mellitus with unspecified complications,Diabetes without chronic complication
E109,Type 1 diabetes mellitus without complications,Diabetes without chronic complication
E110,Type 2 diabetes mellitus with hyperosmolarity,Diabetes without chronic complication
E111,Type 2 diabetes mellitus with ketoacidosis,Diabetes without chronic complication
E116,Type 2 diabetes mellitus with other specified complications,Diabetes without chronic complication
E118,Type 2 diabetes mellitus with unspecified complications,Diabetes without chronic complication
E119,Type 2 diabetes mellitus without complications,Diabetes without chronic complication
E120,Malnutrition-related diabetes mellitus with coma,Diabetes without chronic complication
E121,Malnutrition-related diabetes mellitus with ketoacidosis,Diabetes without chronic complication
E126, Malnutrition-related diabetes mellitus with other specified complications,Diabetes without chronic complication
E128, Malnutrition-related diabetes mellitus with unspecified complications,Diabetes without chronic complication
E129,Malnutrition-related diabetes mellitus without complications,Diabetes without chronic complication
E130,Other specified diabetes mellitus with hyperosmolarity,Diabetes without chronic complication
E131,Other specified diabetes mellitus with ketoacidosis,Diabetes without chronic complication
E136,Other specified diabetes mellitus with other specified complications,Diabetes without chronic complication
E138,Other specified diabetes mellitus with unspecified complications,Diabetes without chronic complication
E139,Other specified diabetes mellitus without complications,Diabetes without chronic complication
E140,Unspecified diabetes mellitus with hyperosmolarity,Diabetes without chronic complication
E141,Unspecified diabetes ellitus with ketoacidosis,Diabetes without chronic complication
E146,Unspecified diabetes mellitus with other specified complications,Diabetes without chronic complication
E148,Unspecified diabetes mellitus with unspecified complications,Diabetes without chronic complication
E149,Unspecified diabetes mellitus without complications,Diabetes without chronic complication
G041,Tropical spastic paraplegia,Hemiplegia or paraplegia
G114,Hereditary spastic paraplegia,Hemiplegia or paraplegia
G114,Hereditary spastic paraplegia,Hemiplegia or paraplegia
G801,Spastic diplegic cerebral palsy,Hemiplegia or paraplegia
G802,Spastic hemiplegic cerebral palsy,Hemiplegia or paraplegia
G81,Hemiplegia and hemiparesis,Hemiplegia or paraplegia
G810,Flaccid hemiplegia,Hemiplegia or paraplegia
G811,Spastic hemiplegia,Hemiplegia or paraplegia
G819,"Hemiplegia, unspecified",Hemiplegia or paraplegia
G81X,Hemiplegia and hemiparesis,Hemiplegia or paraplegia
G82,Paraplegia (paraparesis) and quadriplegia (quadriparesis),Hemiplegia or paraplegia
G820,Flaccid paraplegia,Hemiplegia or paraplegia
G821,Spastic paraplegia,Hemiplegia or paraplegia
G822,Paraplegia,Hemiplegia or paraplegia
G823,Flaccid tetraplegia,Hemiplegia or paraplegia
G824,Spastic tetraplegia,Hemiplegia or paraplegia
G825,Quadriplegia,Hemiplegia or paraplegia
G82X,Paraplegia (paraparesis) and quadriplegia (quadriparesis),Hemiplegia or paraplegia
G830,Diplegia of upper limbs,Hemiplegia or paraplegia
G831,Monoplegia of lower limb,Hemiplegia or paraplegia
G832,Monoplegia of upper limb,Hemiplegia or paraplegia
G833,"Monoplegia, unspecified",Hemiplegia or paraplegia
G834,Cauda equina syndrome,Hemiplegia or paraplegia
G839,"Paralytic syndrome, unspecified",Hemiplegia or paraplegia
B20,Human immunodeficiency virus [HIV] disease,HIV
B200,HIV disease resulting in mycobacterial infection,HIV
B201,HIV disease resulting in other bacterial infections,HIV
B202,HIV disease resulting in cytomegaloviral disease,HIV
B203,HIV disease resulting in other viral infections,HIV
B204,HIV disease resulting in candidiasis,HIV
B205,HIV disease resulting in other mycoses,HIV
B206,HIV disease resulting in Pneumocystis jirovecii pneumonia,HIV
B207,HIV disease resulting in multiple infections,HIV
B208,HIV disease resulting in other infectious and parasitic diseases,HIV
B209,HIV disease resulting in unspecified infectious or parasitic disease,HIV
B20X,Human immunodeficiency virus [HIV] disease,HIV
B21,Human immunodeficiency virus [HIV] disease resulting in malignant neoplasms,HIV
B210,HIV disease resulting in Kaposi sarcoma,HIV
B211,HIV disease resulting in Burkitt lymphoma,HIV
B212,HIV disease resulting in other types of non-Hodgkin lymphoma,HIV
B213,"HIV disease resulting in other malignant neoplasms of lymphoid, haematopoietic and related tissue",HIV
B217,HIV disease resulting in multiple malignant neoplasms,HIV
B218,HIV disease resulting in other malignant neoplasms,HIV
B219,HIV disease resulting in unspecified malignant neoplasm,HIV
B21X,Human immunodeficiency virus [HIV] disease resulting in malignant neoplasms,HIV
B22,Human immunodeficiency virus [HIV] disease resulting in other specified diseases,HIV
B220,HIV disease resulting in encephalopathy,HIV
B221,HIV disease resulting in lymphoid interstitial pneumonitis,HIV
B222,HIV disease resulting in wasting syndrome,HIV
B227,HIV disease resulting in multiple diseases classified elsewhere,HIV
B22X,Human immunodeficiency virus [HIV] disease resulting in other specified diseases,HIV
B24,Unspecified human immunodeficiency virus [HIV] disease,HIV
B24X,Unspecified human immunodeficiency virus [HIV] disease,HIV
C77,Secondary and unspecified malignant neoplasm of lymph nodes,Metastatic Cancer
C770,"Secondary and unspecified malignant neoplasm of lymph nodes of head, face and neck",Metastatic Cancer
C771,Secondary and unspecified malignant neoplasm of intrathoracic lymph nodes,Metastatic Cancer
C772,Secondary and unspecified malignant neoplasm of intra-abdominal lymph nodes,Metastatic Cancer
C773,Secondary and unspecified malignant neoplasm of axilla and upper limb lymph nodes,Metastatic Cancer
C774,Secondary and unspecified malignant neoplasm of inguinal and lower limb lymph nodes,Metastatic Cancer
C775,Secondary and unspecified malignant neoplasm of intrapelvic lymph nodes,Metastatic Cancer
C778,Secondary and unspecified malignant neoplasm of lymph nodes of multiple regions,Metastatic Cancer
C779,"Secondary and unspecified malignant neoplasm of lymph node, unspecified",Metastatic Cancer
C77X,Secondary and unspecified malignant neoplasm of lymph nodes,Metastatic Cancer
C78,Secondary malignant neoplasm of respiratory and digestive organs,Metastatic Cancer
C780,Secondary malignant neoplasm of lung,Metastatic Cancer
C781,Secondary malignant neoplasm of mediastinum,Metastatic Cancer
C782,Secondary malignant neoplasm of pleura,Metastatic Cancer
C783,Secondary malignant neoplasm of other and unspecified respiratory organs,Metastatic Cancer
C784,Secondary malignant neoplasm of small intestine,Metastatic Cancer
C785,Secondary malignant neoplasm of large intestine and rectum,Metastatic Cancer
C786,Secondary malignant neoplasm of retroperitoneum and peritoneum,Metastatic Cancer
C787,Secondary malignant neoplasm of liver and intrahepatic bile duct,Metastatic Cancer
C788,Secondary malignant neoplasm of other and unspecified digestive organs,Metastatic Cancer
C78X,Secondary malignant neoplasm of lung,Metastatic Cancer
C79,Secondary malignant neoplasm of other and unspecified sites,Metastatic Cancer
C790,Secondary malignant neoplasm of kidney and renal pelvis,Metastatic Cancer
C791,Secondary malignant neoplasm of bladder and other and unspecified urinary organs,Metastatic Cancer
C792,Secondary malignant neoplasm of skin,Metastatic Cancer
C793,Secondary malignant neoplasm of brain and cerebral meninges,Metastatic Cancer
C794,Secondary malignant neoplasm of other and unspecified parts of nervous system,Metastatic Cancer
C795,Secondary malignant neoplasm of bone and bone marrow,Metastatic Cancer
C796,Secondary malignant neoplasm of ovary,Metastatic Cancer
C797,Secondary malignant neoplasm of adrenal gland,Metastatic Cancer
C798,Secondary malignant neoplasm of other specified sites,Metastatic Cancer
C799,Secondary malignant neoplasm of unspecified site,Metastatic Cancer
C79X,Secondary malignant neoplasm of other and unspecified sites,Metastatic Cancer
C80,Malignant neoplasm without specification of site,Metastatic Cancer
C800,"Disseminated malignant neoplasm, unspecified",Metastatic Cancer
C801,"Malignant (primary) neoplasm, unspecified",Metastatic Cancer
C802,Malignant neoplasm associated with transplanted organ,Metastatic Cancer
C80X,Malignant neoplasm without specification of site,Metastatic Cancer
B18,Chronic viral hepatitis,Mild Liver Disease
B180,Chronic viral hepatitis B with delta-agent,Mild Liver Disease
B181,Chronic viral hepatitis B without delta-agent,Mild Liver Disease
B182,Chronic viral hepatitis C,Mild Liver Disease
B188,Other chronic viral hepatitis,Mild Liver Disease
B189,"Chronic viral hepatitis, unspecified",Mild Liver Disease
B18X,Chronic viral hepatitis C,Mild Liver Disease
K70,Alcoholic liver disease,Mild Liver Disease
K700,Alcoholic fatty liver,Mild Liver Disease
K701,Alcoholic fatty liver,Mild Liver Disease
K701,Alcoholic hepatitis,Mild Liver Disease
K702,Alcoholic fibrosis and sclerosis of liver,Mild Liver Disease
K703,Alcoholic cirrhosis of liver,Mild Liver Disease
K709,"Alcoholic liver disease, unspecified",Mild Liver Disease
K710,"Alcoholic liver disease, unspecified",Mild Liver Disease
K713,Toxic liver disease with chronic persistent hepatitis,Mild Liver Disease
K714,Toxic liver disease with chronic lobular hepatitis,Mild Liver Disease
K715,Toxic liver disease with chronic active hepatitis,Mild Liver Disease
K717,Toxic liver disease with fibrosis and cirrhosis of liver,Mild Liver Disease
K73,"Chronic hepatitis, not elsewhere classified",Mild Liver Disease
K730,"Chronic persistent hepatitis, not elsewhere classified",Mild Liver Disease
K731,"Chronic lobular hepatitis, not elsewhere classified",Mild Liver Disease
K732,"Chronic active hepatitis, not elsewhere classified",Mild Liver Disease
K738,"Other chronic hepatitis, not elsewhere classified",Mild Liver Disease
K739,"Chronic hepatitis, unspecified",Mild Liver Disease
K73X,"Chronic hepatitis, not elsewhere classified",Mild Liver Disease
K74,Fibrosis and cirrhosis of liver,Mild Liver Disease
K740,Hepatic fibrosis,Mild Liver Disease
K741,Hepatic sclerosis,Mild Liver Disease
K742,Hepatic fibrosis with hepatic sclerosis,Mild Liver Disease
K743,Primary biliary cirrhosis,Mild Liver Disease
K744,Secondary biliary cirrhosis,Mild Liver Disease
K745,"Biliary cirrhosis, unspecified",Mild Liver Disease
K746,Other and unspecified cirrhosis of liver,Mild Liver Disease
K74X,Fibrosis and cirrhosis of liver,Mild Liver Disease
K760,"Fatty (change of) liver, not elsewhere classified",Mild Liver Disease
K762,Central hemorrhagic necrosis of liver,Mild Liver Disease
K763,Infarction of liver,Mild Liver Disease
K764,Peliosis hepatis,Mild Liver Disease
K768,Other specified diseases of liver,Mild Liver Disease
K769,"Liver disease, unspecified",Mild Liver Disease
Z944,Liver transplant status,Mild Liver Disease
I21,ST elevation (STEMI) and non-ST elevation (NSTEMI) myocardial infarction,Myocardial Infarction
I210,ST elevation (STEMI) myocardial infarction of anterior wall,Myocardial Infarction
I211,ST elevation (STEMI) myocardial infarction of inferior wall,Myocardial Infarction
I212,ST elevation (STEMI) myocardial infarction of other sites,Myocardial Infarction
I213,ST elevation (STEMI) myocardial infarction of unspecified site,Myocardial Infarction
I214,Non-ST elevation (NSTEMI) myocardial infarction,Myocardial Infarction
I219,Acute myocardial infarction unspecified,Myocardial Infarction
I21X,Acute myocardial infarction ,Myocardial Infarction
I22,Subsequent ST elevation (STEMI) and non-ST elevation (NSTEMI) myocardial infarction,Myocardial Infarction
I220,Subsequent ST elevation (STEMI) myocardial infarction of anterior wall,Myocardial Infarction
I221,Subsequent ST elevation (STEMI) myocardial infarction of inferior wall,Myocardial Infarction
I222,Subsequent non-ST elevation (NSTEMI) myocardial infarction,Myocardial Infarction
I228,Subsequent ST elevation (STEMI) myocardial infarction of other sites,Myocardial Infarction
I229,Subsequent ST elevation (STEMI) myocardial infarction of unspecified site,Myocardial Infarction
I22X,Subsequent myocardial infarction,Myocardial Infarction
I252,Old myocardial infarction,Myocardial Infarction
K25,Gastric ulcer,Peptic Ulcer Disease
K250,Acute gastric ulcer with hemorrhage,Peptic Ulcer Disease
K251,Acute gastric ulcer with perforation,Peptic Ulcer Disease
K252,Acute gastric ulcer with both hemorrhage and perforation,Peptic Ulcer Disease
K253,Acute gastric ulcer without hemorrhage or perforation,Peptic Ulcer Disease
K254,Chronic or unspecified gastric ulcer with hemorrhage,Peptic Ulcer Disease
K255,Chronic or unspecified gastric ulcer with perforation,Peptic Ulcer Disease
K256,Chronic or unspecified gastric ulcer with both hemorrhage and perforation,Peptic Ulcer Disease
K257,Chronic gastric ulcer without hemorrhage or perforation,Peptic Ulcer Disease
K259,"Gastric ulcer, unspecified as acute or chronic, without hemorrhage or perforation",Peptic Ulcer Disease
K25X,Gastric ulcer,Peptic Ulcer Disease
K26,Duodenal ulcer,Peptic Ulcer Disease
K260,Acute duodenal ulcer with hemorrhage,Peptic Ulcer Disease
K261,Acute duodenal ulcer with perforation,Peptic Ulcer Disease
K262,Acute duodenal ulcer with both hemorrhage and perforation,Peptic Ulcer Disease
K263,Acute duodenal ulcer without hemorrhage or perforation,Peptic Ulcer Disease
K264,Chronic or unspecified duodenal ulcer with hemorrhage,Peptic Ulcer Disease
K265,Chronic or unspecified duodenal ulcer with perforation,Peptic Ulcer Disease
K266,Chronic or unspecified duodenal ulcer with both hemorrhage and perforation,Peptic Ulcer Disease
K267,Chronic duodenal ulcer without hemorrhage or perforation,Peptic Ulcer Disease
K269,"Duodenal ulcer, unspecified as acute or chronic, without hemorrhage or perforation",Peptic Ulcer Disease
K26X,Duodenal ulcer,Peptic Ulcer Disease
K27,"Peptic ulcer, site unspecified",Peptic Ulcer Disease
K270,"Acute peptic ulcer, site unspecified, with hemorrhage",Peptic Ulcer Disease
K271,"Acute peptic ulcer, site unspecified, with perforation",Peptic Ulcer Disease
K272,"Acute peptic ulcer, site unspecified, with both hemorrhage and perforation",Peptic Ulcer Disease
K273,"Acute peptic ulcer, site unspecified, without hemorrhage or perforation",Peptic Ulcer Disease
K274,"Chronic or unspecified peptic ulcer, site unspecified, with hemorrhage",Peptic Ulcer Disease
K275,"Chronic or unspecified peptic ulcer, site unspecified, with perforation",Peptic Ulcer Disease
K276,"Chronic or unspecified peptic ulcer, site unspecified, with both hemorrhage and perforation",Peptic Ulcer Disease
K277,"Chronic peptic ulcer, site unspecified, without hemorrhage or perforation",Peptic Ulcer Disease
K279,"Peptic ulcer, site unspecified, unspecified as acute or chronic, without hemorrhage or perforation",Peptic Ulcer Disease
K27X,"Peptic ulcer, site unspecified",Peptic Ulcer Disease
K28,Gastrojejunal ulcer,Peptic Ulcer Disease
K280,Acute gastrojejunal ulcer with hemorrhage,Peptic Ulcer Disease
K281,Acute gastrojejunal ulcer with perforation,Peptic Ulcer Disease
K282,Acute gastrojejunal ulcer with both hemorrhage and perforation,Peptic Ulcer Disease
K283,Acute gastrojejunal ulcer without hemorrhage or perforation,Peptic Ulcer Disease
K284,Chronic or unspecified gastrojejunal ulcer with hemorrhage,Peptic Ulcer Disease
K285,Chronic or unspecified gastrojejunal ulcer with perforation,Peptic Ulcer Disease
K286,Chronic or unspecified gastrojejunal ulcer with both hemorrhage and perforation,Peptic Ulcer Disease
K287,Chronic gastrojejunal ulcer without hemorrhage or perforation,Peptic Ulcer Disease
K289,"Gastrojejunal ulcer, unspecified as acute or chronic, without hemorrhage or perforation",Peptic Ulcer Disease
K28X,Gastrojejunal ulcer,Peptic Ulcer Disease
I70,Atherosclerosis,Peripheral Vascular Disease
I700,Atherosclerosis of aorta,Peripheral Vascular Disease
I701,Atherosclerosis of renal artery,Peripheral Vascular Disease
I702,Atherosclerosis of native arteries of the extremities,Peripheral Vascular Disease
I703,Atherosclerosis of unspecified type of bypass graft(s) of the extremities,Peripheral Vascular Disease
I704,Atherosclerosis of autologous vein bypass graft(s) of the extremities,Peripheral Vascular Disease
I705,Atherosclerosis of nonautologous biological bypass graft(s) of the extremities,Peripheral Vascular Disease
I706,Atherosclerosis of nonbiological bypass graft(s) of the extremities,Peripheral Vascular Disease
I707,Atherosclerosis of other type of bypass graft(s) of the extremities,Peripheral Vascular Disease
I708,Atherosclerosis of other arteries,Peripheral Vascular Disease
I709,Other and unspecified atherosclerosis,Peripheral Vascular Disease
I70X,Atherosclerosis,Peripheral Vascular Disease
I71,Aortic aneurysm and dissection,Peripheral Vascular Disease
I710,Dissection of aorta,Peripheral Vascular Disease
I711,"Thoracic aortic aneurysm, ruptured",Peripheral Vascular Disease
I712,"Thoracic aortic aneurysm, without rupture",Peripheral Vascular Disease
I713,"Abdominal aortic aneurysm, ruptured",Peripheral Vascular Disease
I714,"Abdominal aortic aneurysm, without rupture",Peripheral Vascular Disease
I715,"Thoracoabdominal aortic aneurysm, ruptured",Peripheral Vascular Disease
I716,"Thoracoabdominal aortic aneurysm, without rupture",Peripheral Vascular Disease
I718,"Aortic aneurysm of unspecified site, ruptured",Peripheral Vascular Disease
I719,"Aortic aneurysm of unspecified site, without rupture",Peripheral Vascular Disease
I71X,Aortic aneurysm and dissection,Peripheral Vascular Disease
I731,Thromboangiitis obliterans [Buerger's disease],Peripheral Vascular Disease
I738,Other specified peripheral vascular diseases,Peripheral Vascular Disease
I739,"Peripheral vascular disease, unspecified",Peripheral Vascular Disease
I771,Stricture of artery,Peripheral Vascular Disease
I790,Aneurysm of aorta in diseases classified elsewhere,Peripheral Vascular Disease
I792,Peripheral angiopathy in diseases classified elsewhere,Peripheral Vascular Disease
K551,Chronic vascular disorders of intestine,Peripheral Vascular Disease
K558,Other vascular disorders of intestine,Peripheral Vascular Disease
K559,"Vascular disorder of intestine, unspecified",Peripheral Vascular Disease
Z958,Presence of other cardiac and vascular implants and grafts,Peripheral Vascular Disease
I120,Hypertensive chronic kidney disease with stage 5 chronic kidney disease or end stage renal disease,Renal Disease
I120,Hypertensive chronic kidney disease with stage 5 chronic kidney disease or end stage renal disease,Renal Disease
I131,Hypertensive heart and chronic kidney disease without heart failure,Renal Disease
N032,Chronic nephritic syndrome with diffuse membranous glomerulonephritis,Renal Disease
N033,Chronic nephritic syndrome with diffuse mesangial proliferative glomerulonephritis,Renal Disease
N034,Chronic nephritic syndrome with diffuse endocapillary proliferative glomerulonephritis,Renal Disease
N035,Chronic nephritic syndrome with diffuse mesangiocapillary glomerulonephritis,Renal Disease
N036,Chronic nephritic syndrome with dense deposit disease,Renal Disease
N037,Chronic nephritic syndrome with diffuse crescentic glomerulonephritis,Renal Disease
N052,Unspecified nephritic syndrome with diffuse membranous glomerulonephritis,Renal Disease
N053,Unspecified nephritic syndrome with diffuse mesangial proliferative glomerulonephritis,Renal Disease
N054,Unspecified nephritic syndrome with diffuse endocapillary proliferative glomerulonephritis,Renal Disease
N055,Unspecified nephritic syndrome with diffuse mesangiocapillary glomerulonephritis,Renal Disease
N056,Unspecified nephritic syndrome with dense deposit disease,Renal Disease
N057,Unspecified nephritic syndrome with diffuse crescentic glomerulonephritis,Renal Disease
N18,Chronic kidney disease (CKD),Renal Disease
N181,"Chronic kidney disease, stage 1",Renal Disease
N182,"Chronic kidney disease, stage 2 (mild)",Renal Disease
N183,"Chronic kidney disease, stage 3 (moderate)",Renal Disease
N184,"Chronic kidney disease, stage 4 (severe)",Renal Disease
N185,"Chronic kidney disease, stage 5",Renal Disease
N186,End stage renal disease,Renal Disease
N189,"Chronic kidney disease, unspecified",Renal Disease
N18X,Chronic kidney disease (CKD),Renal Disease
N19,Unspecified kidney failure,Renal Disease
N19X,Unspecified kidney failure,Renal Disease
N250,Renal osteodystrophy,Renal Disease
Z490,Preparatory care for renal dialysis,Renal Disease
Z491,Extracorporeal dialysis,Renal Disease
Z492,Other dialysis,Renal Disease
Z940,Kidney transplant status,Renal Disease
Z992,Dependence on renal dialysis,Renal Disease
M05,Rheumatoid arthritis with rheumatoid factor,Rheumatic Disease
M050,Felty's syndrome,Rheumatic Disease
M051,Rheumatoid lung disease with rheumatoid arthritis,Rheumatic Disease
M052,Rheumatoid vasculitis with rheumatoid arthritis,Rheumatic Disease
M053,Rheumatoid heart disease with rheumatoid arthritis,Rheumatic Disease
M054,Rheumatoid myopathy with rheumatoid arthritis,Rheumatic Disease
M055,Rheumatoid polyneuropathy with rheumatoid arthritis,Rheumatic Disease
M056,Rheumatoid arthritis with involvement of other organs and systems,Rheumatic Disease
M057,Rheumatoid arthritis with rheumatoid factor without organ or systems involvement,Rheumatic Disease
M058,Other rheumatoid arthritis with rheumatoid factor,Rheumatic Disease
M059,"Rheumatoid arthritis with rheumatoid factor, unspecified",Rheumatic Disease
M05X,Rheumatoid arthritis with rheumatoid factor,Rheumatic Disease
M06,Other rheumatoid arthritis,Rheumatic Disease
M060,Rheumatoid arthritis without rheumatoid factor,Rheumatic Disease
M061,Adult-onset Still's disease,Rheumatic Disease
M062,Rheumatoid bursitis,Rheumatic Disease
M063,Rheumatoid nodule,Rheumatic Disease
M064,Inflammatory polyarthropathy,Rheumatic Disease
M068,Other specified rheumatoid arthritis,Rheumatic Disease
M069,"Rheumatoid arthritis, unspecified",Rheumatic Disease
M06X,Other rheumatoid arthritis,Rheumatic Disease
M315,Giant cell arteritis with polymyalgia rheumatica,Rheumatic Disease
M32,Systemic lupus erythematosus (SLE),Rheumatic Disease
M320,Drug-induced systemic lupus erythematosus,Rheumatic Disease
M321,Systemic lupus erythematosus with organ or system involvement,Rheumatic Disease
M328,Other forms of systemic lupus erythematosus,Rheumatic Disease
M329,"Systemic lupus erythematosus, unspecified",Rheumatic Disease
M32X,Systemic lupus erythematosus (SLE),Rheumatic Disease
M33,Dermatopolymyositis,Rheumatic Disease
M330,Juvenile dermatopolymyositis,Rheumatic Disease
M331,Other dermatopolymyositis,Rheumatic Disease
M332,Polymyositis,Rheumatic Disease
M339,"Dermatopolymyositis, unspecified",Rheumatic Disease
M33X,Dermatopolymyositis,Rheumatic Disease
M34,Systemic sclerosis [scleroderma],Rheumatic Disease
M340,Progressive systemic sclerosis,Rheumatic Disease
M341,CR(E)ST syndrome,Rheumatic Disease
M342,Systemic sclerosis induced by drug and chemical,Rheumatic Disease
M348,Other forms of systemic sclerosis,Rheumatic Disease
M349,"Systemic sclerosis, unspecified",Rheumatic Disease
M34X,Systemic sclerosis [scleroderma],Rheumatic Disease
M351,Other overlap syndromes,Rheumatic Disease
M353,Polymyalgia rheumatica,Rheumatic Disease
M360,Dermato(poly)myositis in neoplastic disease,Rheumatic Disease
I850,Esophageal varices with bleeding,Severe Liver Disease
I859,Oesophageal varices without bleeding,Severe Liver Disease
I864,Gastric varices,Severe Liver Disease
I982,Oesophageal varices without bleeding in diseases classified elsewhere,Severe Liver Disease
K704,Alcoholic hepatic failure,Severe Liver Disease
K711,Toxic liver disease with hepatic necrosis,Severe Liver Disease
K721,Chronic hepatic failure,Severe Liver Disease
K729,"Hepatic failure, unspecified",Severe Liver Disease
K765,Hepatic veno-occlusive disease,Severe Liver Disease
K766,Portal hypertension,Severe Liver Disease
K767,Hepatorenal syndrome,Severe Liver Disease
"""

# COMMAND ----------

# convert to Spark Dataframe
codelist_charlson = (
  spark.createDataFrame(
    pd.DataFrame(pd.read_csv(io.StringIO(codelist_charlson)))
    .fillna('')
    .astype(str)
  )
)

# add/drop required variables
# drop the term provided by csv codelist as we will merge this from ICD10 lookup
codelist_charlson = codelist_charlson\
  .withColumn('terminology', f.lit('ICD10'))\
  .drop('code_desc')

# remove trailing X's, decimal points, dashes, and spaces
codelist_charlson = codelist_charlson\
  .withColumn('code', f.regexp_replace('code', r'X$', ''))\
  .withColumn('code', f.regexp_replace('code', r'[\.\-\s]', ''))

# drop duplicate rows
codelist_charlson = codelist_charlson\
  .distinct()

# COMMAND ----------

# re-label co-morbidities for integration with standard Charlson methodology notebook
codelist_charlson = codelist_charlson\
  .withColumn('name', f.regexp_replace('name', r'X$', ''))\
  .withColumn('name', f.when(f.col('name') == "Cancer", f.lit("canc")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Cerebrovascular Disease", f.lit("cevd")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name').isin("Chronic pulmonary disease","Chronic pulmonary Disease"), f.lit("cpd")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Congestive Heart Failure", f.lit("chf")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Dementia", f.lit("dementia")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Diabetes with chronic complication", f.lit("diabwc")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Diabetes without chronic complication", f.lit("diab")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Hemiplegia or paraplegia", f.lit("hp")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "HIV", f.lit("hiv")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Metastatic Cancer", f.lit("metacanc")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Mild Liver Disease", f.lit("mld")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Myocardial Infarction", f.lit("mi")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Peptic Ulcer Disease", f.lit("pud")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Peripheral Vascular Disease", f.lit("pad")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Renal Disease", f.lit("rend")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Rheumatic Disease", f.lit("rheumd")).otherwise(f.col('name')))\
  .withColumn('name', f.when(f.col('name') == "Severe Liver Disease", f.lit("sld")).otherwise(f.col('name')))


# COMMAND ----------

display(codelist_charlson)

# COMMAND ----------

# MAGIC %md # 3 Check

# COMMAND ----------

# check for duplicates
count_var(codelist_charlson, 'code')

# COMMAND ----------

# check validity of codes against ICD10 lookup table
codelist_charlson = codelist_charlson\
  .join(icd10.withColumn('inICD10', f.lit(1)), on = 'code', how = 'left')\
  .withColumn('inICD10', f.when(f.col('inICD10').isNull(), f.lit(0)).otherwise(f.col('inICD10')))

# freq of invalid codes
tmpt = tab(codelist_charlson, 'inICD10'); print()

# note that invalid codes not excluded from codelist

# COMMAND ----------

# display invalid ICD10 codes
display(codelist_charlson.where(f.col('inICD10') == 0))

# COMMAND ----------

# remove unrequired variables
codelist_charlson = codelist_charlson\
  .drop('inICD10')

# COMMAND ----------

# codelist checks
tmpt = tab(codelist_charlson, 'name'); print()
tmpt = tab(codelist_charlson, 'terminology'); print()
tmpt = tab(codelist_charlson, 'name', 'terminology', var2_unstyled=1); print()
print(codelist_charlson.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# check
display(codelist_charlson)

# COMMAND ----------

# MAGIC %md # 4 Save

# COMMAND ----------

# save
save_table(df=codelist_charlson, out_name=f'{proj}_codelist_charlson', save_previous=True, dbc=dbc)