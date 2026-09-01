clear all
odbc load, exec("SELECT * FROM .ccu052_02_c01_v01_out_combined WHERE CHUNK=1") dsn("databricks") clear
save "\CCU052\chunk1.dta", replace 

clear all
odbc load, exec("SELECT * FROM .ccu052_02_c01_v01_out_combined WHERE CHUNK=2") dsn("databricks") clear
save "\CCU052\chunk2.dta", replace 

clear all
odbc load, exec("SELECT * FROM .ccu052_02_c01_v01_out_combined WHERE CHUNK=3") dsn("databricks") clear
save "\CCU052\chunk3.dta", replace 

clear all
odbc load, exec("SELECT * FROM .ccu052_02_c01_v01_out_combined WHERE CHUNK=4") dsn("databricks") clear
save "\CCU052\chunk4.dta", replace 

clear all
odbc load, exec("SELECT * FROM .ccu052_02_c01_v01_out_combined WHERE CHUNK=5") dsn("databricks") clear
save "\CCU052\chunk5.dta", replace 

clear all
odbc load, exec("SELECT * FROM .ccu052_02_c01_v01_out_combined WHERE CHUNK=6") dsn("databricks") clear
save "\CCU052\chunk6.dta", replace 

clear all
odbc load, exec("SELECT * FROM .ccu052_02_c01_v01_out_combined WHERE CHUNK=7") dsn("databricks") clear
save "\CCU052\chunk7.dta", replace 

clear all
odbc load, exec("SELECT * FROM .ccu052_02_c01_v01_out_combined WHERE CHUNK=8") dsn("databricks") clear
save "\CCU052\chunk8.dta", replace 

clear all
odbc load, exec("SELECT * FROM .ccu052_02_c01_v01_out_combined WHERE CHUNK=9") dsn("databricks") clear
save "\CCU052\chunk9.dta", replace 

clear all
odbc load, exec("SELECT * FROM .ccu052_02_c01_v01_out_combined WHERE CHUNK=10") dsn("databricks") clear
save "\CCU052\chunk10.dta", replace 

***********************************************
clear all
odbc load, exec("SELECT * FROM .ccu052_02_c01_v01_out_outcomes_multirow") dsn("databricks") clear
save "\CCU052\exacerbations.dta", replace 

*copd exacerbations 
use "\CCU052\exacerbations.dta", clear 
gen month_date=month(DATE) 
gen year_date=year(DATE)
gen day_date=1
gen month_year=mdy(month_date, day_date, year_date)
format month_year %td

keep if name=="aecopd_gp" | name=="aecopd_hes" 
sort PERSON_ID month_year name
by  PERSON_ID month_year: gen litn=_n
by PERSON_ID month_year: gen bign=_N
keep if litn==bign
rename bign any_aecopd
recode any_aecopd 3=2 4=2
keep PERSON_ID month_year any_aecopd DATE 
duplicates drop
save "\CCU052\copd_exacerbations_all.dta", replace 

*copd exacerbations- moderate
use "\CCU052\exacerbations.dta", clear 
gen month_date=month(DATE) 
gen year_date=year(DATE)
gen day_date=1
gen month_year=mdy(month_date, day_date, year_date)
format month_year %td

keep if name=="aecopd_gp" 
sort PERSON_ID month_year name
by  PERSON_ID month_year: gen litn=_n
by PERSON_ID month_year: gen bign=_N
keep if litn==bign
rename bign gp_aecopd
recode gp_aecopd 4=2
keep PERSON_ID month_year gp_aecopd DATE 
duplicates drop
save "\CCU052\copd_exacerbations_gp.dta", replace 


*copd exacerbations- severe
use "\CCU052\exacerbations.dta", clear 
gen month_date=month(DATE) 
gen year_date=year(DATE)
gen day_date=1
gen month_year=mdy(month_date, day_date, year_date)
format month_year %td

keep if name=="aecopd_hes" 
sort PERSON_ID month_year name
by  PERSON_ID month_year: gen litn=_n
by PERSON_ID month_year: gen bign=_N
keep if litn==bign
rename bign hes_aecopd
recode hes_aecopd 4=2
keep PERSON_ID month_year hes_aecopd DATE 
duplicates drop
save "\CCU052\copd_exacerbations_hes.dta", replace 

use "\CCU052\copd_exacerbations_all.dta", clear 
drop DATE 
merge 1:1 PERSON_ID month_year using  "\CCU052\copd_exacerbations_gp.dta"
drop DATE 
drop _m
merge 1:1 PERSON_ID month_year using  "\CCU052\copd_exacerbations_hes.dta"
drop _m
drop DATE
recode any_aecopd .=0
recode gp_aecopd .=0
recode hes_aecopd .=0
save "\CCU052\copd_exacerbations.dta", replace 



*asthma  exacerbations 
use "\CCU052\exacerbations.dta", clear 
gen month_date=month(DATE) 
gen year_date=year(DATE)
gen day_date=1
gen month_year=mdy(month_date, day_date, year_date)
format month_year %td

keep if name=="aeasthma_gp" | name=="aeasthma_hes" 
sort PERSON_ID month_year name
by  PERSON_ID month_year: gen litn=_n
by PERSON_ID month_year: gen bign=_N
keep if litn==bign
rename bign any_aeasthma
recode any_aeasthma 3=2 4=2
keep PERSON_ID month_year any_aeasthma DATE 
duplicates drop
save "\CCU052\asthma_exacerbations_all.dta", replace 

*asthma exacerbations- moderate
use "\CCU052\exacerbations.dta", clear 
gen month_date=month(DATE) 
gen year_date=year(DATE)
gen day_date=1
gen month_year=mdy(month_date, day_date, year_date)
format month_year %td

keep if name=="aeasthma_gp" 
sort PERSON_ID month_year name
by  PERSON_ID month_year: gen litn=_n
by PERSON_ID month_year: gen bign=_N
keep if litn==bign
rename bign gp_aeasthma
recode gp_aeasthma 4=2
keep PERSON_ID month_year gp_aeasthma DATE 
duplicates drop
save "\CCU052\asthma_exacerbations_gp.dta", replace 


*asthma exacerbations- severe
use "\CCU052\exacerbations.dta", clear 
gen month_date=month(DATE) 
gen year_date=year(DATE)
gen day_date=1
gen month_year=mdy(month_date, day_date, year_date)
format month_year %td

keep if name=="aeasthma_hes" 
sort PERSON_ID month_year name
by  PERSON_ID month_year: gen litn=_n
by PERSON_ID month_year: gen bign=_N
keep if litn==bign
rename bign hes_aeasthma
recode hes_aeasthma 4=2
keep PERSON_ID month_year hes_aeasthma DATE 
duplicates drop
save "\CCU052\asthma_exacerbations_hes.dta", replace 

use "\CCU052\asthma_exacerbations_all.dta", clear 
drop DATE 
merge 1:1 PERSON_ID month_year using  "\CCU052\asthma_exacerbations_gp.dta"
drop DATE 
drop _m
merge 1:1 PERSON_ID month_year using  "\CCU052\asthma_exacerbations_hes.dta"
drop _m
drop DATE
recode any_aeasthma .=0
recode gp_aeasthma .=0
recode hes_aeasthma .=0
save "\CCU052\asthma_exacerbations.dta", replace 




*ild  exacerbations - hes only
use "\CCU052\exacerbations.dta", clear 
gen month_date=month(DATE) 
gen year_date=year(DATE)
gen day_date=1
gen month_year=mdy(month_date, day_date, year_date)
format month_year %td

keep if name=="aeild_hes" 
sort PERSON_ID month_year name
by  PERSON_ID month_year: gen litn=_n
by PERSON_ID month_year: gen bign=_N
keep if litn==bign
rename bign any_aeild
recode any_aeild 3=2 4=2
keep PERSON_ID month_year any_aeild DATE 
duplicates drop
save "\CCU052\ild_exacerbations_all.dta", replace 

