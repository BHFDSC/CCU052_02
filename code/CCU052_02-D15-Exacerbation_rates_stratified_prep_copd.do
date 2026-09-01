*COPD EXACERBATION

****************************************************************
*Numerator
***************************************************************

forvalues i=1/10{
use "\CCU052\chunk`i'.dta", clear
merge 1:m PERSON_ID using "\CCU052\copd_exacerbations.dta"
keep if _m==3 | _m==1
drop _m
keep if region!="" 

*Generate a month/year variable from the date of incident aecopd/aecopd etc- this will be the grouping variable for numberator counts 

gen ethnicity=1 if ETHNIC_CAT=="White"
replace ethnicity=2 if ETHNIC_CAT=="Black or Black British"
replace ethnicity=3 if ETHNIC_CAT=="Asian or Asian British"
replace ethnicity=4 if ETHNIC_CAT=="Mixed"
replace ethnicity=5 if ETHNIC_CAT=="Other"
replace ethnicity=6 if ETHNIC_CAT=="Unknown"
replace ethnicity=6 if ETHNIC_CAT==""
label define lab_ethnic 1"White" 2"Black" 3"Asian" 4"Mixed" 5"Other" 6"Uknown"
label values ethnicity lab_ethnic

*keep those who had copd
keep if cov_hx_out_copd_flag==1 
codebook PERSON_ID 

*generating age. Stratified by baseline age? With the standardised rates does age need to be updated every year/month? Also, what is everyone's age cut off for aecopd-40? use 40 aecopd, 40 for ild
sort month_year
gen age=month_year-DOB
replace age=age/365.25
keep if age>=40

gen age_group=1 if age>=40 & age<50
replace age_group=2 if age>=50 & age<60
replace age_group=3 if age>=60 & age<70
replace age_group=4 if age>=70 

label define lab_age 1"40-50" 2"50-60" 3"60-70" 4"70+" 
label values age_group lab_age 


*stratify by sex, age, ethnicity, IMD
sort month_year SEX  age_group ethnicity IMD_2019_DECILES region
by  month_year SEX  age_group ethnicity IMD_2019_DECILES region: gen litn=_n
by  month_year SEX  age_group ethnicity IMD_2019_DECILES region: gen bign=_N 
keep if litn==bign 
drop litn 
rename bign tot_aecopd_`i'
keep month_year SEX  age_group ethnicity IMD_2019_DECILES region tot_aecopd_`i'  
save "\CCU052\aecopd_numerator_chunk`i'", replace 
}

*Merge all together and sum counts from each count for each month
use "\CCU052\aecopd_numerator_chunk1", clear 
forvalues i=2/10{
	merge 1:1 month_year SEX  age_group ethnicity IMD_2019_DECILES region using "\CCU052\aecopd_numerator_chunk`i'"
	drop _m
}

sort  month_year SEX  age_group ethnicity IMD_2019_DECILES region
forvalues i=1/10{
	replace tot_aecopd_`i'=0 if tot_aecopd_`i'==.
}

gen numerator=tot_aecopd_1 +tot_aecopd_2+ tot_aecopd_3+ tot_aecopd_4+ tot_aecopd_5+ tot_aecopd_6+ tot_aecopd_7 +tot_aecopd_8+ tot_aecopd_9+ tot_aecopd_10
drop tot_aecopd_1 tot_aecopd_2 tot_aecopd_3 tot_aecopd_4 tot_aecopd_5 tot_aecopd_6 tot_aecopd_7 tot_aecopd_8 tot_aecopd_9 tot_aecopd_10
save  "\CCU052\aecopd_numerator", replace 

*******************************************************************
*Combine with denominator made from mortality files 

use  "\CCU052\aecopd_numerator", clear 
drop if IMD_2019_DECILES==.
drop if region==""
merge 1:1 month_year SEX  age_group ethnicity IMD_2019_DECILES region using "\CCU052\copd_death_denom"
drop _m
recode numerator .=0
save  "\CCU052\aecopd_data", replace 





