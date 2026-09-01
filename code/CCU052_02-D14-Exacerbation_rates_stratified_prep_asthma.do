*asthma EXACERBATION

****************************************************************
*Numerator
***************************************************************

forvalues i=1/10{
use "\CCU052\chunk`i'.dta", clear
merge 1:m PERSON_ID using "\CCU052\asthma_exacerbations.dta"
keep if _m==3 
drop _m
keep if region!="" 

*Generate a month/year variable from the date of incident aeasthma/aeasthma etc- this will be the grouping variable for numberator counts 

gen ethnicity=1 if ETHNIC_CAT=="White"
replace ethnicity=2 if ETHNIC_CAT=="Black or Black British"
replace ethnicity=3 if ETHNIC_CAT=="Asian or Asian British"
replace ethnicity=4 if ETHNIC_CAT=="Mixed"
replace ethnicity=5 if ETHNIC_CAT=="Other"
replace ethnicity=6 if ETHNIC_CAT=="Unknown"
replace ethnicity=6 if ETHNIC_CAT==""
label define lab_ethnic 1"White" 2"Black" 3"Asian" 4"Mixed" 5"Other" 6"Uknown"
label values ethnicity lab_ethnic

*keep those who had asthma
keep if cov_hx_out_asthma_flag==1 
codebook PERSON_ID 

*generating age. Stratified by baseline age? With the standardised rates does age need to be updated every year/month? Also, what is everyone's age cut off for aeasthma-40? use 40 aeasthma, 40 for ild
sort month_year
gen age=month_year-DOB
replace age=age/365.25
gen age_group=1 if age>=0 & age<5
replace age_group=2 if age>=5 & age<10
replace age_group=3 if age>=10 & age<15
replace age_group=4 if age>=15 & age<20
replace age_group=5 if age>=20 & age<30
replace age_group=6 if age>=30 & age<40
replace age_group=7 if age>=40 & age<50
replace age_group=8 if age>=50 & age<60
replace age_group=9 if age>=60 & age<70
replace age_group=10 if age>=70 

label define lab_age 1"0-4" 2"5-9" 3"10-14" 4"15-19" 5"20-30" 6"30-40" 7"40-50" 8"50-60" 9"60-70" 10"70+" 
label values age_group lab_age 



*stratify by sex, age, ethnicity, IMD
sort month_year SEX  age_group ethnicity IMD_2019_DECILES region
by  month_year SEX  age_group ethnicity IMD_2019_DECILES region: gen litn=_n
by  month_year SEX  age_group ethnicity IMD_2019_DECILES region: gen bign=_N 
keep if litn==bign 
drop litn 
rename bign tot_aeasthma_`i'
keep month_year SEX  age_group ethnicity IMD_2019_DECILES region tot_aeasthma_`i'  
save "\CCU052\aeasthma_numerator_chunk`i'", replace 
}

*Merge all together and sum counts from each count for each month
use "\CCU052\aeasthma_numerator_chunk1", clear 
forvalues i=2/10{
	merge 1:1 month_year SEX  age_group ethnicity IMD_2019_DECILES region using "\CCU052\aeasthma_numerator_chunk`i'"
	drop _m
}

sort  month_year SEX  age_group ethnicity IMD_2019_DECILES region
forvalues i=1/10{
	replace tot_aeasthma_`i'=0 if tot_aeasthma_`i'==.
}

gen numerator=tot_aeasthma_1 +tot_aeasthma_2+ tot_aeasthma_3+ tot_aeasthma_4+ tot_aeasthma_5+ tot_aeasthma_6+ tot_aeasthma_7 +tot_aeasthma_8+ tot_aeasthma_9+ tot_aeasthma_10
drop tot_aeasthma_1 tot_aeasthma_2 tot_aeasthma_3 tot_aeasthma_4 tot_aeasthma_5 tot_aeasthma_6 tot_aeasthma_7 tot_aeasthma_8 tot_aeasthma_9 tot_aeasthma_10
save  "\CCU052\aeasthma_numerator", replace 

*******************************************************************
*Combine with denominator made from mortality files 

use  "\CCU052\aeasthma_numerator", clear 
drop if IMD_2019_DECILES==.
drop if region==""
merge 1:1 month_year SEX  age_group ethnicity IMD_2019_DECILES region using "\CCU052\asthma_death_denom"
drop _m
recode numerator .=0
save  "\CCU052\aeasthma_data", replace 





