*Calculate rates 
**************************************************
use  "\CCU052\aeild_data", clear 

gen year=year(month_year)
gen month=month(month_year)

gen time_period= 12*(year-2019)+(month-10)
gen quarter=ceil(time_period/3)

collapse (sum) numerator (sum) tot_person_time, by(SEX region IMD_2019_DECILES ethnicity age_group quarter)
save  "\CCU052\aeild_data_3months", replace 


use  "\CCU052\aeild_data", clear 

gen year=year(month_year)
gen month=month(month_year)

gen time_period= 12*(year-2019)+(month-10)
gen half=ceil(time_period/6)

collapse (sum) numerator (sum) tot_person_time, by(SEX region IMD_2019_DECILES ethnicity age_group half)
save  "\CCU052\aeild_data_6months", replace 

 ********************************************************
*1) OVERALL IMD STRATIFIED RATES PER MONTH 
********************************************************
*1a) Crude rates stratified by IMD /////////////////////

use  "\CCU052\aeild_data_3months", clear 

sort quarter IMD_2019_DECILES 
by quarter IMD_2019_DECILES: egen overall_numerator=total(numerator)
by quarter IMD_2019_DECILES: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep quarter IMD_2019_DECILES overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 
*
forvalues i=1/150{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "\CCU052\Final_rates\crude_aeild_by_imd_3months", replace 

export delimited quarter  overall_numerator overall_denom IMD_2019_DECILES rate_est rate_se rate_lb rate_ub  using  "\CCU052\Final_rates\crude_aeild_by_imd.csv" , datafmt replace 

*2a) Crude rates stratified by ethnicity /////////////////////

use  "\CCU052\aeild_data_6months", clear 

sort half ethnicity 
by half ethnicity: egen overall_numerator=total(numerator)
by half ethnicity: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep half ethnicity overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 


forvalues i=1/48{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "\CCU052\Final_rates\crude_aeild_by_ethnic_6months", replace 

export delimited half  overall_numerator overall_denom ethnicity rate_est rate_se rate_lb rate_ub  using  "\CCU052\Final_rates\crude_aeild_by_ethnic.csv" , datafmt replace 

* Crude rates stratified by region /////////////////////

use  "\CCU052\aeild_data_3months", clear 

sort quarter region 
by quarter region: egen overall_numerator=total(numerator)
by quarter region: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep quarter region overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/135{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "\CCU052\Final_rates\crude_aeild_by_region_3months", replace 

export delimited quarter  overall_numerator overall_denom region rate_est rate_se rate_lb rate_ub  using  "\CCU052\Final_rates\crude_aeild_by_region.csv" , datafmt replace 



******************************************************
*1b) Age & sex adjusted IMD stratified monthly rates //////////////////////////
use  "\CCU052\aeild_data_3months", clear 

sort quarter SEX age_group IMD_2019_DECILES
by quarter SEX age_group IMD_2019_DECILES: egen overall_numerator=total(numerator)
by quarter SEX age_group IMD_2019_DECILES: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep quarter SEX age_group overall_numerator overall_denom IMD_2019_DECILES
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "\CCU052\age_sex_weights_40plus.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort quarter IMD_2019_DECILES
by quarter IMD_2019_DECILES: egen step2=total(age_sex_standardised_rate)

*sum expected denominator
sort quarter IMD_2019_DECILES
by quarter IMD_2019_DECILES: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep quarter IMD_2019_DECILES step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 


forvalues i=1/150{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}


save  "\CCU052\Final_rates\adj_aeild_by_imd_3months", replace 

export delimited quarter IMD_2019_DECILES rate_est rate_lb rate_ub using  "\CCU052\Final_rates\adj_aeild_by_imd.csv" , datafmt replace 


******************************************************
*2b) Age & sex adjusted ethnicity stratified monthly rates //////////////////////////

use  "\CCU052\aeild_data_6months", clear 

sort half SEX age_group ethnicity
by half SEX age_group ethnicity: egen overall_numerator=total(numerator)
by half SEX age_group ethnicity: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep half SEX age_group overall_numerator overall_denom ethnicity
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "\CCU052\age_sex_weights_40plus.dta"
drop _m



gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort half ethnicity
by half ethnicity: egen step2=total(age_sex_standardised_rate)

*sum expected denominator
sort half ethnicity
by half ethnicity: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep half ethnicity step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/48{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}


save  "\CCU052\Final_rates\adj_aeild_by_ethnic_6months", replace 

export delimited half ethnicity rate_est rate_lb rate_ub using  "\CCU052\Final_rates\adj_aeild_by_ethnic.csv" , datafmt replace 



******************************************************
* Age & sex adjusted region stratified monthly rates //////////////////////////

use  "\CCU052\aeild_data_3months", clear 

sort quarter SEX age_group region
by quarter SEX age_group region: egen overall_numerator=total(numerator)
by quarter SEX age_group region: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep quarter SEX age_group overall_numerator overall_denom region
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "\CCU052\age_sex_weights_40plus.dta"
drop _m
drop if region==""


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort quarter region
by quarter region: egen step2=total(age_sex_standardised_rate)

*sum expected denominator
sort quarter region
by quarter region: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep quarter region step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/135{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}


save  "\CCU052\Final_rates\adj_aeild_by_region_3months", replace 

export delimited quarter region rate_est rate_lb rate_ub using  "\CCU052\Final_rates\adj_aeild_by_region.csv" , datafmt replace 



**********************************
*OVER all-cause mortality 
*******************************
*Crude rates ////////////////////////////////

use  "\CCU052\aeild_data", clear 

sort month_year 
by month_year: egen overall_numerator=total(numerator)
by month_year: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days


keep month_year overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/44{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

export delimited month_year overall_numerator overall_denom rate_est rate_se rate_lb rate_ub  using  "\CCU052\Final_rates\crude_aeild_rates.csv" , datafmt replace 

save  "\CCU052\Final_rates\crude_aeild_rates", replace 

*Age & sex adjusted overall monthly rates //////////////////////////////

use  "\CCU052\aeild_data", clear 

sort month_year SEX age_group
by month_year SEX age_group: egen overall_numerator=total(numerator)
by month_year SEX age_group: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year SEX age_group overall_numerator overall_denom
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "\CCU052\age_sex_weights_40plus.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort month_year
by month_year: egen step2=total(age_sex_standardised_rate)

*sum denominator 
sort month_year
by month_year: egen step3=total(europeanstandardpopulation)

*calculate rates iwth new numerator (step2) and denominator (step3)
keep month_year  step2  step3
duplicates drop 
gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/44{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}


save  "\CCU052\Final_rates\adj_aeild_rates", replace 

export delimited month_year  rate_est rate_lb rate_ub using  "\CCU052\Final_rates\adj_aeild_rates.csv" , datafmt replace 


 
 ********************************************************
*OVERALL SEX STRATIFIED RATES PER MONTH 
********************************************************

use  "\CCU052\aeild_data", clear 

sort month_year SEX 
by month_year SEX: egen overall_numerator=total(numerator)
by month_year SEX: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep month_year SEX overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

*44 months * 2 sexes=88
forvalues i=1/88{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "\CCU052\Final_rates\crude_aeild_by_sex_", replace 

export delimited month_year SEX overall_numerator overall_denom rate_est rate_se rate_lb rate_ub  using  "\CCU052\Final_rates\crude_aeild_by_sex.csv" , datafmt replace 


*Age adjusted sex stratified monthly rates //////////////////////////

use  "\CCU052\aeild_data", clear 

sort month_year age_group SEX 
by month_year age_group SEX: egen overall_numerator=total(numerator)
by month_year age_group SEX : egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days


keep month_year SEX age_group overall_numerator overall_denom
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "\CCU052\age_sex_weights_40plus.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort month_year sex
by month_year sex: egen step2=total(age_sex_standardised_rate)

*sum expected denom
sort month_year sex
by month_year sex: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep month_year sex step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/88{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

save  "\CCU052\Final_rates\adj_aeild_by_sex", replace 

export delimited month_year sex rate_est rate_lb rate_ub using  "\CCU052\Final_rates\adj_aeild_by_sex.csv" , datafmt replace 

 
 ********************************************************
*3) OVERALL Age STRATIFIED RATES PER MONTH 
********************************************************
*3a) Crude rates stratified by age group /////////////////////in men and women? 

use  "\CCU052\aeild_data_3months", clear 
sort quarter age_group 
by quarter age_group: egen overall_numerator=total(numerator)
by quarter age_group: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep quarter age_group overall_numerator overall_denom
duplicates drop
gen litn=_n


gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/60{

ci means overall_numerator if litn==`i', poisson exposure(overall_denom)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}
save  "\CCU052\Final_rates\crude_aeild_by_age_3months", replace 

export delimited quarter overall_numerator overall_denom age_group rate_est rate_se rate_lb rate_ub  using  "\CCU052\Final_rates\crude_aeild_by_age.csv" , datafmt replace 



*3b) Sex adjusted age stratified monthly rates //////////////////////////

use  "\CCU052\aeild_data_3months", clear 

sort quarter SEX age_group
by quarter SEX age_group: egen overall_numerator=total(numerator)
by quarter SEX age_group: egen overall_denom=total(tot_person_time)
replace overall_denom=overall_denom/365.25 // To generate person-years instead of person-days

keep quarter SEX age_group overall_numerator overall_denom
duplicates drop

rename SEX sex
destring sex, replace 
label define lab_s 1"Male" 2"Female"
label values sex lab_s

merge m:1 sex age_group using "\CCU052\age_sex_weights_40plus.dta"
drop _m


gen age_sex_standardised_rate=((overall_numerator/overall_denom)*europeanstandardpopulation)

*times rate by the EU population for that strata 
*keep month_year overall_numerator overall_denom  age_sex_standardised_rate eu_population europeanstandardpopulation

*sum expected numerator
sort quarter age_group
by quarter age_group: egen step2=total(age_sex_standardised_rate)

*sum expected denominator
sort quarter age_group
by quarter age_group: egen step3=total(europeanstandardpopulation)


*calculate rates wiwth new numerator (step2) and denominator (step3)
keep quarter age_group step3 step2  
duplicates drop 

gen litn=_n 

gen rate_est = .
gen rate_se = .
gen rate_lb = .
gen rate_ub = .
format rate_est rate_se rate_lb rate_ub %9.2f 

forvalues i=1/60{

ci means step2 if litn==`i', poisson exposure(step3)
replace rate_est =1000*r(mean) if litn==`i'
replace rate_se =1000*r(se) if litn==`i'
replace rate_lb = 1000*r(lb) if litn==`i'
replace rate_ub = 1000*r(ub) if litn==`i'
}

save  "\CCU052\Final_rates\adj_aeild_by_age_3months", replace 

export delimited quarter age_group rate_est rate_lb rate_ub using  "\CCU052\Final_rates\adj_aeild_by_age.csv" , datafmt replace 


