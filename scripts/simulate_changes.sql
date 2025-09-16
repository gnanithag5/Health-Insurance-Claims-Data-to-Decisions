-- STEP 1: Simulate provider fraud status change in RAW
update HC_CLAIMS.RAW_DATA.PROVIDER_RAW
set POTENTIALFRAUD = 'Yes'
where PROVIDER = 'PRV51001';

-- STEP 2: Simulate beneficiary coverage change in RAW
update HC_CLAIMS.RAW_DATA.BENEFICIARY_RAW
set NoOfMonths_PartACov = NoOfMonths_PartACov - 6
where BENEID = 'BENE11002';

-- STEP 3: Simulate beneficiary chronic condition change in RAW
update HC_CLAIMS.RAW_DATA.BENEFICIARY_RAW
set ChronicCond_Diabetes = 1
where BENEID = 'BENE11003';

-- STEP 4: Simulate diagnosis change in RAW inpatient
update HC_CLAIMS.RAW_DATA.INPATIENT_RAW
set ClmDiagnosisCode_1 = '1745'  
where CLAIMID = 'CLM46614'
  and ClmDiagnosisCode_1 = '1970';

-- STEP 5: Simulate procedure change in RAW outpatient
update HC_CLAIMS.RAW_DATA.OUTPATIENT_RAW
set ClmDiagnosisCode_2 = '28860'  
where CLAIMID = 'CLM624349'
  and ClmDiagnosisCode_2 = 'V5866';  
