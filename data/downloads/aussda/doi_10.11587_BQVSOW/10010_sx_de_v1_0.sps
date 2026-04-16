* Encoding: UTF-8.

* Modell 1993

DATASET ACTIVATE DataSet1.
USE ALL.
COMPUTE filter_$=(year_5 = 1).
VARIABLE LABELS filter_$ 'year_5 = 1 (FILTER)'.
VALUE LABELS filter_$ 0 'Not Selected' 1 'Selected'.
FORMATS filter_$ (f1.0).
FILTER BY filter_$.
EXECUTE.

REGRESSION
  /DESCRIPTIVES MEAN STDDEV CORR SIG N
  /MISSING LISTWISE
  /STATISTICS COEFF OUTS R ANOVA COLLIN TOL
  /CRITERIA=PIN(.05) POUT(.10)
  /NOORIGIN 
  /DEPENDENT topbot_rec
  /METHOD=ENTER degree_zentr_1993 degree_zentr_quadr_1993 hhq1_dummy hhq2_dummy hhq3_dummy hhek_q0 earningsq1_dummy earningsq2_dummy earningsq3_dummy earnings_q0 isei 
age_zentr_1993 age_zentr_1993_sqare sex partner_erwerbstätig kinderanzahl.

# Modell 2000

USE ALL.
COMPUTE filter_$=(year_5 = 2).
VARIABLE LABELS filter_$ 'year_5 = 2 (FILTER)'.
VALUE LABELS filter_$ 0 'Not Selected' 1 'Selected'.
FORMATS filter_$ (f1.0).
FILTER BY filter_$.
EXECUTE.

DATASET ACTIVATE DataSet1.
REGRESSION
  /DESCRIPTIVES MEAN STDDEV CORR SIG N
  /MISSING LISTWISE
  /STATISTICS COEFF OUTS R ANOVA COLLIN TOL
  /CRITERIA=PIN(.05) POUT(.10)
  /NOORIGIN 
  /DEPENDENT topbot_rec
  /METHOD=ENTER degree_zentr_2000 degree_zentr_quadr_2000 hhq1_dummy hhq2_dummy hhq3_dummy hhek_q0 earningsq1_dummy earningsq2_dummy earningsq3_dummy earnings_q0 isei
age_zentr_2000 age_zentr_square_2000 sex partner_erwerbstätig hhgröße.

# Modell 2003

USE ALL.
COMPUTE filter_$=(year_5 = 3).
VARIABLE LABELS filter_$ 'year_5 = 3 (FILTER)'.
VALUE LABELS filter_$ 0 'Not Selected' 1 'Selected'.
FORMATS filter_$ (f1.0).
FILTER BY filter_$.
EXECUTE.

REGRESSION
  /DESCRIPTIVES MEAN STDDEV CORR SIG N
  /MISSING LISTWISE
  /STATISTICS COEFF OUTS R ANOVA COLLIN TOL
  /CRITERIA=PIN(.05) POUT(.10)
  /NOORIGIN 
  /DEPENDENT topbot_rec
  /METHOD=ENTER degree_zentr_2003 degree_zentr_2003_quadr hhq1_dummy hhq2_dummy hhq3_dummy hhek_q0 age_zentr_2003 age_zentr_2003_quadr earningsq1_dummy 
    earningsq2_dummy earningsq3_dummy earnings_q0 isei 
sex partner_erwerbstätig kinderanzahl.

# Modell 2010

USE ALL.
COMPUTE filter_$=(year_5 = 4).
VARIABLE LABELS filter_$ 'year_5 = 4 (FILTER)'.
VALUE LABELS filter_$ 0 'Not Selected' 1 'Selected'.
FORMATS filter_$ (f1.0).
FILTER BY filter_$.
EXECUTE.

REGRESSION
  /DESCRIPTIVES MEAN STDDEV CORR SIG N
  /MISSING LISTWISE
  /STATISTICS COEFF OUTS R ANOVA COLLIN TOL
  /CRITERIA=PIN(.05) POUT(.10)
  /NOORIGIN 
  /DEPENDENT topbot_rec
  /METHOD=ENTER degree_zentr_2010 degree_zentr_2010_square hhq1_dummy hhq2_dummy hhq3_dummy hhek_q0 age_zentr_2010 age_zentr_2010_square sex earningsq1_dummy 
    earningsq2_dummy earningsq3_dummy earnings_q0 isei partner_erwerbstätig hhgröße.

# Modell 2016

USE ALL.
COMPUTE filter_$=(year_5 = 5).
VARIABLE LABELS filter_$ 'year_5 = 5 (FILTER)'.
VALUE LABELS filter_$ 0 'Not Selected' 1 'Selected'.
FORMATS filter_$ (f1.0).
FILTER BY filter_$.
EXECUTE.

REGRESSION
  /DESCRIPTIVES MEAN STDDEV CORR SIG N
  /MISSING LISTWISE
  /STATISTICS COEFF OUTS R ANOVA COLLIN TOL
  /CRITERIA=PIN(.05) POUT(.10)
  /NOORIGIN 
  /DEPENDENT topbot_rec
  /METHOD=ENTER degree_zentr_2016 degree_zentr_2016_square hhq1_dummy hhq2_dummy hhq3_dummy hhek_q0 age_zentr_2016 age_zentr_2016_square sex earningsq1_dummy 
    earningsq2_dummy earningsq3_dummy earnings_q0 isei partner_erwerbstätig 
    kinderanzahl.

# Gesamtmodell 1993-2016

FILTER OFF.
USE ALL.
EXECUTE.

REGRESSION
  /DESCRIPTIVES MEAN STDDEV CORR SIG N
  /MISSING LISTWISE
  /STATISTICS COEFF OUTS R ANOVA COLLIN TOL
  /CRITERIA=PIN(.05) POUT(.10)
  /NOORIGIN 
  /DEPENDENT topbot_rec
  /METHOD=ENTER year_5 degree_zentr_overall degree_zentr_overall_square hhq1_dummy hhq2_dummy hhq3_dummy hhek_q0 age_zentr_overall age_zentr_overall_square sex earningsq1_dummy 
    earningsq2_dummy earningsq3_dummy earnings_q0 partner_erwerbstätig isei.




