import pandas as pd

def aggregate_to_blumenstock_exp4(df_orig, df_perf):
    LOAN_LEVEL_VARIABLES = ['INT_RATE', 'ORIG_UPB', 'FICO_SCORE', 'DTI_R', 'LTV_R', 'BAL_REPAID', 'T_ACT_12M', 'T_DEL_30D', 'T_DEL_60D']

    # loan-level-variables that can just be copied
    LOAN_LEVEL_VARIABLES_COPIED = ['LOAN_SEQUENCE_NUMBER', 'ORIGINAL_INTEREST_RATE', 'ORIGINAL_UPB', 'CREDIT_SCORE', 'DTI', 'LTV']
    
    # current fraction of balance repaid
    df_min_actual_upb = df_perf.groupby('LOAN_SEQUENCE_NUMBER')['CURRENT_ACTUAL_UPB'].min()
    df = pd.merge(df_orig, df_min_actual_upb, on='LOAN_SEQUENCE_NUMBER', how='left')
    df['BAL_REPAID'] = df['CURRENT_ACTUAL_UPB'] / df['ORIGINAL_UPB']

    # number of times not being delinquent in the last 12 months
    df_perf['T_ACT_12M'] = df_perf['CURRENT_LOAN_DELINQUENCY_STATUS'].map(lambda x: 1 if (x == 0) else 0)
    df_t_act_12m = df_perf.groupby('LOAN_SEQUENCE_NUMBER')['T_ACT_12M'].sum()
    df = pd.merge(df, df_t_act_12m, on='LOAN_SEQUENCE_NUMBER', how='left')

    # number of times being 30 days delinquent in the last 12 months
    df_perf['T_DEL_30D'] = df_perf['CURRENT_LOAN_DELINQUENCY_STATUS'].map(lambda x: 1 if (x == 1) else 0)
    df_t_act_12m = df_perf.groupby('LOAN_SEQUENCE_NUMBER')['T_DEL_30D'].sum()
    df = pd.merge(df, df_t_act_12m, on='LOAN_SEQUENCE_NUMBER', how='left')

    # number of times being 60 days delinquent in the last 12 months
    df_perf['T_DEL_60D'] = df_perf['CURRENT_LOAN_DELINQUENCY_STATUS'].map(lambda x: 1 if (x > 1) else 0)
    df_t_act_12m = df_perf.groupby('LOAN_SEQUENCE_NUMBER')['T_DEL_60D'].sum()
    df = pd.merge(df, df_t_act_12m, on='LOAN_SEQUENCE_NUMBER', how='left')

    LOAN_LEVEL_VARIABLES_CREATED_VARIABLES = ['BAL_REPAID', 'T_ACT_12M', 'T_DEL_30D', 'T_DEL_60D']

    df = df[LOAN_LEVEL_VARIABLES_COPIED + LOAN_LEVEL_VARIABLES_CREATED_VARIABLES]


    return df



orig_headers = ['CREDIT_SCORE','FIRST_PAYMENT_DATE','FIRST_TIME_HOMEBUYER_FLAG','MATURITY_DATE','MSA','MI_PCT',
                'NUMBER_OF_UNITS','OCCUPANCY_STATUS','CLTV','DTI','ORIGINAL_UPB','LTV','ORIGINAL_INTEREST_RATE',
                'CHANNEL','PPM','AMORTIZATION_TYPE','PROPERTY_STATE', 'PROPERTY_TYPE','POSTAL_CODE',
                'LOAN_SEQUENCE_NUMBER','LOAN_PURPOSE', 'ORIGINAL_LOAN_TERM','NUMBER_OF_BORROWERS','SELLER_NAME',
                'SERVICER_NAME','SUPER_CONFORMING_FLAG','PRE-RELIEF_REFINANCE_LOAN_SEQUENCE_NUMBER', 
                'PROGRAM_INDICATOR', 'RELIEF_REFINANCE_INDICATOR', 'PROPERTY_VALUATION_METHOD', 'IO_INDICATOR']

