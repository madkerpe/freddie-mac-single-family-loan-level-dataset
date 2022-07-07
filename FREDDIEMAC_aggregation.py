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
    df_t_del_30d = df_perf.groupby('LOAN_SEQUENCE_NUMBER')['T_DEL_30D'].sum()
    df = pd.merge(df, df_t_del_30d, on='LOAN_SEQUENCE_NUMBER', how='left')

    # number of times being 60 days delinquent in the last 12 months
    df_perf['T_DEL_60D'] = df_perf['CURRENT_LOAN_DELINQUENCY_STATUS'].map(lambda x: 1 if (x > 1) else 0)
    df_t_del_60d = df_perf.groupby('LOAN_SEQUENCE_NUMBER')['T_DEL_60D'].sum()
    df = pd.merge(df, df_t_del_60d, on='LOAN_SEQUENCE_NUMBER', how='left')

    # loan turning into 3 month delinquency
    # TODO concatenate to this length
    df_perf['DEFAULT'] = df_perf['CURRENT_LOAN_DELINQUENCY_STATUS'].map(lambda x: 1 if (x > 2) else 0)
    df_default = df_perf.groupby('LOAN_SEQUENCE_NUMBER')['DEFAULT'].sum()
    df_default = df_default.map(lambda x: 1 if x > 0 else 0)
    df = pd.merge(df, df_default, on='LOAN_SEQUENCE_NUMBER', how='left')

    # loan prepaying? "or Matured? (Voluntary payoff)"?
    df_perf['PREPAYMENT_OR_MATURED'] = df_perf['ZERO_BALANCE_CODE'].map(lambda x: 1 if (x == "01") else 0)
    df_prepayment = df_perf.groupby('LOAN_SEQUENCE_NUMBER')['PREPAYMENT_OR_MATURED'].sum()
    df_prepayment = df_prepayment.map(lambda x: 1 if x > 0 else 0)
    df = pd.merge(df, df_prepayment, on='LOAN_SEQUENCE_NUMBER', how='left')

    # remaining months
    df_remaining_months = df_perf.groupby('LOAN_SEQUENCE_NUMBER')['REMAINING_MONTHS_TO_LEGAL_MATURITY'].min()
    df = pd.merge(df, df_remaining_months, on='LOAN_SEQUENCE_NUMBER', how='left')

    LOAN_LEVEL_VARIABLES_CREATED_VARIABLES = ['BAL_REPAID', 'T_ACT_12M', 'T_DEL_30D', 'T_DEL_60D', 'DEFAULT', 'PREPAYMENT_OR_MATURED', 'REMAINING_MONTHS_TO_LEGAL_MATURITY']
    df = df[LOAN_LEVEL_VARIABLES_COPIED + LOAN_LEVEL_VARIABLES_CREATED_VARIABLES]
    
    # create labels (0 -> prepay, 1 -> default, 2 -> full repay, 3 -> censored)
    def label_row(row):

        if row['PREPAYMENT_OR_MATURED'] == 1:
            if row['REMAINING_MONTHS_TO_LEGAL_MATURITY'] >= 2:
                return 0
            else:
                return 2
        
        if row['DEFAULT'] == 1:
            return 1
        
        else:
            return 3

    df['LABEL'] = df.apply(lambda row: label_row(row), axis=1)

    LOAN_LEVEL_VARIABLES_CREATED_VARIABLES_NEEDED = ['BAL_REPAID', 'T_ACT_12M', 'T_DEL_30D', 'T_DEL_60D', 'LABEL']
    df = df[LOAN_LEVEL_VARIABLES_COPIED + LOAN_LEVEL_VARIABLES_CREATED_VARIABLES_NEEDED]

    return df