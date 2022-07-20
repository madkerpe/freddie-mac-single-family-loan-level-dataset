import pandas as pd
import dask.dataframe as dd

def aggregate_to_blumenstock_exp4(df_orig, df_perf_shortened):

    """
    Blumenstock's experiment 4 specifies the following variables:
        ['INT_RATE',
        'ORIG_UPB', 
        'FICO_SCORE', 
        'DTI_R', 
        'LTV_R', 
        'BAL_REPAID', 
        'T_ACT_12M', 
        'T_DEL_30D', 
        'T_DEL_60D']
    
    we add the following variables, mainly for the sake of completeness:
        ['LOAN_SEQUENCE_NUMBER',
        'FIRST_PAYMENT_DATE', 
        'REMAINING_MONTHS_TO_LEGAL_MATURITY', 
        'TOTAL_OBSERVED_LENGTH', 
        'TIME_TO_EVENT', 
        'LABEL']
    """

    # loan-level-variables that can just be copied
    LOAN_LEVEL_VARIABLES_COPIED = [
        "LOAN_SEQUENCE_NUMBER",
        "ORIGINAL_INTEREST_RATE",
        "ORIGINAL_UPB",
        "CREDIT_SCORE",
        "DTI",
        "LTV",
        "FIRST_PAYMENT_DATE",
        "REMAINING_MONTHS_TO_LEGAL_MATURITY",
        "TOTAL_OBSERVED_LENGTH",
        "TIME_TO_EVENT",
        "LABEL" 
    ]

    # current fraction of balance repaid
    df_min_actual_upb = df_perf_shortened.groupby("LOAN_SEQUENCE_NUMBER")["CURRENT_ACTUAL_UPB"].min()
    df = pd.merge(df_orig, df_min_actual_upb, on="LOAN_SEQUENCE_NUMBER", how="left")
    df["BAL_REPAID"] = df["CURRENT_ACTUAL_UPB"] / df["ORIGINAL_UPB"]

    # number of times not being delinquent in the last 12 months
    df_perf_shortened["T_ACT_12M"] = df_perf_shortened["CURRENT_LOAN_DELINQUENCY_STATUS"].map(lambda x: 1 if (x == 0) else 0)
    df_t_act_12m = df_perf_shortened.groupby("LOAN_SEQUENCE_NUMBER")["T_ACT_12M"].sum()
    df = pd.merge(df, df_t_act_12m, on="LOAN_SEQUENCE_NUMBER", how="left")

    # number of times being 30 days delinquent in the last 12 months
    df_perf_shortened["T_DEL_30D"] = df_perf_shortened["CURRENT_LOAN_DELINQUENCY_STATUS"].map(lambda x: 1 if (x == 1) else 0)
    df_t_del_30d = df_perf_shortened.groupby("LOAN_SEQUENCE_NUMBER")["T_DEL_30D"].sum()
    df = pd.merge(df, df_t_del_30d, on="LOAN_SEQUENCE_NUMBER", how="left")

    # number of times being 60 days delinquent in the last 12 months
    df_perf_shortened["T_DEL_60D"] = df_perf_shortened["CURRENT_LOAN_DELINQUENCY_STATUS"].map(lambda x: 1 if (x > 1) else 0)
    df_t_del_60d = df_perf_shortened.groupby("LOAN_SEQUENCE_NUMBER")["T_DEL_60D"].sum()
    df = pd.merge(df, df_t_del_60d, on="LOAN_SEQUENCE_NUMBER", how="left")

    LOAN_LEVEL_VARIABLES_CREATED_VARIABLES = [
        "BAL_REPAID",
        "T_ACT_12M",
        "T_DEL_30D",
        "T_DEL_60D"
    ]

    df = df[LOAN_LEVEL_VARIABLES_COPIED + LOAN_LEVEL_VARIABLES_CREATED_VARIABLES]

    df = df.rename(columns={'ORIGINAL_INTEREST_RATE': 'INT_RATE',
                'ORIGINAL_UPB' : 'ORIG_UPB',
                'CREDIT_SCORE': 'FICO_SCORE',
                'DTI': 'DTI_R',
                'LTV': 'LTV_R',
                'BAL_REPAID': 'BAL_REPAID',
                'T_ACT_12M': 'T_ACT_12M',
                'T_DEL_30D': 'T_DEL_30D',
                'T_DEL_60D': 'T_DEL_60D'})

    return df


def aggregate_to_blumenstock_exp4_dynamic(df_orig, df_perf):

    """
    Blumenstock's experiment 4 specifies the following variables, 
    we make the dynamic in the following way:
        ['INT_RATE' --> CURRENT_INTEREST_RATE, ORIGINAL_INTEREST_RATE,
        'ORIG_UPB', --> CURRENT_ACTUAL_UPB, ORIGINAL_UPB, 
        'FICO_SCORE', --> CREDIT_SCORE
        'DTI_R', --> DTI,
        'LTV_R', --> LTV, ELTV
        'BAL_REPAID', --> CURRENT_ACTUAL_UPB, ORIGINAL_UPB,
        'T_ACT_12M', --> CURRENT_LOAN_DELINQUENCY_STATUS
        'T_DEL_30D', --> CURRENT_LOAN_DELINQUENCY_STATUS
        'T_DEL_60D' --> CURRENT_LOAN_DELINQUENCY_STATUS]
    
    we add the following variables, mainly for the sake of completeness:
        ['LOAN_SEQUENCE_NUMBER', --> LOAN_SEQUENCE_NUMBER
        'FIRST_PAYMENT_DATE', --> MONTHLY_REPORTING_PERIOD,
        'REMAINING_MONTHS_TO_LEGAL_MATURITY', --> REMAINING_MONTHS_TO_LEGAL_MATURITY,
        'TOTAL_OBSERVED_LENGTH', --> TOTAL_OBSERVED_LENGTH,
        'TIME_TO_EVENT', --> TIME_TO_EVENT,
        'LABEL' --> LABEL]
    """

    # loan-level-variables that can just be copied from df_perf
    LOAN_LEVEL_VARIABLES_COPIED = [
        "CURRENT_INTEREST_RATE",
        "ELTV",
        "CURRENT_ACTUAL_UPB",
        "CURRENT_LOAN_DELINQUENCY_STATUS",
        "MONTHLY_REPORTING_PERIOD",
        "REMAINING_MONTHS_TO_LEGAL_MATURITY",
        "LOAN_AGE",
        "TOTAL_OBSERVED_LENGTH",
        "TIME_TO_EVENT",
        "LABEL"
    ]

    # static loan-level-variables from df_orig
    LOAN_LEVEL_VARIABLES_STATIC = [
        "LOAN_SEQUENCE_NUMBER",
        "ORIGINAL_INTEREST_RATE",
        "ORIGINAL_UPB",
        "CREDIT_SCORE",
        "DTI",
        "LTV"
    ]

    # loan-level-variables that are created from df_orig and df_perf
    LOAN_LEVEL_VARIABLES_COMPUTED = [
        "BAL_REPAID"
    ]

    df = dd.merge(df_perf, df_orig[LOAN_LEVEL_VARIABLES_STATIC], on="LOAN_SEQUENCE_NUMBER", how="left")
    
    df["BAL_REPAID"] = df["CURRENT_ACTUAL_UPB"] / df["ORIGINAL_UPB"]

    df = df[LOAN_LEVEL_VARIABLES_COPIED + LOAN_LEVEL_VARIABLES_STATIC + LOAN_LEVEL_VARIABLES_COMPUTED]        

    return df


"""
orig_headers = ['CREDIT_SCORE','FIRST_PAYMENT_DATE','FIRST_TIME_HOMEBUYER_FLAG','MATURITY_DATE','MSA','MI_PCT',
                'NUMBER_OF_UNITS','OCCUPANCY_STATUS','CLTV','DTI','ORIGINAL_UPB','LTV','ORIGINAL_INTEREST_RATE',
                'CHANNEL','PPM','AMORTIZATION_TYPE','PROPERTY_STATE', 'PROPERTY_TYPE','POSTAL_CODE',
                'LOAN_SEQUENCE_NUMBER','LOAN_PURPOSE', 'ORIGINAL_LOAN_TERM','NUMBER_OF_BORROWERS','SELLER_NAME',
                'SERVICER_NAME','SUPER_CONFORMING_FLAG','PRE-RELIEF_REFINANCE_LOAN_SEQUENCE_NUMBER', 
                'PROGRAM_INDICATOR', 'RELIEF_REFINANCE_INDICATOR', 'PROPERTY_VALUATION_METHOD', 'IO_INDICATOR']

perf_headers = ['LOAN_SEQUENCE_NUMBER','MONTHLY_REPORTING_PERIOD','CURRENT_ACTUAL_UPB',
                'CURRENT_LOAN_DELINQUENCY_STATUS','LOAN_AGE','REMAINING_MONTHS_TO_LEGAL_MATURITY', 
                'DEFECT_SETTLEMENT_DATE','MODIFICATION_FLAG', 'ZERO_BALANCE_CODE', 
                'ZERO_BALANCE_EFFECTIVE_DATE','CURRENT_INTEREST_RATE','CURRENT_DEFERRED_UPB','DDLPI',
                'MI_RECOVERIES', 'NET_SALE_PROCEEDS','NON_MI_RECOVERIES','EXPENSES', 'LEGAL_COSTS',
                'MAINTENANCE_AND_PRESERVATION_COSTS','TAXES_AND_INSURANCE','MISCELLANEOUS_EXPENSES',
                'ACTUAL_LOSS_CALCULATION', 'MODIFICATION_COST','STEP_MODIFICATION_FLAG','DEFERRED_PAYMENT_PLAN',
                'ELTV','ZERO_BALANCE_REMOVAL_UPB','DELINQUENT_ACCRUED_INTEREST','DELINQUENCY_DUE_TO_DISASTER',
                'BORROWER_ASSISTANCE_STATUS_CODE','CURRENT_MONTH_MODIFICATION_COST','INTEREST_BEARING_UPB']
"""

