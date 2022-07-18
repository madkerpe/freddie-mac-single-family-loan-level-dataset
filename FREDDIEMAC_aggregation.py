import pandas as pd


def aggregate_to_blumenstock_exp4(df_orig, df_perf_shortened):

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
        "TIME_TO_EVENT"   
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

    return df


# def aggregate_to_blumenstock_dynamic(df_orig, df_perf_shortened):
#     LOAN_LEVEL_VARIABLES_COPIED = [
#         "LOAN_SEQUENCE_NUMBER",
#         "ORIGINAL_INTEREST_RATE",
#         "ORIGINAL_UPB",
#         "CREDIT_SCORE",
#         "DTI",
#         "LTV",
#     ]

#     return df_perf_shortened

