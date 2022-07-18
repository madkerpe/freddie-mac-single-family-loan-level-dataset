from wsgiref import headers
from tqdm import tqdm
import glob
import pandas as pd


def orig_fill_NAN(df):
    df["CREDIT_SCORE"] = df["CREDIT_SCORE"].fillna(0)
    df["FIRST_TIME_HOMEBUYER_FLAG"] = df["FIRST_TIME_HOMEBUYER_FLAG"].fillna("X")
    df["MSA"] = df["MSA"].fillna(0)
    df["MI_PCT"] = df["MI_PCT"].fillna(0)
    df["NUMBER_OF_UNITS"] = df["NUMBER_OF_UNITS"].fillna(0)
    df["OCCUPANCY_STATUS"] = df["OCCUPANCY_STATUS"].fillna("X")
    df["CLTV"] = df["CLTV"].fillna(0)
    df["DTI"] = df["DTI"].fillna(0)
    df["LTV"] = df["LTV"].fillna(0)
    df["CHANNEL"] = df["CHANNEL"].fillna("X")
    df["PPM"] = df["PPM"].fillna("X")
    df["PROPERTY_TYPE"] = df["PROPERTY_TYPE"].fillna("XX")
    df["POSTAL_CODE"] = df["POSTAL_CODE"].fillna(0)
    df["LOAN_PURPOSE"] = df["LOAN_PURPOSE"].fillna("X")
    df["NUMBER_OF_BORROWERS"] = df["NUMBER_OF_BORROWERS"].fillna(0)
    df["SUPER_CONFORMING_FLAG"] = df["SUPER_CONFORMING_FLAG"].fillna("N")
    df["PRE-RELIEF_REFINANCE_LOAN_SEQUENCE_NUMBER"] = df[
        "PRE-RELIEF_REFINANCE_LOAN_SEQUENCE_NUMBER"
    ].fillna("X")
    df["RELIEF_REFINANCE_INDICATOR"] = df["RELIEF_REFINANCE_INDICATOR"].fillna("X")
    return df


def orig_change_data_type(df):
    # Change the data types for all column
    # df[['CREDIT_SCORE','MSA','MI_PCT','NUMBER_OF_BORROWERS','NUMBER_OF_UNITS','CLTV','DTI','ORIGINAL_UPB','LTV','POSTAL_CODE','ORIGINAL_LOAN_TERM']] = df[['CREDIT_SCORE','MSA','MI_PCT','NUMBER_OF_BORROWERS','NUMBER_OF_UNITS','CLTV','DTI','ORIGINAL_UPB','LTV','POSTAL_CODE','ORIGINAL_LOAN_TERM']].astype('int64')
    # df[['SUPER_CONFORMING_FLAG','SERVICER_NAME']] = df[['SUPER_CONFORMING_FLAG','SERVICER_NAME']].astype('str')
    return df


def orig_mutate_data(df):
    df["QUARTER"] = [x[4:5] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))]
    df["YEAR"] = [x[1:3] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))]
    df["LOAN_SEQUENCE_NUMBER"] = [
        x[5:] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))
    ]
    return df


def perf_fill_NAN(df):
    df["CURRENT_LOAN_DELINQUENCY_STATUS"] = df[
        "CURRENT_LOAN_DELINQUENCY_STATUS"
    ].fillna(0)
    df["DEFECT_SETTLEMENT_DATE"] = df["DEFECT_SETTLEMENT_DATE"].fillna("X")
    df["MODIFICATION_FLAG"] = df["MODIFICATION_FLAG"].fillna("N")
    df["ZERO_BALANCE_CODE"] = df["ZERO_BALANCE_CODE"].fillna(00)
    df["ZERO_BALANCE_EFFECTIVE_DATE"] = df["ZERO_BALANCE_EFFECTIVE_DATE"].fillna(
        "189901"
    )
    df["CURRENT_DEFERRED_UPB"] = df["CURRENT_DEFERRED_UPB"].fillna(0)
    df["DDLPI"] = df["DDLPI"].fillna("189901")
    df["MI_RECOVERIES"] = df["MI_RECOVERIES"].fillna(0)
    df["NET_SALE_PROCEEDS"] = df["NET_SALE_PROCEEDS"].fillna("U")
    df["NON_MI_RECOVERIES"] = df["NON_MI_RECOVERIES"].fillna(0)
    df["EXPENSES"] = df["EXPENSES"].fillna(0)
    df["LEGAL_COSTS"] = df["LEGAL_COSTS"].fillna(0)
    df["MAINTENANCE_AND_PRESERVATION_COSTS"] = df[
        "MAINTENANCE_AND_PRESERVATION_COSTS"
    ].fillna(0)
    df["TAXES_AND_INSURANCE"] = df["TAXES_AND_INSURANCE"].fillna(0)
    df["MISCELLANEOUS_EXPENSES"] = df["MISCELLANEOUS_EXPENSES"].fillna(0)
    df["ACTUAL_LOSS_CALCULATION"] = df["ACTUAL_LOSS_CALCULATION"].fillna(0)
    df["MODIFICATION_COST"] = df["MODIFICATION_COST"].fillna(0)

    return df


def perf_change_data_type(df):
    # Change the data types for all column
    # df[["CURRENT_LOAN_DELINQUENCY_STATUS"]] = df[
    #     ["CURRENT_LOAN_DELINQUENCY_STATUS"]
    # ].astype("int")
    # df[['loan_age','mths_remng','cd_zero_bal','non_int_brng_upb','delq_sts','actual_loss']] = df[['loan_age','mths_remng','cd_zero_bal','non_int_brng_upb','delq_sts','actual_loss']].astype('int64')
    # df[['svcg_cycle','dt_zero_bal','dt_lst_pi']] = df[['svcg_cycle','dt_zero_bal','dt_lst_pi']].astype('str')
    return df


def perf_mutate_data(df):
    df["QUARTER"] = [x[4:5] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))]
    df["YEAR"] = [x[1:3] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))]
    df["LOAN_SEQUENCE_NUMBER"] = [
        x[5:] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))
    ]

    # replacing REO Acquisition with the value 99
    df["REO_ACQUISITION"] = df["CURRENT_LOAN_DELINQUENCY_STATUS"].map(
        lambda x: 1 if (x == "RA") else 0
    )
    df["CURRENT_LOAN_DELINQUENCY_STATUS"] = df["CURRENT_LOAN_DELINQUENCY_STATUS"].map(
        lambda x: x if (x != "RA") else 99
    )

    df_length = (
        df.groupby("LOAN_SEQUENCE_NUMBER")["LOAN_AGE"]
        .count()
        .rename("ORIGINAL_TOTAL_OBSERVED_LENGTH")
    )
    df = pd.merge(df, df_length, on="LOAN_SEQUENCE_NUMBER", how="left")

    return df


# Create a data frame for all Origination files
def create_origination_from_raw_data(input_filenames, headers, data_types, output_filename):

    annual_dataset_elements = tqdm(glob.glob(input_filenames))
    first_annual_dataset_elements = True

    with open(output_filename, "w", encoding="utf-8", newline="") as file:
        for filename in annual_dataset_elements:
            annual_dataset_elements.set_description("Working on  %s" % filename)
            orig_df = pd.read_csv(
                filename,
                sep="|",
                names=headers,
                dtype=data_types,
                skipinitialspace=True,
            )

            orig_df = orig_fill_NAN(orig_df)
            orig_df = orig_change_data_type(orig_df)
            orig_df = orig_mutate_data(orig_df)

            orig_df.to_csv(
                output_filename,
                mode="a",
                header=first_annual_dataset_elements,
                index=False,
            )
            first_annual_dataset_elements = False


def create_performance_from_raw_data(input_filenames, headers, data_types, output_filename):
    annual_dataset_elements = tqdm(glob.glob(input_filenames))
    first_annual_dataset_elements = True

    with open(output_filename, "w", encoding="utf-8", newline="") as file:
        for filename in annual_dataset_elements:
            annual_dataset_elements.set_description("Working on  %s" % filename)
            perf_df = pd.read_csv(
                filename,
                sep="|",
                names=headers,
                dtype=data_types,
                skipinitialspace=True,
            )

            perf_df = perf_fill_NAN(perf_df)
            perf_df = perf_mutate_data(perf_df)
            perf_df = perf_change_data_type(perf_df)

            perf_df.to_csv(
                output_filename,
                mode="a",
                header=first_annual_dataset_elements,
                index=False,
            )
            first_annual_dataset_elements = False
