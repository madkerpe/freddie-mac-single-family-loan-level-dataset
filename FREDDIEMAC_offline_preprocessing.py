from tqdm import tqdm
import glob
from pathlib import Path
import dask.dataframe as dd


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
    df["PRE-RELIEF_REFINANCE_LOAN_SEQUENCE_NUMBER"] = df["PRE-RELIEF_REFINANCE_LOAN_SEQUENCE_NUMBER"].fillna("X")
    df["RELIEF_REFINANCE_INDICATOR"] = df["RELIEF_REFINANCE_INDICATOR"].fillna("X")
    return df


def orig_change_data_type(df):
    # Change the data types for all column

    return df


def orig_mutate_data(df):
    df["ORIGINAL_LOAN_SEQUENCE_NUMBER"] = df["LOAN_SEQUENCE_NUMBER"].astype(str)
    df["QUARTER"] = [x[4:5] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))]
    df["YEAR"] = [x[1:3] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))]
    df["LOAN_SEQUENCE_NUMBER"] = [x[5:] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))]
    return df


def perf_fill_NAN(df):
    df["CURRENT_LOAN_DELINQUENCY_STATUS"] = df["CURRENT_LOAN_DELINQUENCY_STATUS"].fillna(0)
    df["DEFECT_SETTLEMENT_DATE"] = df["DEFECT_SETTLEMENT_DATE"].fillna("X")
    df["MODIFICATION_FLAG"] = df["MODIFICATION_FLAG"].fillna("N")
    df["ZERO_BALANCE_CODE"] = df["ZERO_BALANCE_CODE"].fillna(00)
    df["ZERO_BALANCE_EFFECTIVE_DATE"] = df["ZERO_BALANCE_EFFECTIVE_DATE"].fillna("189901")
    df["CURRENT_DEFERRED_UPB"] = df["CURRENT_DEFERRED_UPB"].fillna(0)
    df["DDLPI"] = df["DDLPI"].fillna("189901")
    df["MI_RECOVERIES"] = df["MI_RECOVERIES"].fillna(0)
    df["NET_SALE_PROCEEDS"] = df["NET_SALE_PROCEEDS"].fillna("U")
    df["NON_MI_RECOVERIES"] = df["NON_MI_RECOVERIES"].fillna(0)
    df["EXPENSES"] = df["EXPENSES"].fillna(0)
    df["LEGAL_COSTS"] = df["LEGAL_COSTS"].fillna(0)
    df["MAINTENANCE_AND_PRESERVATION_COSTS"] = df["MAINTENANCE_AND_PRESERVATION_COSTS"].fillna(0)
    df["TAXES_AND_INSURANCE"] = df["TAXES_AND_INSURANCE"].fillna(0)
    df["MISCELLANEOUS_EXPENSES"] = df["MISCELLANEOUS_EXPENSES"].fillna(0)
    df["ACTUAL_LOSS_CALCULATION"] = df["ACTUAL_LOSS_CALCULATION"].fillna(0)
    df["MODIFICATION_COST"] = df["MODIFICATION_COST"].fillna(0)

    return df


def perf_change_data_type(df):
    # Change the data types for all column

    return df


def perf_mutate_data(df):
    df["ORIGINAL_LOAN_SEQUENCE_NUMBER"] = df["LOAN_SEQUENCE_NUMBER"].astype(str)
    df["QUARTER"] = [x[4:5] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))]
    df["YEAR"] = [x[1:3] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))]
    df["LOAN_SEQUENCE_NUMBER"] = [x[5:] for x in (df["LOAN_SEQUENCE_NUMBER"].apply(lambda x: x))]

    # replacing REO Acquisition with the value 99
    df["REO_ACQUISITION"] = df["CURRENT_LOAN_DELINQUENCY_STATUS"].map(lambda x: 1 if (x == "RA") else 0)
    df["CURRENT_LOAN_DELINQUENCY_STATUS"] = df["CURRENT_LOAN_DELINQUENCY_STATUS"].map(lambda x: x if (x != "RA") else 99)

    df_length = (
        df.groupby("ORIGINAL_LOAN_SEQUENCE_NUMBER")["LOAN_SEQUENCE_NUMBER"]
        .count()
        .rename("ORIGINAL_TOTAL_OBSERVED_LENGTH")
    )
    df = dd.merge(df, df_length, on="ORIGINAL_LOAN_SEQUENCE_NUMBER", how="left")

    return df


def pipeline_from_raw_data(input_data_orig_file_name, 
                           input_data_perf_file_name,
                           headers_orig,
                           headers_perf,
                           data_types_orig, 
                           data_types_perf,
                           mutated_data_orig,
                           mutated_data_perf,
                           data_folder):

    annual_dataset_paths_orig = glob.glob(data_folder + input_data_orig_file_name)
    annual_dataset_paths_perf = glob.glob(data_folder + input_data_perf_file_name)
    annual_dataset_iterator = tqdm(zip(annual_dataset_paths_orig, annual_dataset_paths_perf))

    for path_orig, path_perf  in annual_dataset_iterator:
        annual_dataset_iterator.set_description("Working on %s and %s" % (Path(path_orig).stem, Path(path_perf).stem))

        annual_df_orig = dd.read_csv(
            path_orig,
            sep="|",
            names=headers_orig,
            dtype=data_types_orig,
            skipinitialspace=True)
        annual_df_orig = annual_df_orig.astype(data_types_orig)
        

        annual_df_perf = dd.read_csv(
            path_perf,
            sep="|",
            names=headers_perf,
            dtype=data_types_perf,
            skipinitialspace=True,)
        annual_df_perf = annual_df_perf.astype(data_types_perf)


        annual_df_orig = orig_fill_NAN(annual_df_orig)
        annual_df_orig = orig_change_data_type(annual_df_orig)
        annual_df_orig = orig_mutate_data(annual_df_orig)
        annual_df_orig = annual_df_orig.astype(mutated_data_orig)

        annual_df_perf = perf_fill_NAN(annual_df_perf)
        annual_df_perf = perf_mutate_data(annual_df_perf)
        annual_df_perf = perf_change_data_type(annual_df_perf)
        annual_df_perf = annual_df_perf.astype(mutated_data_perf)
        
        annual_df_orig.to_parquet(Path(data_folder) / (str(Path(path_orig).stem) + ".parquet.gzip"), compression="gzip")
        annual_df_perf.to_parquet(Path(data_folder) / (str(Path(path_perf).stem) + ".parquet.gzip"), compression="gzip")