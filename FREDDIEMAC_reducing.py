import dask.dataframe as dd

def reduce_length_of_sequence(df_perf, length):
    return (
        df_perf.groupby("ORIGINAL_LOAN_SEQUENCE_NUMBER")
        .apply(lambda x: x.iloc[-length:])
        .reset_index(drop=True)
    )

def drop_short_sequences(df_orig_labeled, df_perf_labeled, min_length):
    df_perf_too_short = df_perf_labeled[df_perf_labeled["TOTAL_OBSERVED_LENGTH"] < min_length]
    df_perf_too_short_index = df_perf_too_short.index

    df_orig_too_short = df_orig_labeled[df_orig_labeled["TOTAL_OBSERVED_LENGTH"] < min_length]
    df_orig_too_short_index = df_orig_too_short.index

    return df_orig_labeled.drop(df_orig_too_short_index), df_perf_labeled.drop(df_perf_too_short_index)

def select_specific_original_loan_term(df_orig, df_perf, loan_terms_list):
    df_orig = df_orig[df_orig["ORIGINAL_LOAN_TERM"].isin(loan_terms_list)]

    df_perf = dd.merge(df_perf, df_orig[["ORIGINAL_LOAN_SEQUENCE_NUMBER", "ORIGINAL_LOAN_TERM"]], on="ORIGINAL_LOAN_SEQUENCE_NUMBER", how="left")
    df_perf = df_perf[df_perf["ORIGINAL_LOAN_TERM"].isin(loan_terms_list)]

    return df_orig, df_perf