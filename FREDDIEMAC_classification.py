import dask.dataframe as dd


def assign_labels_to_orig(df_orig, df_perf):
    # loan turning into 3 month delinquency
    df_perf["DEFAULT"] = df_perf["CURRENT_LOAN_DELINQUENCY_STATUS"].map(lambda x: 1 if (x > 2) else 0)
    df_default = df_perf.groupby("ORIGINAL_LOAN_SEQUENCE_NUMBER")["DEFAULT"].sum()
    df_default = df_default.map(lambda x: 1 if x > 0 else 0)
    df_orig = dd.merge(df_orig, df_default, on="ORIGINAL_LOAN_SEQUENCE_NUMBER", how="left")

    # loan either prepaying or matured? (Voluntary payoff)"?
    df_perf["PREPAYMENT_OR_MATURED"] = df_perf["ZERO_BALANCE_CODE"].map(lambda x: 1 if (x == "01") else 0)
    df_prepayment = df_perf.groupby("ORIGINAL_LOAN_SEQUENCE_NUMBER")["PREPAYMENT_OR_MATURED"].sum()
    df_prepayment = df_prepayment.map(lambda x: 1 if x > 0 else 0)
    df_orig = dd.merge(df_orig, df_prepayment, on="ORIGINAL_LOAN_SEQUENCE_NUMBER", how="left")

    # remaining months
    df_remaining_months = df_perf.groupby("ORIGINAL_LOAN_SEQUENCE_NUMBER")["REMAINING_MONTHS_TO_LEGAL_MATURITY"].min()
    df_orig = dd.merge(df_orig, df_remaining_months, on="ORIGINAL_LOAN_SEQUENCE_NUMBER", how="left")

    # create labels (0 -> prepay, 1 -> default, 2 -> full repay, 3 -> censored)
    def label_row(row):

        if row["PREPAYMENT_OR_MATURED"] == 1:
            if row["REMAINING_MONTHS_TO_LEGAL_MATURITY"] >= 2:
                return 0
            else:
                return 2

        if row["DEFAULT"] == 1:
            return 1

        else:
            return 3

    df_orig["LABEL"] = df_orig.apply(lambda row: label_row(row), axis=1)

    return df_orig


def assign_labels_to_perf(df_orig_with_label, df_perf):
    df = dd.merge(df_perf, df_orig_with_label[["ORIGINAL_LOAN_SEQUENCE_NUMBER", "LABEL"]], on="ORIGINAL_LOAN_SEQUENCE_NUMBER", how="left")
    return df


def specific_cutoff(sequence):
    # pretend there is only one sequence with prepayment

    if len(sequence.index) < 12:
        sequence["TOTAL_OBSERVED_LENGTH"] = len(sequence.index)
        sequence["TIME_TO_EVENT"] = len(sequence.index)
        return sequence

    # if prepay (label 0), cut of the last entry with zero-balance
    if sequence["LABEL"].head(1).item() == 0:
        sequence = sequence.iloc[:-1]
        sequence["TOTAL_OBSERVED_LENGTH"] = len(sequence.index)
        sequence["TIME_TO_EVENT"] = len(sequence.index)
        return sequence

    # if default (label 1), cut of right before the first time we see 90-day delinquency
    if sequence["LABEL"].head(1).item() == 1:
        resetted_sequence = sequence.reset_index(drop=True)
        first_occurence_index = resetted_sequence[resetted_sequence["CURRENT_LOAN_DELINQUENCY_STATUS"] > 2].index[0]
        
        if first_occurence_index > 0:
            sequence = sequence.iloc[:first_occurence_index - 1]
        else:
            sequence = sequence.iloc[:first_occurence_index]
        
        sequence["TOTAL_OBSERVED_LENGTH"] = len(sequence.index)
        sequence["TIME_TO_EVENT"] = len(sequence.index)
        return sequence

    # if full repay (label 2), cut of the last entry with zero-balance
    if sequence["LABEL"].head(1).item() == 2:
        sequence = sequence.iloc[:-1]
        sequence["TOTAL_OBSERVED_LENGTH"] = len(sequence.index)
        sequence["TIME_TO_EVENT"] = len(sequence.index)
        return sequence

    # if censored, do nothing
    if sequence["LABEL"].head(1).item() == 3:
        sequence["TOTAL_OBSERVED_LENGTH"] = len(sequence.index)
        sequence["TIME_TO_EVENT"] = len(sequence.index)
        return sequence

def cutoff_sequence_according_to_label(df_orig, df_perf_labeled):

    df_perf = df_perf_labeled.groupby('ORIGINAL_LOAN_SEQUENCE_NUMBER').apply(lambda x: specific_cutoff(x)).reset_index(drop=True)

    # remap the remaining months
    df_remaining_months = df_perf.groupby("ORIGINAL_LOAN_SEQUENCE_NUMBER")["REMAINING_MONTHS_TO_LEGAL_MATURITY"].min()
    df_orig = df_orig.rename(columns={"REMAINING_MONTHS_TO_LEGAL_MATURITY": "ORIGINAL_REMAINING_MONTHS_TO_LEGAL_MATURITY"})
    df_orig = dd.merge(df_orig, df_remaining_months, on="ORIGINAL_LOAN_SEQUENCE_NUMBER", how="left")

    df_perf_grouped = df_perf.groupby("ORIGINAL_LOAN_SEQUENCE_NUMBER")["TOTAL_OBSERVED_LENGTH"].apply(lambda x: x.iloc[0])
    df_orig = dd.merge(df_orig, df_perf_grouped, on="ORIGINAL_LOAN_SEQUENCE_NUMBER", how="left")

    df_perf_grouped = df_perf.groupby("ORIGINAL_LOAN_SEQUENCE_NUMBER")["TIME_TO_EVENT"].apply(lambda x: x.iloc[0])
    df_orig = dd.merge(df_orig, df_perf_grouped, on="ORIGINAL_LOAN_SEQUENCE_NUMBER", how="left")
    
    return df_orig, df_perf