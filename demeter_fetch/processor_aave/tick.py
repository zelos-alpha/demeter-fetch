import pandas as pd
import demeter_fetch.processor_aave.aave_utils as aave_utils


def preprocess_one(df: pd.DataFrame):
    df["tx_type"] = df.apply(lambda x: aave_utils.get_tx_type(x.topics), axis=1)
    append_columns = ["reserve", "owner", "amount", "liquidator", "debt_asset", "debt_amount", "atoken"]
    if df.empty:
        for column in append_columns:
            df[column] = None
    else:
        df[append_columns] = df.apply(
            lambda x: aave_utils.handle_event(x.tx_type, x.topics, x.DATA),
            axis=1,
            result_type="expand",
        )
    columns = df.columns.tolist() + append_columns
    if len(df.index) == 0:  # if empty
        df = df[columns]
        return df
    df["tx_type"] = df.apply(lambda x: x.tx_type.name, axis=1)
    df = df[columns]
    return df
