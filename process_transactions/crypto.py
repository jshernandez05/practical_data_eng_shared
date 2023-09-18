import pandas as pd

from practical_data_eng_shared.io.extract import extract_table
from practical_data_eng_shared.io.load import load_dataframe
from practical_data_eng_shared.io.eth import get_eth_price
from practical_data_eng_shared.utils import get_lookup_fn, hash_id
from practical_data_eng_shared.constants import TAX_RATE


def transform_crypto_transactions(df, products_df=None):
    if products_df is None:
        products_df = extract_table("products")

    sku_to_name = get_lookup_fn(products_df, from_col="sku", to_col="name")

    eth_price_df = get_eth_price()
    eth_price_df['Date'] = eth_price_df["Date"].astype('datetime64[ns]')
    eth_price_df.set_index('Date', inplace=True)

    transactions = []

    for i, row in df.iterrows():
        transaction = row[
            [
            "created_at",
            "location",
            "sku",
            "payment_method",
            "quantity",
            ]
        ].to_dict()

        transaction["transaction_id"] = hash_id(row["transaction_id"])
        transaction["product_name"] = sku_to_name(row["sku"])
        transaction["source"] = "crypto_sale"


        eth_price = eth_price_df.loc[row["created_at"].strftime("%Y-%m-%d")]["Open"]
        usd_total = eth_price * row["total"]
        transaction["total"] = round(usd_total, 2)
        transaction["tax"] = round(usd_total * TAX_RATE, 2)
        transaction["unit_price"] = round(usd_total / (1 + TAX_RATE) / row["quantity"], 2)

        transactions.append(transaction)

    transactions_df = pd.DataFrame(transactions)


    return transactions_df


def process_crypto_transactions():
    df = extract_table("crypto_transactions")
    df = transform_crypto_transactions(df)

    return df
    # load_dataframe(df)


# eth_price_df = pd.read_csv("ETH-USD.csv")
# eth_price_df['Date'] = eth_price_df["Date"].astype('datetime64[ns]')
# eth_price_df.set_index('Date', inplace=True)

# transactions = []

# for i, row in crypto_df.iterrows():
#     transaction = row[
#         [
#         "created_at",
#         "location",
#         "sku",
#         "payment_method",
#         "quantity",
#         ]
#     ].to_dict()

#     transaction["transaction_id"] = hash_id(row["transaction_id"])
#     # transaction["product_name"] = sku_to_name(row["sku"])
#     transaction["source"] = "crypto_sale"

#     print(row['created_at'])

#     eth_price = eth_price_df.loc[row["created_at"].strftime("%Y-%m-%d")]["Open"]
#     usd_total = eth_price * row["total"]
#     transaction["total"] = round(usd_total, 2)
#     transaction["tax"] = round(usd_total * TAX_RATE, 2)
#     transaction["unit_price"] = round(usd_total / (1 + TAX_RATE) / row["quantity"], 2)

#     transactions.append(transaction)

# transactions_df = pd.DataFrame(transactions)