# This file is to import the CSV files that contain the transactions (activity) and credit (request fact) info, while making necessary changes
# CSV files are pulled from an SFTP connection (localhost used as an example) and then inserted into tables using a MySQL connection
# by Ryan Baldwin, last modified 1/6/26
import os
import pandas as pd
from sqlalchemy import create_engine, text
import paramiko
import csv
import io # uses io module to treat the SFTP file object as a text file

# SFTP connection variables
hostname = 'Ryans-MacBook-Air-125.local'
port = 22
username = 'ryanbaldwin'
private_key_path = '/Users/ryanbaldwin/.ssh/id_ed25519'
private_key_passphrase = None # no passphrase, just a key file
remote_file_path1 = '/Users/ryanbaldwin/TestFiles/VN_Activity.csv'
remote_file_path2 = '/Users/ryanbaldwin/TestFiles/VN_Request_Fact.csv'

# MySQL connection variables
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "new_user"
DB_PASS = "user_password"
DB_NAME = "Dining"

def upsert_dataframe_mysql(engine, df, target_table, key_cols): # function that is used later to update/insert data into table
    if df.empty:
        return

    with engine.begin() as conn:
        table_cols = [r[0] for r in conn.execute(text(f"SHOW COLUMNS FROM `{target_table}`")).fetchall()]
        df2 = df[[c for c in df.columns if c in table_cols]].copy()

        missing_keys = [k for k in key_cols if k not in df2.columns]
        if missing_keys:
            raise ValueError(f"{target_table}: missing key cols after alignment: {missing_keys}")

        all_cols = list(df2.columns)
        update_cols = [c for c in all_cols if c not in key_cols]

        staging_table = f"_stg_{target_table}"
        df2.to_sql(staging_table, con=conn, if_exists="replace", index=False)

        cols_csv = ", ".join(f"`{c}`" for c in all_cols)
        updates_csv = ", ".join(f"`{c}` = new.`{c}`" for c in update_cols) if update_cols else f"`{key_cols[0]}` = new.`{key_cols[0]}`"

        sql = f"""
        INSERT INTO `{target_table}` ({cols_csv})
        SELECT {cols_csv}
        FROM `{staging_table}` AS new
        ON DUPLICATE KEY UPDATE {updates_csv};
        """

        conn.execute(text(sql))
        conn.execute(text(f"DROP TABLE `{staging_table}`;"))


# connects to server and inputs the CSV files into dataframes
try:
    # Load private key
    pkey = paramiko.Ed25519Key.from_private_key_file(
    private_key_path,
    password=private_key_passphrase
)

    # establish SSH connection
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, pkey=pkey)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # open the remote CSV file and read directly into a pandas dataframe
    with sftp.open(remote_file_path1, mode='r') as remote_file1:
        remote_file1.prefetch() 
        df1 = pd.read_csv(remote_file1)
        # changes being made to the dataframe to match the SQL table
        df1 = df1.drop(columns=["key"], errors="ignore")
        df1["original_amount"] = df1["amount"]
        df1["altered_amount"] = df1["amount"]
        df1 = df1.drop(columns=["amount"])
        for col in ["original_amount", "altered_amount"]:
            if col in df1.columns:
                df1[col] = pd.to_numeric(df1[col], errors="coerce")
        for col in ["created_at", "updated_at", "expired_at"]:
            if col in df1.columns:
                df1[col] = pd.to_datetime(df1[col], errors="coerce")
        # display the dataframe
        print(df1.head())

    with sftp.open(remote_file_path2, mode='r') as remote_file2:
        remote_file2.prefetch() 
        df2 = pd.read_csv(remote_file2)
        # changes being made to the dataframe to match the SQL table
        df2["original_VC_amount"] = df2["VC_amount"]
        df2["altered_VC_amount"] = df2["VC_amount"]
        df2 = df2.drop(columns=["VC_amount"])
        for col in ["original_VC_amount", "altered_VC_amount"]:
            if col in df2.columns:
                df2[col] = pd.to_numeric(df2[col], errors="coerce")
        for col in ["created_at", "updated_at", "expired_at"]:
            if col in df2.columns:
                df2[col] = pd.to_datetime(df2[col], errors="coerce")
        # display the dataframe
        print(df2.head())

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # ensure connections are closed
    if sftp:
        sftp.close()
    if transport:
        transport.close()

try:
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        pool_pre_ping=True,
    )

    upsert_dataframe_mysql(
    engine,
    df1,
    target_table="vn_transactions",
    key_cols=["transactions_id", "transactions_uuid"],
)

    upsert_dataframe_mysql(
    engine,
    df2,
    target_table="vn_credit",
    key_cols=["VC_key"],
)

    print("Upserts completed successfully.")

except Exception as e:
    print(f"MySQL error: {e}")
    raise
