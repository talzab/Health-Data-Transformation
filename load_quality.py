import pandas as pd
import numpy as np
import psycopg
import logging
import sys
from credentials import DB_USER, DB_PASSWORD
from logging_module import setup_logging


setup_logging()


def check_duplicate_ids(curr, table_name, facility_ids, date):
    """
    Check if there are duplicate entries for the given facility IDs and date
    in the specified table.

    Parameters:
    - curr: psycopg cursor
    - table_name: str, name of the table to check for duplicates
    - facility_ids: int list, facility IDs to check for duplicates
    - date: date, date to check for duplicates

    Returns:
    - list, facility IDs with duplicates
    """
    placeholders = ', '.join(['%s'] * len(facility_ids))
    query = f"SELECT facility_id FROM {table_name} WHERE facility_id IN ({placeholders}) AND data_date = %s"
    curr.execute(query, facility_ids + [date])
    duplicates = [row[0] for row in curr.fetchall()]
    return duplicates


def batch_insert_rows(curr, insert_query, rows, df):
    """
    Perform batch inserts for a list of rows.

    Parameters:
    - curr: psycopg cursor
    - insert_query: str, SQL insert query
    - rows: list of tuples, rows to be inserted
    - df: pandas df, original df that was inputted 

    Returns:
    - int, number of successfully inserted rows
    - int, number of failures for inserting rows
    - int list, list that keeps track of indicies of the failed rows
    """
    try:
        # Start a new transaction
        curr.execute('BEGIN')
        curr.executemany(insert_query, rows)
        # Commit the transaction
        curr.execute('COMMIT')
        return len(rows), 0, []

    except psycopg.Error:
        curr.execute('ROLLBACK')
        num_rows_inserted = 0
        num_rows_failed = 0
        batch_invalid_ind = []

        # Handle individual row insertions and log errors
        for row in rows:
            try:
                curr.execute('BEGIN')
                curr.execute(insert_query, row)
                curr.execute('COMMIT')
                num_rows_inserted += 1
            except psycopg.Error as row_error:
                num_rows_failed += 1
                index = (df == row).all(axis=1).idxmax()
                logging.error(f"Error inserting row {index} in the batch: {row_error}")
                # Rollback the transaction for the specific row
                curr.execute('ROLLBACK')
                batch_invalid_ind.append(index)

        return num_rows_inserted, num_rows_failed, batch_invalid_ind


def load_quality_data(csv_file, conn, date):
    """
    Load quality data from a CSV file into a PostgreSQL database.

    Rows that are either duplicates or that has invalid entries will be
    written out to a CSV file under ./invalid_dta/quality.csv.

    Parameters:
    - csv_file: str, path to the CSV file containing quality data
    - conn: psycopg connection, connection to the PostgreSQL database
    - date: str, date in 'YYYY-MM-DD' format

    Returns:
    - None, but logs how many rows have been successfully inserted and how many
      are not.
    """
    # Read csv file
    column_names = ["Facility ID", "Hospital overall rating", "Emergency Services", "Hospital Type", "Hospital Ownership"]
    df = pd.read_csv(csv_file, dtype=object, usecols=column_names)

    # Df pre-processing
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    df.replace({np.nan: None, 'Not Available': 0}, inplace=True)
    df['hospital_overall_rating'] = df['hospital_overall_rating'].astype(float)
    df['emergency_services'] = df['emergency_services'].replace({'Yes': True, 'No': False})
    date = pd.to_datetime(date).date()
    df['data_date'] = date

    num_rows_inserted = 0
    error_count = 0

    try:
        with conn.cursor() as curr:
            insert_query = '''
                INSERT INTO HospitalQualityInformation (facility_id, hospital_type, hospital_ownership, emergency_services, hospital_overall_rating, data_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            '''

            # Determine duplicates (by 'facility_id' and 'date').
            #  - indices of rows that are duplicates will be kept for later writing out to csv file
            #  - valid_df is the new df that we will look at, which DOES NOT include any duplicate rows
            facility_ids = df['facility_id'].tolist()
            duplicates = set(check_duplicate_ids(curr, 'HospitalQualityInformation', facility_ids, date))
            invalid_ind = df['facility_id'].isin(duplicates).index[df['facility_id'].isin(duplicates)].tolist()
            n_dups = len(invalid_ind)
            valid_df = df[~df['facility_id'].isin(duplicates)]
            rows_to_insert = [tuple(row) for row in valid_df.itertuples(index=False, name=None)]

            # Inserts 500 rows at once to SQL
            batch_size = 500
            for i in range(0, len(rows_to_insert), batch_size):
                batch = rows_to_insert[i:i + batch_size]
                n_success, n_fail, new_invalid_ind = batch_insert_rows(curr, insert_query, batch, df)
                num_rows_inserted += n_success
                error_count += n_fail
                invalid_ind.extend(new_invalid_ind)

            conn.commit()

            # Write out csv file that includes original rows that are invalid
            with open("invalid_data/quality.csv", "w", encoding="utf-8") as f:
                orig_df = pd.read_csv(csv_file, dtype=object)
                f.write(orig_df.iloc[invalid_ind].to_csv(index=False, lineterminator='\r'))

            logging.info("Data loaded successfully.")
            logging.info(f"{num_rows_inserted} successful inserts out of {len(df)}, errors: ({n_dups} duplicates, {error_count} errors)")

    except psycopg.Error as e:
        logging.error(f"Data loading failed due to PostgreSQL error: {e}")
        conn.rollback()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        logging.error("Usage: python load-quality.py <date> <csv_file>")
        sys.exit(1)

    date, csv_file = str(sys.argv[1]), str(sys.argv[2])

    try:
        with psycopg.connect(
            host="pinniped.postgres.database.azure.com",
            dbname=DB_USER,
            user=DB_USER,
            password=DB_PASSWORD
        ) as conn:
            load_quality_data(csv_file, conn, date)
    except psycopg.Error as e:
        logging.error(f"PostgreSQL error: {e}")
