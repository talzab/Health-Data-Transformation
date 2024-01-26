import pandas as pd
import numpy as np
import psycopg
import time
from credentials import DB_USER, DB_PASSWORD


def check_duplicate_id(curr, table_name, column_name, value, column_name2 = None, value2 = None):
    """
    Check if there is a duplicate entry in the specified table based on the provided column(s) and value(s).

    Parameters:
    - curr: psycopg cursor
    - table_name: str, name of the table to check for duplicates
    - column_name: str, name of the column to check for duplicates
    - value: value to check for in the specified column
    - column_name2: str (optional), name of the second column to check for duplicates (default is None)
    - value2: value (optional), value to check for in the second column (default is None)

    Returns:
    - bool, True if a duplicate entry exists, False otherwise
    """
    if column_name2 is None:
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = %s"
        curr.execute(query, (value,))
    else:
        value2 = str(value2)
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = %s AND {column_name2} = %s"
        curr.execute(query, (value, value2))

    count = curr.fetchone()[0]
    return count > 0


def load_hhs_data(csv_file, conn):
    """
    Load HHS (Health and Human Services) data from a CSV file into a PostgreSQL database.

    Parameters:
    - csv_file: str, path to the CSV file containing HHS data
    - conn: psycopg connection, connection to the PostgreSQL database

    Raises:
    - ValueError: If data loading fails
    """
    df = pd.read_csv(csv_file)

    # Do necessary processing on the DataFrame (e.g., handle -999 values, parse dates)
    df.replace(-999999, None, inplace=True) 
    df.replace({np.nan: None}, inplace=True)

    df = df.astype(float, errors='ignore')
    df['collection_week'] = pd.to_datetime(df['collection_week'], format = '%Y-%m-%d').dt.date

    hospitals_success_count = 0
    hospitals_error_count = 0

    hospitallocation_success_count = 0
    hospitallocation_error_count = 0

    hospitalbedinformation_success_count = 0
    hospitalbedinformation_error_count = 0

    invalid_row_ind = []
    start_time = time.time()
    try:
        # Create a cursor and open a transaction
        with conn.cursor() as curr:
            # Insert data into the database
            for index, row in df.iterrows():
                # print(f"Processing row {index}")

                try:
                    # Insert into Hospitals table
                    if not check_duplicate_id(curr, 'Hospitals', 'hospital_pk', row['hospital_pk']):
                        curr.execute(''' INSERT INTO Hospitals (hospital_pk, hospital_name) VALUES (%s, %s)''', 
                            (row['hospital_pk'], row['hospital_name']))
                        hospitals_success_count += 1
                    else:
                        print(f"Skipping row {index} due to duplicate ID in row: {row['hospital_pk']}")
                        hospitals_error_count += 1
                        invalid_row_ind.append(index)
                except Exception as e:
                    print(f"Error inserting data into Hospitals for row {index}: {e}")
                    invalid_row_ind.append(index)

                try:
                    # Insert into HospitalLocations table
                    if not check_duplicate_id(curr, 'HospitalLocations', 'hospital_fk', row['hospital_pk']):
                        curr.execute(''' INSERT INTO HospitalLocations (hospital_fk, state, address, city, zip, fips_code, geocoded_hospital_address) VALUES (%s, %s, %s, %s, %s, %s, %s)''', 
                            (row['hospital_pk'], row['state'], row['address'], row['city'], row['zip'], row['fips_code'], row['geocoded_hospital_address']))
                        hospitallocation_success_count += 1
                    else: 
                        print(f"Skipping row {index} due to duplicate ID in row: {row['hospital_pk']}")
                        hospitallocation_error_count += 1
                        invalid_row_ind.append(index)
                except Exception as e:
                    print(f"Error inserting data into HospitalLocations for row {index}: {e}")
                    invalid_row_ind.append(index)

                try:
                    # Insert into HospitalBedInformation table
                    if not check_duplicate_id(curr, 'HospitalBedInformation', 'hospital_fk', row['hospital_pk'], 'collection_week', row['collection_week']):
                        if (row['all_adult_hospital_beds_7_day_avg'] is not None) and (int(row['all_adult_hospital_beds_7_day_avg']) < 0):
                            print(f"Skipping row {index} due to 'all_adult_hospital_beds_7_day_avg' less than 0")
                            invalid_row_ind.append(index)
                        elif (row['all_pediatric_inpatient_beds_7_day_avg'] is not None) and (int(row['all_pediatric_inpatient_beds_7_day_avg']) < 0):
                            print(f"Skipping row {index} due to 'all_pediatric_inpatient_beds_7_day_avg' less than 0")
                            invalid_row_ind.append(index)
                        elif (row['all_adult_hospital_inpatient_bed_occupied_7_day_coverage'] is not None) and (int(row['all_adult_hospital_inpatient_bed_occupied_7_day_coverage']) < 0):
                            print(f"Skipping row {index} due to 'all_adult_hospital_inpatient_bed_occupied_7_day_coverage' less than 0")
                            invalid_row_ind.append(index)
                        elif (row['all_pediatric_inpatient_bed_occupied_7_day_avg'] is not None) and (int(row['all_pediatric_inpatient_bed_occupied_7_day_avg']) < 0):
                            print(f"Skipping row {index} due to 'all_pediatric_inpatient_bed_occupied_7_day_avg' less than 0")
                            invalid_row_ind.append(index)
                        elif (row['total_icu_beds_7_day_avg'] is not None) and (int(row['total_icu_beds_7_day_avg']) < 0):
                            print(f"Skipping row {index} due to 'total_icu_beds_7_day_avg' less than 0")
                            invalid_row_ind.append(index)
                        elif (row['icu_beds_used_7_day_avg'] is not None) and (int(row['icu_beds_used_7_day_avg']) < 0):
                            print(f"Skipping row {index} due to 'icu_beds_used_7_day_avg' less than 0")
                            invalid_row_ind.append(index)
                        elif (row['inpatient_beds_used_covid_7_day_avg']is not None) and (int(row['inpatient_beds_used_covid_7_day_avg']) < 0):
                            print(f"Skipping row {index} due to 'inpatient_beds_used_covid_7_day_avg' less than 0")
                            invalid_row_ind.append(index)
                        elif (row['staffed_icu_adult_patients_confirmed_covid_7_day_avg'] is not None) and (int(row['staffed_icu_adult_patients_confirmed_covid_7_day_avg']) < 0):
                            print(f"Skipping row {index} due to 'staffed_icu_adult_patients_confirmed_covid_7_day_avg' less than 0")
                            invalid_row_ind.append(index)
                        else:
                            curr.execute(''' INSERT INTO HospitalBedInformation (hospital_fk, collection_week, all_adult_hospital_beds_7_day_avg, all_pediatric_inpatient_beds_7_day_avg, 
                                all_adult_hospital_inpatient_bed_occupied_7_day_coverage, all_pediatric_inpatient_bed_occupied_7_day_avg, 
                                total_icu_beds_7_day_avg, icu_beds_used_7_day_avg, inpatient_beds_used_covid_7_day_avg, 
                                staffed_icu_adult_patients_confirmed_covid_7_day_avg)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
                            (row['hospital_pk'], row['collection_week'], row['all_adult_hospital_beds_7_day_avg'], row['all_pediatric_inpatient_beds_7_day_avg'],
                            row['all_adult_hospital_inpatient_bed_occupied_7_day_coverage'], row['all_pediatric_inpatient_bed_occupied_7_day_avg'],
                            row['total_icu_beds_7_day_avg'], row['icu_beds_used_7_day_avg'], row['inpatient_beds_used_covid_7_day_avg'],
                            row['staffed_icu_adult_patients_confirmed_covid_7_day_avg']))
                            hospitalbedinformation_success_count += 1
                    else: 
                        print(f"Skipping row {index} due to duplicate ID and date in row: {row['hospital_pk']}, {row['collection_week']}")
                        hospitalbedinformation_error_count += 1
                        invalid_row_ind.append(index)
                except Exception as e:
                    print(f"Error inserting data into HospitalBedInformation for row {index}: {e}")
                    invalid_row_ind.append(index)

            # Commit the changes
            conn.commit()
            end_time = time.time()
            print(end_time - start_time)
            
            # Write out csv file that includes original rows that are invalid
            with open("invalid_data/hhs.csv", "w", encoding="utf-8") as f:
                orig_df = pd.read_csv(csv_file, dtype=object)
                f.write(orig_df.iloc[invalid_row_ind].to_csv(index=False, lineterminator='\r'))

            print("Data loaded successfully.")
            print(f"Total rows processed: {len(df)}")
            print(f"Successful Hospitals inserts: {hospitals_success_count}, Errors: {hospitals_error_count}")
            print(f"Successful HospitalLocations inserts: {hospitallocation_success_count}, Errors: {hospitallocation_error_count}")
            print(f"Successful HospitalBedInformation inserts: {hospitalbedinformation_success_count}, Errors: {hospitalbedinformation_error_count}")

    except Exception as e:
        print(f"Error: {e}")
        # Rollback the transaction in case of an error
        conn.rollback()
        print("Data loading failed.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python load_hhs.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]

    try:
        with psycopg.connect(
                host="pinniped.postgres.database.azure.com",
                dbname=DB_USER,
                user=DB_USER,
                password=DB_PASSWORD
        ) as conn:
            load_hhs_data(csv_file, conn)
    except ValueError as ve:
        print(ve)
    finally:
        # Close the database connection
        if conn:
            conn.close()
