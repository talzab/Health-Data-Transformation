import streamlit as st
import psycopg
import pandas as pd
import matplotlib.pyplot as plt

# Database connection
conn = psycopg.connect(
    host="pinniped.postgres.database.azure.com", dbname="talzaben",
    user="talzaben", password='klVgh!KCGA'
)

# Set page configuration
st.set_page_config(
    page_title="Hospital Analytics and Reporting",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.set_option('deprecation.showPyplotGlobalUse', False)
selected_week = st.selectbox("Select a Week", ["2022-09-23", "2022-09-30", "2022-10-07", "2022-10-14", "2022-10-21"])

# Function to display records loaded in a specified week and comparison with previous weeks
def display_weekly_records():

    st.subheader('Hospital Records Loaded in a Specified Week and Comparison with Previous Weeks')
    cursor = conn.cursor()
    # Query for records in the specified week
    cursor.execute(
        "SELECT COUNT(*) FROM hospitalBedInformation WHERE collection_week = %s", 
        (selected_week,)
    )
    records_at_week = cursor.fetchone()[0]

    # Query for records in previous weeks
    cursor.execute(
        """
        SELECT collection_week, COUNT(*) FROM hospitalBedInformation WHERE collection_week < %s
        GROUP BY collection_week ORDER BY collection_week
        """,(selected_week,))
    records_previous_weeks = cursor.fetchall()

    st.write(f"Hospital records loaded in the specified week: {records_at_week}")
    st.write("\nHospital records loaded in the previous week(s):")

    if not records_previous_weeks: 
        st.write("There are no records for previous weeks.")
    for week, count in records_previous_weeks:
        st.write(f"{week}: {count}")

# Function to summarize the number of beds available and used in a specified week and compared to the 4 most recent weeks
def display_bed_statistics():

    st.subheader('Summarizing the Number of Adult and Pediatric Beds Available That Week, the Number Used, and the Number Used by Patients with COVID, Compared to the 4 Most Recent Weeks')

    cursor = conn.cursor()

    # Query for statistics in the specified week
    cursor.execute(
        "SELECT ROUND(sum(all_adult_hospital_beds_7_day_avg)::numeric, 2), \
                ROUND(sum(all_pediatric_inpatient_beds_7_day_avg)::numeric, 2), \
                ROUND(sum(all_adult_hospital_inpatient_bed_occupied_7_day_coverage)::numeric, 2), \
                ROUND(sum(all_pediatric_inpatient_bed_occupied_7_day_avg)::numeric, 2), \
                ROUND(sum(inpatient_beds_used_covid_7_day_avg)::numeric, 2) \
                FROM hospitalBedInformation WHERE collection_week = %s",
        (selected_week,)
    )
    week_stats = cursor.fetchone()

    if week_stats is None:
        st.write("No statistics found for the specified week.")
        return

    values = {
        'Available Adult Beds': [week_stats[0]], 
        'Available Pediatric Beds': [week_stats[1]], 
        'Used Adult Beds': [week_stats[2]], 
        'Used Pediatric Beds': [week_stats[3]], 
        'Used Beds by Patients with COVID': [week_stats[4]]
    }
    dataframe = pd.DataFrame(data=values)

    # Query for statistics in the 4 most recent weeks
    cursor.execute(
       """
       SELECT collection_week,
              ROUND(sum(all_adult_hospital_beds_7_day_avg)::numeric, 2),
              ROUND(sum(all_pediatric_inpatient_beds_7_day_avg)::numeric, 2),
              ROUND(sum(all_adult_hospital_inpatient_bed_occupied_7_day_coverage)::numeric, 2),
              ROUND(sum(all_pediatric_inpatient_bed_occupied_7_day_avg)::numeric, 2),
              ROUND(sum(inpatient_beds_used_covid_7_day_avg)::numeric, 2)
       FROM hospitalBedInformation
       GROUP BY collection_week 
       ORDER BY collection_week DESC LIMIT 4
       """
    )
    recent_weeks_stats = cursor.fetchall()

    columns = ['Week', 'Available Adult Beds', 'Available Pediatric Beds', 'Used Adult Beds', 'Used Pediatric Beds', 'Used Beds by Patients with COVID']
    dataframe2 = pd.DataFrame(recent_weeks_stats, columns=columns)

    st.write(f"\nSummary for {selected_week}:")
    st.dataframe(dataframe)

    st.write(f"\nSummary for the 4 most recent weeks:")
    st.dataframe(dataframe2.sort_values('Week'))

# Function to display hospital quality ratings and fraction of beds in use
def display_quality_ratings():

    st.subheader('Hospital Quality Ratings and Fraction of Beds in Use')
    cursor = conn.cursor()

    # Query for hospital quality ratings
    cursor.execute(
        """  
        SELECT hqi.hospital_overall_rating,
        SUM(hbi.all_adult_hospital_inpatient_bed_occupied_7_day_coverage + hbi.all_pediatric_inpatient_bed_occupied_7_day_avg) /
        SUM(hbi.all_adult_hospital_beds_7_day_avg + hbi.all_pediatric_inpatient_beds_7_day_avg) as fraction_of_beds_in_use
        FROM HospitalQualityInformation hqi
        JOIN HospitalBedInformation hbi ON hqi.facility_id = hbi.hospital_fk
        GROUP BY hqi.hospital_overall_rating
        """
    )

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.bar(df['hospital_overall_rating'], df['fraction_of_beds_in_use'], color='skyblue')
    plt.title('Fraction of Beds in Use by Hospital Quality Rating')
    plt.xlabel('Hospital Quality Rating')
    plt.ylabel('Fraction of Beds in Use')
    st.pyplot()

# Function to display total hospital beds used per week, inclusive of all cases and COVID cases
def display_total_bed_usage():

    st.subheader('Total Hospital Beds Used per Week, Inclusive of All Cases and COVID Cases')
    cursor = conn.cursor()

    # Query for total hospital beds used
    cursor.execute(
        """
        SELECT collection_week,
                SUM(all_adult_hospital_inpatient_bed_occupied_7_day_coverage + all_pediatric_inpatient_bed_occupied_7_day_avg + icu_beds_used_7_day_avg) as all_cases,
                SUM(inpatient_beds_used_covid_7_day_avg) as covid_cases
         FROM hospitalBedInformation WHERE collection_week <= %s 
         GROUP BY collection_week 
         ORDER BY collection_week 
         """, (selected_week, )
    )

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)

    # Plotting
    plt.figure(figsize=(12, 6))
    plt.plot(df['collection_week'], df['all_cases'], label='Total Beds Used', marker='o', color='lightblue')
    plt.title('Total Hospital Beds Used per Week (All Cases)')
    plt.xlabel('Collection Week')
    plt.ylabel('Number of Beds')
    plt.xticks(rotation=45)
    plt.legend()
    st.pyplot()

    plt.figure(figsize=(12, 6))
    plt.plot(df['collection_week'], df['covid_cases'], label='COVID Beds Used', marker='o', color='lightgreen')
    plt.title('Total Hospital Beds Used per Week (COVID Cases)')
    plt.xlabel('Collection Week')
    plt.ylabel('Number of Beds')
    plt.xticks(rotation=45)
    plt.legend()
    st.pyplot()

def emergency_services_comparison():

    st.subheader('Comparison of Emergency Services Availability in the Top 20 States')
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT hl.state, COUNT(*) AS count
        FROM HospitalQualityInformation hq
        JOIN Hospitals h ON hq.facility_id = h.hospital_pk
        JOIN HospitalLocations hl ON h.hospital_pk = hl.hospital_fk
        WHERE hq.emergency_services = TRUE
        GROUP BY hl.state
        """
    )

    columns = [desc[0] for desc in cursor.description]
    emergency_services_df = pd.DataFrame(cursor.fetchall(), columns = columns)

    top_20_states_df = emergency_services_df.nlargest(20, 'count')

    plt.figure(figsize = (12, 6))
    plt.bar(top_20_states_df["state"], top_20_states_df["count"], color = 'lightcyan')
    plt.title("Top 20 States with the Highest Availability of Emergency Services")
    plt.xlabel("State")
    plt.ylabel("Number of Hospitals with Emergency Services")
    plt.xticks(rotation = 45, ha='right')
    st.pyplot()


def bed_usage_by_ownership():

    st.subheader('Bed Usage Based on Hospital Ownership Type')
    selected_owner = st.selectbox("Select a Hospital Ownership", ['Government - Federal', 'Government - Hospital District or Authority', 'Government - Local', 'Government - State', 'Proprietary'])

    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT hq.hospital_ownership, hbi.collection_week, SUM(hbi.all_adult_hospital_inpatient_bed_occupied_7_day_coverage + hbi.all_pediatric_inpatient_bed_occupied_7_day_avg) / SUM(hbi.all_adult_hospital_beds_7_day_avg + hbi.all_pediatric_inpatient_beds_7_day_avg) as fraction_of_beds_in_use
        FROM HospitalQualityInformation hq
        JOIN HospitalBedInformation hbi ON hq.facility_id = hbi.hospital_fk
        WHERE hq.hospital_ownership = %s
        GROUP BY hq.hospital_ownership, hbi.collection_week
        ORDER BY hbi.collection_week
        """, (selected_owner, ))
        
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)

    # Plotting the data
    plt.figure(figsize=(10, 6))
    plt.plot(df['collection_week'], df['fraction_of_beds_in_use'], color = 'violet')
    plt.title('Fraction of Beds in Use by Hospital Ownership')
    plt.xlabel('Collection Week')
    plt.ylabel('Fraction of Beds in Use')
    plt.xticks(rotation = 45, ha='right')
    st.pyplot() 

def top_and_bottom_rating():

    st.subheader('Top and Bottom 10 Hospitals Based on Overall Hospital Rating')
    selected_week2 = st.selectbox("Select a Week", ["2021-07-01", "2022-01-01", "2022-10-01"])

    cursor = conn.cursor()

    # Execute a query to count records for the previous weeks
    cursor.execute(
        """
        SELECT q.hospital_overall_rating, l.state, q.data_date
        FROM hospitalqualityinformation as q
        INNER JOIN hospitallocations as l ON q.facility_id = l.hospital_fk
        WHERE q.data_date = %s
        """,
        (selected_week2,))
    quality_weeks = cursor.fetchall()     

    df = pd.DataFrame(quality_weeks, columns=["hospital_overall_rating", "state", "data_date"])
    df = df.dropna()

    df["hospital_overall_rating"] = pd.to_numeric(df["hospital_overall_rating"], errors='coerce')

    # Calculate the average hospital_overall_rating for each state
    avg_ratings = df.groupby("state")["hospital_overall_rating"].mean().sort_values(ascending=False)

    # Get the top 10 and bottom 10 states
    top_states = avg_ratings.head(10)
    bottom_states = avg_ratings.tail(10)

    # Scatter plot for the top 5 states
    plt.figure(figsize=(10, 6))
    plt.scatter(top_states.index, top_states.values, color='green', label='Top 10 States', s=100)
    plt.scatter(bottom_states.index, bottom_states.values, color='red', label='Bottom 10 States', s=100)
    plt.title(f'Average Hospital Overall Rating by State in week: {selected_week2}')
    plt.xlabel('State')
    plt.ylabel('Average Hospital Overall Rating')
    plt.legend()
    st.pyplot()

display_weekly_records()
display_bed_statistics()
display_quality_ratings()
display_total_bed_usage()
emergency_services_comparison()
bed_usage_by_ownership()
top_and_bottom_rating()