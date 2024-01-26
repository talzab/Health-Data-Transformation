## Overview
This project includes Python scripts designed to load data into an SQL database. The scripts are designed for two datasets: Weekly updates from HHS (Health and Human Services) and CMS (Centers for Medicare & Medicaid Services) quality data. The README provides instructions on using these scripts and outlines their functionality.

## Requirements
- Python 3.x
- An SQL database (e.g., MySQL, PostgreSQL)

## Installation
1. Clone this repository to your local machine:
```
git clone https://github.com/your_username/team_aragon.git
cd team_aragon
```
2. Install the required Python packages:
```
pip install psycopg
pip install tdqm
```
## Usage

### Weekly Updates (HHS Data)

To load the HHS data, use the following command:

```
python load_hhs.py <file_name>
```
Replace <file_name> with the name of the CSV file containing the weekly updates.

For example: 
```
python load_hhs.py 2022-01-04-hhs-data.csv
```

### Quality Data (CMS Data)

To load the CMS data, use the following command:
```
python load_quality.py <date> <file_name>
```
Replace <file_name> with the name of the CSV file containing the quality data.

For example:
```
python load_quality.py 2021-07-01 Hospital_General_Information-2021-07.csv
```

### Running the Pipeline 

To run the automatic reporting pipeline, use the following command:
```
streamlit run Reporting.py
```
Then, copy and paste the localhost into your browser.
