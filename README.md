# Influencer Campaign Metrics

This project automates the process of collecting and analyzing influencer campaign data from YouTube videos. It extracts data from a Google Sheet, enriches it with YouTube metrics, and stores the results in a Snowflake database.

## Assumptions

Based on the project requirements:

### Data Source Assumptions:

Google Sheet named "Influencer_Campaign_Data" is accessible and contains:

Influencer_Name
Video_Url
Campaign_name


The necessary credentials and connection details for Google Sheets and Snowflake are available as environment variables or in a configuration file
YouTube videos are publicly accessible for scraping metrics


### Technical Assumptions:

Airflow installation is available for DAG deployment
Sufficient permissions to create and modify tables in Snowflake
Network access to Google Sheets API and YouTube


### Data Processing Assumptions:

Daily updates are sufficient for the data pipeline
YouTube metrics (publish date and likes count) are available for all videos
The volume of data is manageable within standard API rate limits

## Setup

1. Clone this repository:

```bash
git clone https://github.com/eyalozeri23/youtube-influencers-project.git
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

3. Set up your Google Cloud project and create a service account with access to the Google Sheets API. Download the service account key JSON file and update the `service_account.json` file.


4. Update the `GOOGLE_SHEET_ID` in `source/config.py` if you're using a different Google Sheet.

## Running the Script

To run the script manually:

```bash
python source/main.py
```

## Airflow Integration

To run this script as an Airflow DAG:

1. Copy the `dags/influencer_metrics_dag.py` file to your Airflow DAGs folder.
2. Ensure that Airflow can access all the necessary files and dependencies.

The DAG is set to run daily and will execute the `run()` function from `source/main.py`.

## Project Structure

- `source/`: Contains the main Python modules
- `config/`: Contains configuration files
- `logs/`: Directory for log files
- `airflow/dags/`: Contains the Airflow DAG definition
- `tests/`: Contains unit tests (to be implemented)



## License

All rights reserved. Copyright © 2024 Eyal Ozeri.