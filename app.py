import os
import json
import sys
import time
from tqdm import tqdm
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "petcolove-13c87f31f4c2.json"

# Initialize BigQuery client
bq_client = bigquery.Client()

# Initialize the Analytics Reporting API V4 client
credentials = service_account.Credentials.from_service_account_file(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    scopes=["https://www.googleapis.com/auth/analytics.readonly"]
)
analytics = build("analyticsreporting", "v4", credentials=credentials)

# Define the Dynamic Inputs

VIEW_ID = input(
    "Let's get mining! Enter your GA3 View ID, NOT Tracking ID, to Export to BQ: ").strip()
START_DATE = input(
    "Enter a Start Date for your dataset window (YYYY-MM-DD): ").strip()
END_DATE = input(
    "Enter the End Date for your dataset window (YYYY-MM-DD): ").strip()

# Define the GA3 requests for different tables
requests = {
    'user': {
        'viewId': VIEW_ID,
        'dateRanges': [{'startDate': START_DATE, 'endDate': END_DATE}],
        'metrics': [
            {'expression': 'ga:users'},
            {'expression': 'ga:newUsers'},
            {'expression': 'ga:percentNewSessions'}
        ],
        'dimensions': [
            {'name': 'ga:userType'},
            {'name': 'ga:sessionCount'},
            {'name': 'ga:daysSinceLastSession'},
            {'name': 'ga:userDefinedValue'},
            {'name': 'ga:userBucket'}
        ],
        'pageSize': 10000
    },
    'session': {
        'viewId': VIEW_ID,
        'dateRanges': [{'startDate': START_DATE, 'endDate': END_DATE}],
        'metrics': [
            {'expression': 'ga:sessions'},
            {'expression': 'ga:bounces'},
            {'expression': 'ga:bounceRate'},
            {'expression': 'ga:sessionDuration'},
            {'expression': 'ga:avgSessionDuration'}
        ],
        'dimensions': [
            {'name': 'ga:sessionDurationBucket'}
        ],
        'pageSize': 10000
    },
    'traffic_source': {
        'viewId': VIEW_ID,
        'dateRanges': [{'startDate': START_DATE, 'endDate': END_DATE}],
        'metrics': [
            {'expression': 'ga:organicSearches'}
        ],
        'dimensions': [
            {'name': 'ga:referralPath'},
            {'name': 'ga:fullReferrer'},
            {'name': 'ga:campaign'},
            {'name': 'ga:source'},
            {'name': 'ga:medium'},
            {'name': 'ga:keyword'}
        ],
        'pageSize': 10000
    }
    # Add more tables as needed
}


def fetch_report_data(raw_request):
    """
    Fetch data from Google Analytics API with pagination.
    """
    all_rows = []
    next_page_token = None
    report = None

    while True:
        if next_page_token:
            raw_request['pageToken'] = next_page_token

        try:
            response = analytics.reports().batchGet(  # pylint: disable=no-member
                body={'reportRequests': [raw_request]}).execute()
        except HttpError as err:
            print(f"An error occurred: {err}")
            exit()

        for report in response.get('reports', []):
            column_header = report.get('columnHeader', {})
            dimension_headers = column_header.get('dimensions', [])
            metric_headers = column_header.get(
                'metricHeader', {}).get('metricHeaderEntries', [])

            for row in report.get('data', {}).get('rows', []):
                row_dict = {}
                dimensions = row.get('dimensions', [])
                date_range_values = row.get('metrics', [])

                for header, dimension in zip(dimension_headers, dimensions):
                    row_dict[header] = dimension

                for values in date_range_values:
                    for metric_header, value in zip(metric_headers, values.get('values')):
                        row_dict[metric_header.get('name')] = value

                all_rows.append(row_dict)

        next_page_token = report.get('nextPageToken', None)
        if not next_page_token:
            break

    if report is not None:
        print(f"Fetched {len(all_rows)} rows from GA report.")
    else:
        print("No report data fetched.")
    return all_rows


def estimate_size(raw_data):
    """
    Estimate the size of the data in bytes.
    """
    json_data = json.dumps(raw_data)
    total_size_in_bytes = sys.getsizeof(json_data)
    return total_size_in_bytes


def estimate_costs(raw_size_in_bytes):
    """
    Estimate the storage and ingestion costs for the data.
    """
    size_in_gb = raw_size_in_bytes / (1024 ** 3)
    storage_cost_per_gb = 0.02  # $0.02 per GB per month for active storage
    ingestion_cost_per_gb = 0.01  # $0.01 per GB for data ingestion

    total_storage_cost = size_in_gb * storage_cost_per_gb
    total_ingestion_cost = 0 if size_in_gb <= 1 else (
        size_in_gb - 1) * ingestion_cost_per_gb

    return total_storage_cost, total_ingestion_cost


def generate_schema(schema_request):
    """
    Generate BigQuery schema based on the request dimensions and metrics.
    """
    new_schema = []
    for dimension in schema_request['dimensions']:
        new_schema.append(bigquery.SchemaField(dimension['name'], "STRING"))
    for metric in schema_request['metrics']:
        # Assuming metrics are numeric, but this can be adjusted as needed
        new_schema.append(bigquery.SchemaField(
            metric['expression'], "FLOAT64"))
    return new_schema


# Fetch the report data with progress bar
print("Fetching data from Google Analytics...")
for table, request in requests.items():
    data = fetch_report_data(request)
    print(f"Total rows fetched for {table} table: {len(data)}")

    # Estimate the size of the data
    size_in_bytes = estimate_size(data)
    size_in_mb = size_in_bytes / (1024 * 1024)
    print(f"Estimated size of the data for {table} table: {
          size_in_bytes} bytes ({size_in_mb:.2f} MB)")

    # Estimate the costs
    storage_cost, ingestion_cost = estimate_costs(size_in_bytes)
    print(f"Estimated storage cost per month for {
          table} table: ${storage_cost:.2f}")
    print(f"Estimated ingestion cost for {table} table: ${ingestion_cost:.2f}")

    # Prompt user for confirmation before uploading
    user_input = input(f"The estimated size of the data for {table} table is {size_in_mb:.2f} MB. "
                       f"Storage cost per month: ${
                           storage_cost:.2f}, Ingestion cost: ${ingestion_cost:.2f}. "
                       "Do you want to proceed with the upload? (yes/no): ").strip().lower()
    if user_input != 'yes':
        print(f"Upload canceled for {table} table by user.")
        continue

    # Generate the schema for the BigQuery table
    schema = generate_schema(request)

    # Define the BigQuery table schema
    TABLE_ID = f"chs-website-409300.UA_Backup.{table}_Data"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Load data into BigQuery with progress bar
    print(f"Uploading data to BigQuery for {table} table...")
    job = bq_client.load_table_from_json(data, TABLE_ID, job_config=job_config)
    with tqdm(total=100, desc=f"Uploading to BigQuery for {table} table") as pbar:
        while not job.done():
            pbar.update(1)
            # Sleep for a short while before checking the job status again
            time.sleep(1)
    job.result()  # Wait for the job to complete

    print(f"Data loaded into BigQuery successfully for {table} table.")
