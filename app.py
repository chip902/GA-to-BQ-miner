import os
import json
import sys
import time
import multiprocessing
from tqdm import tqdm
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Set the path to your service account key file
# "chs-website-409300-4302acfcf512.json"
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

VIEW_ID = '242734056'
# input("Let's get mining! Enter your GA3 View ID, NOT Tracking ID, to Export to BQ: ").strip()
START_DATE = input(
    "Enter a Start Date for your dataset window (YYYY-MM-DD): ").strip()
END_DATE = input(
    "Enter the End Date for your dataset window (YYYY-MM-DD): ").strip()
BQ_LOCATION = input("Enter the exact BigQuery Project Name: ").strip()

# Define the GA3 requests for different tables
requests = [
    {
        'events_1': {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:eventCategory"},
                {"name": "ga:eventAction"},
                {"name": "ga:eventLabel"},
                {"name": "ga:pagePath"},
                {"name": "ga:sourceMedium"}
            ],
            "metrics": [
                {"expression": "ga:totalEvents"},
                {"expression": "ga:uniqueEvents"}
            ],
            "pageSize": 100000
        },
    },
    {
        "events_2": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:eventCategory"},
                {"name": "ga:eventAction"},
                {"name": "ga:eventLabel"},
                {"name": "ga:pagePath"},
                {"name": "ga:sourceMedium"}
            ],
            "metrics": [
                {"expression": "ga:totalEvents"},
                {"expression": "ga:eventValue"}
            ],
            "pageSize": 100000
        }
    },
    {
        "session": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:sourceMedium"},
                {"name": "ga:channelGrouping"},
                {"name": "ga:keyword"},
                {"name": "ga:campaign"},
                {"name": "ga:adContent"},
                {"name": "ga:fullReferrer"}
            ],
            "metrics": [
                {"expression": "ga:sessions"},
            ],
            "pageSize": 100000
        }
    },
    {
        "new_users": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:sourceMedium"},
                {"name": "ga:channelGrouping"},
                {"name": "ga:keyword"},
                {"name": "ga:campaign"},
                {"name": "ga:adContent"},
                {"name": "ga:fullReferrer"}
            ],
            "metrics": [
                {"expression": "ga:newUsers"}
            ],
            "pageSize": 100000
        },
    },
    {
        "total_users": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:sourceMedium"},
                {"name": "ga:channelGrouping"},
                {"name": "ga:keyword"},
                {"name": "ga:campaign"},
                {"name": "ga:adContent"},
                {"name": "ga:fullReferrer"}
            ],
            "metrics": [
                {"expression": "ga:sessions"}
            ],
            "pageSize": 100000
        },
    },
    {
        "geo_1": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:country"},
                {"name": "ga:region"},
                {"name": "ga:city"},
                {"name": "ga:language"}
            ],
            "metrics": [
                {"expression": "ga:users"},
                {"expression": "ga:newUsers"},
                {"expression": "ga:sessions"}
            ],
            "pageSize": 100000
        },
    },
    {
        "geo_2": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:longitude"},
                {"name": "ga:latitude"}
            ],
            "metrics": [
                {"expression": "ga:users"},
                {"expression": "ga:newUsers"},
                {"expression": "ga:sessions"}
            ],
            "pageSize": 100000
        }
    },
    # {
    #     "demographics": {
    #         "viewId": VIEW_ID,
    #         "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
    #         "dimensions": [
    #             {"name": "ga:clientId"},
    #             {"name": "ga:date"},
    #             {"name": "ga:age"},
    #             {"name": "ga:gender"},
    #             {"name": "ga:region"},
    #             {"name": "ga:city"},
    #             {"name": "ga:browserLanguage"}
    #         ],
    #         "metrics": [
    #             {"expression": "ga:users"},
    #             {"expression": "ga:uniqueUsers"}
    #         ],
    #         "pageSize": 100000
    #     }
    # },
    {
        "user_types": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:userType"},
                {"name": "ga:sourceMedium"},
                {"name": "ga:pagePath"}
            ],
            "metrics": [
                {"expression": "ga:users"},
                {"expression": "ga:sessions"},
                {"expression": "ga:bounceRate"}
            ],
            "pageSize": 100000
        }
    },
    {
        "technology_1": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:browser"},
                {"name": "ga:operatingSystem"},
                {"name": "ga:screenResolution"},
                {"name": "ga:pagePath"}
            ],
            "metrics": [
                {"expression": "ga:users"},
                {"expression": "ga:sessions"},
                {"expression": "ga:bounceRate"}
            ],
            "pageSize": 100000
        }
    },
    {
        "technology_2": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:deviceCategory"},
                {"name": "ga:pagePath"},
                {"name": "ga:sourceMedium"},
                {"name": "ga:mobileDeviceInfo"}
            ],
            "metrics": [
                {"expression": "ga:sessions"},
                {"expression": "ga:newUsers"},
                {"expression": "ga:bounceRate"}
            ],
            "pageSize": 100000
        }
    },
    {
        "page_tracking_1": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:pagePath"},
                {"name": "ga:sourceMedium"}
            ],
            "metrics": [
                {"expression": "ga:bounceRate"},
                {"expression": "ga:pageviews"},
                {"expression": "ga:uniquePageviews"},
                {"expression": "ga:exits"},
                {"expression": "ga:entrances"}
            ],
            "pageSize": 100000
        }
    },
    {
        "page_tracking_2": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:landingPagePath"},
                {"name": "ga:pagePath"},
            ],
            "metrics": [
                {"expression": "ga:bounceRate"},
                {"expression": "ga:pageviews"},
                {"expression": "ga:uniquePageviews"},
                {"expression": "ga:exits"},
                {"expression": "ga:entrances"}
            ],
            "pageSize": 100000
        }
    },
    {
        "page_tracking_3": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:pagePath"},
                {"name": "ga:sourceMedium"}
            ],
            "metrics": [
                {"expression": "ga:exits"},
                {"expression": "ga:pageviews"}
            ],
            "pageSize": 100000
        }
    },
    {
        "performance": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:browser"},
                {"name": "ga:pagePath"},
                {"name": "ga:country"},
                {"name": "ga:sourceMedium"}
            ],
            "metrics": [
                {"expression": "ga:avgPageLoadTime"},
                {"expression": "ga:pageviews"},
                {"expression": "ga:uniquePageviews"}
            ],
            "pageSize": 100000
        }
    },
    {
        "ads_1": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:campaign"},
                {"name": "ga:adGroup"}
            ],
            "metrics": [
                {"expression": "ga:adClicks"},
                {"expression": "ga:adCost"},
                {"expression": "ga:CPC"},
                {"expression": "ga:sessions"},
                {"expression": "ga:goalCompletionsAll"}
            ],
            "pageSize": 100000
        }
    },
    {
        "ads_2": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:searchKeyword"},
                {"name": "ga:adMatchType"}
            ],
            "metrics": [
                {"expression": "ga:adClicks"},
                {"expression": "ga:adCost"},
                {"expression": "ga:CPC"},
                {"expression": "ga:sessions"},
                {"expression": "ga:goalCompletionsAll"}
            ],
            "pageSize": 100000
        }
    },
    # {
    #     "ads_3": {
    #         "viewId": VIEW_ID,
    #         "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
    #         "dimensions": [
    #             {"name": "ga:clientId"},
    #             {"name": "ga:date"},
    #             {"name": "ga:searchKeyword"},
    #             {"name": "ga:sourceMedium"}
    #         ],
    #         "metrics": [
    #             {"expression": "ga:impressions"},
    #             {"expression": "ga:adClicks"},
    #             {"expression": "ga:sessions"},
    #             {"expression": "ga:CTR"},
    #             {"expression": "ga:avgPosition"}
    #         ],
    #         "pageSize": 100000
    #     }
    # },
    {
        "ads_4": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:deviceCategory"},
                {"name": "ga:searchKeyword"}
            ],
            "metrics": [
                {"expression": "ga:impressions"},
                {"expression": "ga:adClicks"},
                {"expression": "ga:sessions"},
                {"expression": "ga:CTR"},
                {"expression": "ga:avgPosition"}
            ],
            "pageSize": 100000
        }
    },
    {
        "ecommerce": {
            "viewId": VIEW_ID,
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "ga:clientId"},
                {"name": "ga:date"},
                {"name": "ga:productName"},
                {"name": "ga:productCategory"},
                {"name": "ga:productSku"}
            ],
            "metrics": [
                {"expression": "ga:transactions"},
                {"expression": "ga:transactionRevenue"},
                {"expression": "ga:transactionShipping"},
                {"expression": "ga:itemQuantity"}
            ],
            "pageSize": 100000
        }
    }
]


def fetch_report_data(request, table_name):
    """
    Fetch data from Google Analytics API with pagination.
    """
    all_rows = []
    next_page_token = None
    report = None

    with tqdm(desc=f"Fetching data for {table_name}", unit="request") as pbar:
        while True:
            if next_page_token:
                request['pageToken'] = next_page_token

            try:
                response = analytics.reports().batchGet(  # pylint: disable=no-member
                    body={'reportRequests': [request]}
                ).execute()
            except HttpError as err:
                print(f"An error occurred for table {table_name}: {err}")
                log_error(request, err)
                return None

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

            pbar.update(1)

    if report is not None:
        print(f"Fetched {len(all_rows)
                         } rows from GA report for table {table_name}.")
    else:
        print(f"No report data fetched for table {table_name}.")
    return all_rows


def log_error(request, error):
    """
    Log the request and error details to a file for troubleshooting.
    """
    with open('error_log.txt', 'a', encoding="utf-8") as f:
        f.write(f"Request: {json.dumps(request, indent=2)}\n")
        f.write(f"Error: {error}\n\n")


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


def process_request_wrapper(args):
    request, index = args
    process_request(request)
    return index


# Fetch the report data with progress bar
print("Fetching data from Google Analytics...")


def process_request(request):
    """
    Process a single request: fetch data, estimate costs, and upload to BigQuery.
    """
    for table, table_request in request.items():
        data = fetch_report_data(table_request, table)
        if data is None:
            print(f"Skipping upload for {table} due to errors.")
            continue
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
        print(f"Estimated ingestion cost for {
              table} table: ${ingestion_cost:.2f}")

        # Prompt user for confirmation before uploading
        user_input = input(f"The estimated size of the data for {table} table is {size_in_mb:.2f} MB. "
                           f"Storage cost per month: ${
                               storage_cost:.2f}, Ingestion cost: ${ingestion_cost:.2f}. "
                           "Do you want to proceed with the upload? (yes/no): ").strip().lower()
        if user_input != 'yes':
            print(f"Upload canceled for {table} table by user.")
            continue

        # Generate the schema for the BigQuery table
        schema = generate_schema(table_request)

        # Define the BigQuery table schema
        TABLE_ID = f"{BQ_LOCATION}.UA_Backup.{table}_Data"
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        # Load data into BigQuery with progress bar
        print(f"Uploading data to BigQuery for {table} table...")
        job = bq_client.load_table_from_json(
            data, TABLE_ID, job_config=job_config)
        with tqdm(total=100, desc=f"Uploading to BigQuery for {table} table") as pbar:
            while not job.done():
                pbar.update(1)
                # Sleep for a short while before checking the job status again
                time.sleep(1)
        job.result()  # Wait for the job to complete

        print(f"Data loaded into BigQuery successfully for {table} table.")


# Run the requests in parallel
print("Fetching data from Google Analytics...")
with multiprocessing.Pool(processes=3) as pool:
    results = list(tqdm(pool.imap_unordered(process_request_wrapper, [
                   (request, i) for i, request in enumerate(requests)]), total=len(requests), desc="Overall Progress"))
