import os
import json
import sys
import time
import argparse
import logging
from tqdm import tqdm
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.api_core.exceptions import GoogleAPIError
import concurrent.futures
from requests.exceptions import Timeout
from httplib2 import Http
from google_auth_httplib2 import AuthorizedHttp

# Setup logging
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s %(message)s')


def fetch_report_data(raw_request, table_name, analytics_service):
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
            response = analytics_service.reports().batchGet(
                body={'reportRequests': [raw_request]}
            ).execute()  # pylint: disable=no-member
        except HttpError as err:
            logging.error(f"An error occurred for table {table_name}: {err}")
            log_error(raw_request, err)
            return None
        except TimeoutError as err:
            logging.error(f"Timeout error for table {table_name}: {err}")
            log_error(raw_request, err)
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

    if report is not None:
        logging.info(
            f"Fetched {len(all_rows)} rows from GA report for table {table_name}.")
    else:
        logging.info(f"No report data fetched for {table_name}.")
    return all_rows


def log_error(request, error):
    """
    Log the request and error details to a file for troubleshooting.
    """
    with open('error_log.txt', 'a') as f:
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
    """
    Wrapper function for threading.
    """
    request, index, view_id, start_date, end_date, project, upload, estimate_only = args
    return process_request(request, view_id, start_date, end_date, project, upload, estimate_only), index


# Fetch the report data with progress bar
print("Fetching data from Google Analytics...")


def process_request(request, view_id, start_date, end_date, project, upload, estimate_only):
    """
    Process a single request: fetch data, estimate costs, and optionally upload to BigQuery.
    """
    # Reinitialize analytics and BigQuery clients
    credentials = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    http = Http(timeout=3600)
    authorized_http = AuthorizedHttp(credentials, http=http)
    analytics_service = build(
        "analyticsreporting", "v4", cache_discovery=False, http=authorized_http)
    bq_service = bigquery.Client()

    total_size_in_bytes = 0

    for table, table_request in request.items():
        table_request['viewId'] = view_id
        table_request['dateRanges'] = [
            {"startDate": start_date, "endDate": end_date}]
        data = fetch_report_data(table_request, table, analytics_service)
        if data is None:
            logging.info(f"Skipping upload for {table} due to errors.")
            continue
        logging.info(f"Total rows fetched for {table} table: {len(data)}")

        # Estimate the size of the data
        size_in_bytes = estimate_size(data)
        total_size_in_bytes += size_in_bytes
        size_in_mb = size_in_bytes / (1024 * 1024)
        logging.info(f"Estimated size of the data for {table} table: {
                     size_in_bytes} bytes ({size_in_mb:.2f} MB)")

        # Estimate the costs
        storage_cost, ingestion_cost = estimate_costs(size_in_bytes)
        logging.info(f"Estimated storage cost per month for {
                     table} table: ${storage_cost:.2f}")
        logging.info(f"Estimated ingestion cost for {
                     table} table: ${ingestion_cost:.2f}")

        if not estimate_only and upload:
            # Generate the schema for the BigQuery table
            schema = generate_schema(table_request)

            # Define the BigQuery table schema
            table_id = f"{project}.UA_Backup_{view_id}.{table}"
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

            # Load data into BigQuery with progress bar
            logging.info(f"Uploading data to BigQuery for {table} table...")
            job = bq_service.load_table_from_json(
                data, table_id, job_config=job_config)
            with tqdm(total=100, desc=f"Uploading to BigQuery for {table} table") as pbar:
                while not job.done():
                    pbar.update(1)
                    # Sleep for a short while before checking the job status again
                    time.sleep(1)
            job.result()  # Wait for the job to complete

            logging.info(f"Data loaded into BigQuery successfully for {
                         table} table.")

    return total_size_in_bytes


def main():
    # Set the path to your service account key file

    # Argument parser setup
    parser = argparse.ArgumentParser(
        description="Fetch and optionally upload Google Analytics data to BigQuery")
    parser.add_argument("view_id", help="Google Analytics View ID")
    parser.add_argument(
        "start_date", help="Start date for the data (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date for the data (YYYY-MM-DD)")
    parser.add_argument("project_name", help="BigQuery project name")
    parser.add_argument("--upload", action="store_true",
                        help="Upload data to BigQuery")
    parser.add_argument("--estimate-only", action="store_true",
                        help="Only estimate the costs and row consumption without uploading")
    args = parser.parse_args()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "chs-website-409300-4302acfcf512.json"

    # Define the Dynamic Inputs
    VIEW_ID = args.view_id
    START_DATE = args.start_date
    END_DATE = args.end_date
    PROJECT_NAME = args.project_name

    # Define the GA3 requests for different tables
    requests = [
        {
            "hits_event": {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
                "dimensions": [
                    {"name": "ga:clientId"},
                    {"name": "ga:dateHourMinute"},
                    {"name": "ga:eventAction"},
                    {"name": "ga:eventCategory"},
                    {"name": "ga:eventLabel"},
                    {"name": "ga:pagePath"},
                    {"name": "ga:pageTitle"}
                ],
                "metrics": [
                    {"expression": "ga:totalEvents"},
                    {"expression": "ga:uniqueEvents"}
                ],
                "pageSize": 2000000
            }
        },
        {
            "users_geo": {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
                "dimensions": [
                    {"name": "ga:clientId"},
                    {"name": "ga:dateHourMinute"},
                    {"name": "ga:city"},
                    {"name": "ga:country"},
                    {"name": "ga:region"}
                ],
                "metrics": [
                    {"expression": "ga:users"}
                ],
                "pageSize": 2000000
            }
        },
        {
            "users_acquisition": {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
                "dimensions": [
                    {"name": "ga:clientId"},
                    {"name": "ga:dateHourMinute"},
                    {"name": "ga:campaign"},
                    {"name": "ga:sourceMedium"},
                    {"name": "ga:keyword"},
                    {"name": "ga:adContent"}
                ],
                "metrics": [
                    {"expression": "ga:users"},
                    {"expression": "ga:sessions"},
                    {"expression": "ga:bounces"},
                ],
                "pageSize": 2000000
            }
        },
        {
            "sessions_source": {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
                "dimensions": [
                    {"name": "ga:clientId"},
                    {"name": "ga:dateHourMinute"},
                    {"name": "ga:sourceMedium"},
                    {"name": "ga:adContent"},
                    {"name": "ga:referralPath"},
                    {"name": "ga:channelGrouping"}
                ],
                "metrics": [
                    {"expression": "ga:users"},
                    {"expression": "ga:bounces"},
                    {"expression": "ga:sessions"},
                ],
                "pageSize": 2000000
            }
        },
        {
            "hits_pages": {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
                "dimensions": [
                    {"name": "ga:clientId"},
                    {"name": "ga:dateHourMinute"},
                    {"name": "ga:pagePath"},
                    {"name": "ga:pageTitle"}
                ],
                "metrics": [
                    {"expression": "ga:entrances"},
                    {"expression": "ga:pageviews"},
                    {"expression": "ga:uniquePageviews"},
                    {"expression": "ga:timeOnPage"},
                ],
                "pageSize": 2000000
            }
        },
        {
            "hits_landing_pages": {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
                "dimensions": [
                    {"name": "ga:clientId"},
                    {"name": "ga:dateHourMinute"},
                    {"name": "ga:landingPagePath"},
                    {"name": "ga:pageTitle"},
                    {"name": "ga:pagePath"},
                ],
                "metrics": [
                    {"expression": "ga:entrances"},
                    {"expression": "ga:pageviews"},
                    {"expression": "ga:uniquePageviews"},
                    {"expression": "ga:timeOnPage"},
                ],
                "pageSize": 2000000
            }
        },
        {
            "hits_exit_pages": {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
                "dimensions": [
                    {"name": "ga:clientId"},
                    {"name": "ga:dateHourMinute"},
                    {"name": "ga:exitPagePath"},
                    {"name": "ga:pageTitle"},
                    {"name": "ga:pagePath"},
                ],
                "metrics": [
                    {"expression": "ga:exits"},
                    {"expression": "ga:pageviews"},
                    {"expression": "ga:uniquePageviews"},
                    {"expression": "ga:timeOnPage"},
                ],
                "pageSize": 2000000
            }
        },
        {
            "sessions_googleads": {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
                "dimensions": [
                    {"name": "ga:clientId"},
                    {"name": "ga:date"},
                    {"name": "ga:keyword"},
                    {"name": "ga:adwordsAdGroupID"},
                    {"name": "ga:campaignCode"}
                ],
                "metrics": [
                    {"expression": "ga:adClicks"},
                    {"expression": "ga:CPM"},
                    {"expression": "ga:CPC"},
                ],
                "pageSize": 2000000
            }
        },
        {
            "ecomm_products": {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
                "dimensions": [
                    {"name": "ga:clientId"},
                    {"name": "ga:dateHourMinute"},
                    {"name": "ga:productName"},
                    {"name": "ga:productSku"},
                ],
                "metrics": [
                    {"expression": "ga:itemQuantity"},
                    {"expression": "ga:uniquePurchases"},
                    {"expression": "ga:itemRevenue"},
                ],
                "pageSize": 2000000
            }
        },
        {
            "ecomm_transactions": {
                "viewId": VIEW_ID,
                "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
                "dimensions": [
                    {"name": "ga:clientId"},
                    {"name": "ga:dateHourMinute"},
                    {"name": "ga:transactionId"},

                ],
                "metrics": [
                    {"expression": "ga:transactions"},
                    {"expression": "ga:transactionRevenue"},
                    {"expression": "ga:itemQuantity"},
                    {"expression": "ga:transactionsPerSession"},
                ],
                "pageSize": 2000000
            }
        }
    ]

    # Run the requests in parallel using concurrent.futures
    print("Fetching data from Google Analytics...")
    total_size_in_bytes = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(process_request_wrapper, (request, i, args.view_id, args.start_date, args.end_date,
                                                             args.project_name, args.upload, args.estimate_only)): i for i, request in enumerate(requests)}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Overall Progress"):
            try:
                result, _ = future.result()  # Wait for the job to complete
                total_size_in_bytes += result
                total_size_in_mb = total_size_in_bytes / (1024 * 1024)
                storage_cost, ingestion_cost = estimate_costs(
                    total_size_in_bytes)
                tqdm.write(f"Total estimated size: {total_size_in_mb:.2f} MB")
                tqdm.write(f"Total estimated storage cost per month: ${
                    storage_cost:.2f}")
                tqdm.write(f"Total estimated ingestion cost: ${
                    ingestion_cost:.2f}")
            except (HttpError, TimeoutError, concurrent.futures.thread.BrokenThreadPool) as e:
                logging.error(f"An error occurred: {e}")
            except GoogleAPIError as e:
                logging.error(f"A BigQuery error occurred: {e}")
            except (OSError, ValueError) as e:
                logging.error(f"An unexpected error occurred: {e}")


if __name__ == '__main__':
    main()
