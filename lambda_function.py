from datetime import datetime
import boto3
import pandas as pd
from io import StringIO 
import json
import io

s3_client = boto3.client('s3')

def calculate_booking_duration(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    duration = (end_date - start_date).days
    return duration

def lambda_handler(event, context):

    print(event)
    # Filter events based on booking duration more than 1 day
    filtered_events = [event['body'] for event in event if calculate_booking_duration(event['body']['startDate'], event['body']['endDate']) > 1]
    current_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    file_name = f"{current_date}-processed.csv"
    filtered_df = pd.DataFrame(filtered_events)
    tgt_bucket_name = 'airbnb-booking-filteredrecords'
    file_path = 'filtered_data/'+file_name
    if not filtered_df.empty:
    # Initialize a CSV buffer
        csv_buffer = io.StringIO()

        filtered_df.to_csv(csv_buffer, index=False)

        csv_buffer.seek(0)

        s3_client.put_object(Body=csv_buffer.getvalue().encode(), Bucket=tgt_bucket_name, Key=file_path)
    else:
        print("DataFrame is empty, skipping upload to S3.")

    return {
        'statusCode': 200,
        'body': json.dumps('Data has been processed and filtered and stored in S3')
    }

