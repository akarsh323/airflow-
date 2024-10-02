from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import json
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'book_of_the_day_dag',
    default_args=default_args,
    description='DAG for fetching a book of the day from Open Library API',
    schedule_interval=timedelta(days=1),
)

# Define the functions that will be used in the PythonOperator tasks

def get_random_book(**kwargs):
    # Fetch random book info from Open Library API
    logical_date = kwargs['logical_date']
    response = requests.get("https://openlibrary.org/api/books?bibkeys=OLID:OL1M&format=json&jscmd=data")
    book_info = response.json()

    # Save the book info to S3 as JSON
    s3 = boto3.client('s3')
    bucket_name = 'your_s3_bucket'
    file_name = f"initial_info_{logical_date}.json"
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(book_info))
    
def get_authors(**kwargs):
    # Load the initial book info from S3
    logical_date = kwargs['logical_date']
    s3 = boto3.client('s3')
    bucket_name = 'your_s3_bucket'
    initial_info_file = f"initial_info_{logical_date}.json"
    obj = s3.get_object(Bucket=bucket_name, Key=initial_info_file)
    book_info = json.loads(obj['Body'].read())
    
    # Extract author keys and fetch author names from Open Library API
    author_keys = book_info['authors']
    author_names = []
    for key in author_keys:
        response = requests.get(f"https://openlibrary.org/authors/{key}.json")
        author_names.append(response.json()['name'])
    
    # Save the author names to S3 as JSON
    file_name = f"author_names_{logical_date}.json"
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(author_names))
    
def get_cover(**kwargs):
    # Load the initial book info from S3
    logical_date = kwargs['logical_date']
    s3 = boto3.client('s3')
    bucket_name = 'your_s3_bucket'
    initial_info_file = f"initial_info_{logical_date}.json"
    obj = s3.get_object(Bucket=bucket_name, Key=initial_info_file)
    book_info = json.loads(obj['Body'].read())
    
    # Get cover image using cover identifier
    if 'cover' in book_info:
        cover_id = book_info['cover']['id']
        cover_url = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg"
        cover_response = requests.get(cover_url)
        
        # Save the cover to S3 as a jpg
        file_name = f"cover_{logical_date}.jpg"
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=cover_response.content)

def save_final_book_record(**kwargs):
    # Load book info and author names from S3
    logical_date = kwargs['logical_date']
    s3 = boto3.client('s3')
    bucket_name = 'your_s3_bucket'
    
    initial_info_file = f"initial_info_{logical_date}.json"
    author_names_file = f"author_names_{logical_date}.json"
    
    book_obj = s3.get_object(Bucket=bucket_name, Key=initial_info_file)
    author_obj = s3.get_object(Bucket=bucket_name, Key=author_names_file)
    
    book_info = json.loads(book_obj['Body'].read())
    author_names = json.loads(author_obj['Body'].read())
    
    # Combine data and save final record
    final_record = {
        "title": book_info['title'],
        "authors": author_names,
        "subjects": book_info['subjects']
    }
    
    cover_file = f"cover_{logical_date}.jpg"
    try:
        s3.get_object(Bucket=bucket_name, Key=cover_file)
        final_record['cover_link'] = f"s3://{bucket_name}/{cover_file}"
    except:
        final_record['cover_link'] = None

    file_name = f"book_record_{logical_date}.json"
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(final_record))

def cleanup(**kwargs):
    logical_date = kwargs['logical_date']
    s3 = boto3.client('s3')
    bucket_name = 'your_s3_bucket'

    # Delete the intermediate files
    s3.delete_object(Bucket=bucket_name, Key=f"initial_info_{logical_date}.json")
    s3.delete_object(Bucket=bucket_name, Key=f"author_names_{logical_date}.json")
    s3.delete_object(Bucket=bucket_name, Key=f"cover_{logical_date}.jpg")

# Define the tasks
start_task = EmptyOperator(
    task_id='start',
    dag=dag
)

get_random_book_task = PythonOperator(
    task_id='get_random_book',
    python_callable=get_random_book,
    provide_context=True,
    dag=dag,
)

get_authors_task = PythonOperator(
    task_id='get_authors',
    python_callable=get_authors,
    provide_context=True,
    dag=dag,
)

get_cover_task = PythonOperator(
    task_id='get_cover',
    python_callable=get_cover,
    provide_context=True,
    dag=dag,
)

save_final_book_record_task = PythonOperator(
    task_id='save_final_book_record',
    python_callable=save_final_book_record,
    provide_context=True,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup,
    provide_context=True,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag
)

# Set up the task dependencies
start_task >> get_random_book_task >> [get_authors_task, get_cover_task] >> save_final_book_record_task >> cleanup_task >> end_task
