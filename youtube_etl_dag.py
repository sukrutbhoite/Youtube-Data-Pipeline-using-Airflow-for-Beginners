import datetime
from datetime import timedelta
import os
import json
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"



video_ids = ["q8q3OFFfY6c"]

s3_bucket_name = "youtube-data-airflow-project-sukrut"


default_args = {
    "owner": "Sukrut",
    "email" : "iamsukrut.sb@gmail.com",
    "start_date": datetime.datetime(day=13, month=7, year=2025),
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "email_on_failure": True,
    "email_on_retry": False,
    "depends_on_past": False,
    "execution_timeout": timedelta(minutes=2)
}


@dag(
    dag_id="youtube_etl_dynamic_dag",
    schedule=timedelta(minutes=1),
    default_args=default_args,
    catchup=False
)
def youtube_etl_dynamic():
    @task
    def extract_comments(video_id: str):
        import googleapiclient.discovery

        try:
            conn = BaseHook.get_connection("youtube_api_conn")
            conn_extra = json.loads(conn.extra)
            api_service_name = conn_extra.get("api_service_name")
            api_version = conn_extra.get("api_version")
            developer_key = conn_extra.get("developer_key")

        except Exception as e:
            raise ValueError(f"Could not retrieve 'youtube_api_conn' or parse credentials: {e}")


        youtube_connection = googleapiclient.discovery.build(
            api_service_name,
            api_version,
            developerKey=developer_key
        )


        request = youtube_connection.commentThreads().list(
            part="snippet, replies",
            videoId=video_id
        )
        response = request.execute()


        return {"video_id": video_id, "response": response}

    @task
    def process_comments(response_data):
        video_id = response_data["video_id"]
        response = response_data["response"]

        comments_list = []
        response_items = response.get("items", [])

        for comment_item in response_items:

            snippet = comment_item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {})
            author = snippet.get('authorDisplayName')
            comment_text = snippet.get('textOriginal')
            publish_time = snippet.get('publishedAt')

            comment_info = {
                'video_id': video_id,
                'author': author,
                'comment': comment_text,
                'published_at': publish_time
            }
            comments_list.append(comment_info)

        return comments_list


    @task
    def load_to_s3(processed_comments_list):
        s3_hook = S3Hook(aws_conn_id='aws_conn_s3')

        uploaded_files = 0
        for video_comments_list in processed_comments_list:
            if not video_comments_list:
                continue

            video_id = video_comments_list[0]['video_id']
            for i, comment_info in enumerate(video_comments_list):
                s3_key = f"{video_id}/comment_{i}.json"
                comment_json = json.dumps(comment_info, indent=4)
                s3_hook.load_string(
                    string_data=comment_json,
                    key=s3_key,
                    bucket_name=s3_bucket_name,
                    replace=True
                )
                uploaded_files += 1
        print(f"Successfully uploaded {uploaded_files} comments to S3 bucket: {s3_bucket_name}")



    extracted_responses = extract_comments.expand(video_id=video_ids)

    processed_comments_list = process_comments.partial().expand(response_data=extracted_responses)

    load_to_s3(processed_comments_list)


youtube_etl_dynamic()