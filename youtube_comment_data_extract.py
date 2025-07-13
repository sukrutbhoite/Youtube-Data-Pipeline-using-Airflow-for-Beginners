import csv
import os
import googleapiclient.discovery


def load_credentials():
    credentials_dict = {}
    try:
        with open(file_path, mode='r', encoding='utf-8-sig') as csvfile:
            csv_reader = csv.reader(csvfile)
            for row in csv_reader:
                    key = row[0].strip()
                    value = row[1].strip()
                    credentials_dict[key] = value

    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return None
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return None

    return credentials_dict


def extract_comments(video_id):
    request = youtube_connection.commentThreads().list(
        part="snippet, replies",
        videoId=video_id
    )
    response = request.execute()

    return response


def process_comments(response_items):
    comments = []
    for comment in response_items:
            author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
            comment_text = comment['snippet']['topLevelComment']['snippet']['textOriginal']
            publish_time = comment['snippet']['topLevelComment']['snippet']['publishedAt']
            comment_info = {'author': author,
                    'comment': comment_text, 'published_at': publish_time}
            comments.append(comment_info)
    print(f'Finished processing {len(comments)} comments.')
    return comments



file_path = "H:/Authentication Keys/Google Youtube Data.csv"

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
api_service_name = load_credentials().get("api_service_name")
api_version = load_credentials().get("api_version")
developer_key = load_credentials().get("API_KEY")

youtube_connection = googleapiclient.discovery.build(
        api_service_name,
        api_version,
        developerKey = developer_key)


video_ids = ["q8q3OFFfY6c"]
def main():
    comments_dict = {}
    for video_id in video_ids:
        response = extract_comments(video_id)
        comments = process_comments(response["items"])

        comments_dict[video_id] = comments
    return comments_dict



