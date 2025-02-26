import requests, sys, time, os, argparse

# List of simple to collect features
snippet_features = ["title",
                    "publishedAt",
                    "channelId",
                    "channelTitle",
                    "categoryId"]

# Any characters to exclude, generally these are things that become problematic in CSV files
unsafe_characters = ['\n', '"']

# Used to identify columns, currently hardcoded order
header = ["video_id"] + snippet_features + ["trending_date", "tags", "view_count", "likes", "dislikes",
                                            "comment_count", "thumbnail_link", "comments_disabled",
                                            "ratings_disabled", "description"]

def setup(api_path, code_path):
    with open(api_path, 'r') as file:
        api_key = file.readline()

    with open(code_path) as file:
        country_codes = [x.rstrip() for x in file]

    return api_key, country_codes

def prepare_feature(feature):
    # Removes any character from the unsafe characters list and surrounds the whole item in quotes
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'

def api_request(page_token, country_code, max_results=50): #added max_results
    # Builds the URL and requests the JSON from it
    request_url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&videoCategoryId=10&regionCode={country_code}&maxResults={max_results}&key={api_key}{page_token}" #changed to search endpoint, added categoryId
    request = requests.get(request_url)
    if request.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        sys.exit()
    return request.json()

def get_tags(tags_list):
    # Takes a list of tags, prepares each tag and joins them into a string by the pipe character
    return prepare_feature("|".join(tags_list))

def get_videos(items, country_code): #added country code
    lines = []
    video_ids = [] #to store video ids for video details request
    for video in items:
        video_ids.append(video['id']['videoId'])

    #Get video details
    for i in range(0, len(video_ids), 50): #max 50 video ids per request
        chunk = video_ids[i:i + 50]
        video_details_url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={','.join(chunk)}&regionCode={country_code}&key={api_key}"
        video_details_request = requests.get(video_details_url)
        if video_details_request.status_code == 429:
            print("Temp-Banned due to excess requests, please wait and continue later")
            sys.exit()
        video_details_json = video_details_request.json()
        if 'items' in video_details_json:
            for video_detail in video_details_json['items']:

                comments_disabled = False
                ratings_disabled = False

                if "statistics" not in video_detail:
                    continue

                video_id = prepare_feature(video_detail['id'])

                snippet = video_detail['snippet']
                statistics = video_detail['statistics']

                features = [prepare_feature(snippet.get(feature, "")) for feature in snippet_features]

                description = snippet.get("description", "")
                thumbnail_link = snippet.get("thumbnails", dict()).get("default", dict()).get("url", "")
                trending_date = time.strftime("%y.%d.%m")
                tags = get_tags(snippet.get("tags", ["[none]"]))
                view_count = statistics.get("viewCount", 0)

                if 'likeCount' in statistics and 'dislikeCount' in statistics:
                    likes = statistics['likeCount']
                    dislikes = statistics['dislikeCount']
                else:
                    ratings_disabled = True
                    likes = 0
                    dislikes = 0

                if 'commentCount' in statistics:
                    comment_count = statistics['commentCount']
                else:
                    comments_disabled = True
                    comment_count = 0

                line = [video_id] + features + [prepare_feature(x) for x in [trending_date, tags, view_count, likes, dislikes,
                                                                            comment_count, thumbnail_link, comments_disabled,
                                                                            ratings_disabled, description]]
                lines.append(",".join(line))

    return lines

def get_pages(country_code, max_results=50, total_results=1000): #added total_results, max_results
    country_data = []
    next_page_token = "&"
    collected_results = 0
    while next_page_token is not None and collected_results < total_results:
        video_data_page = api_request(next_page_token, country_code, max_results)
        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else None #correct None value.

        items = video_data_page.get('items', [])
        page_results = get_videos(items, country_code) #added country_code
        country_data += page_results

        collected_results += len(items)
        print(f"Collected {collected_results} of {total_results}")

    return country_data[:total_results] #limit results to total_results

def write_to_file(country_code, country_data):
    print(f"Writing {country_code} data to file...")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(f"{output_dir}/{time.strftime('%y.%d.%m')}_{country_code}_music_videos.csv", "w+", encoding='utf-8') as file:
        for row in country_data:
            file.write(f"{row}\n")

def get_data():
    for country_code in country_codes:
        country_data = [",".join(header)] + get_pages(country_code)
        write_to_file(country_code, country_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--key_path', help='Path to the file containing the api key, by default will use api_key.txt in the same directory', default=r'D:\Git_repository\dataEn_final_project\ignore_dir\api_key.txt')
    parser.add_argument('--country_code_path', help='Path to the file containing the list of country codes to scrape, by default will use country_codes.txt in the same directory', default=r'D:\Git_repository\dataEn_final_project\ignore_dir\country_codes.txt')
    parser.add_argument('--output_dir', help='Path to save the outputted files in', default='output2/')

    args = parser.parse_args()

    output_dir = args.output_dir
    api_key, country_codes = setup(args.key_path, args.country_code_path)

    get_data()