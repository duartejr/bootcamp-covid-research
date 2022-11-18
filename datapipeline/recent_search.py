import requests
import os
import json

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


def auth():
    return os.environ.get("BEARER_TOKEN")


def create_url(country):
    query = "covid"
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text,geo,possibly_sensitive"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at,location,public_metrics,verified"
    filters = "start_time=2022-11-10T16:59:00.00Z&end_time=2022-11-16T00:00:00.00Z"
    place_countries = f"(place_country:{country})"
    url = "https://api.twitter.com/2/tweets/search/all?max_results=10&query={}%20{}&{}&{}&{}".format(
        query, place_countries, tweet_fields, user_fields, filters
    )
    # https://api.twitter.com/2/tweets/search/recent?query=%23nowplaying%20(happy%20OR%20exciting%20OR%20excited%20OR%20favorite%20OR%20fav%20OR%20amazing%20OR%20lovely%20OR%20incredible)%20(place_country%3AUS%20OR%20place_country%3AMX%20OR%20place_country%3ACA)%20-horrible%20-worst%20-sucks%20-bad%20-disappointing
    return url


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    print(headers)
    return headers


def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def paginate(url, headers, next_token=""):
    if next_token:
        full_url = f"{url}&next_token={next_token}"
    else:
        full_url = url
    data = connect_to_endpoint(full_url, headers)
    yield data
    if "next_token" in data.get("meta", {}):
        yield from paginate(url, headers, data['meta']['next_token'])

def main():
    bearer_token = auth()
    url = create_url("ES")
    print(url)
    # headers = create_headers(bearer_token)
    # for json_response in paginate(url, headers):
    #     #json_response = connect_to_endpoint(url, headers)
    #     print(json.dumps(json_response, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()