import json
from airflow.providers.http.hooks.http import HttpHook
import requests
import time


class WikipediaHook(HttpHook):

    def __init__(self, query, table_class = None,  country_list = None):
        self.query = query
        self.table_class = table_class or "table"
        self.country_list = country_list
        # super().__init__(http_conn_id = self.conn_id)
    
    def create_url(self):
        query = self.query
        # Tweet fields are adjustable.
        # Options include:
        # attachments, author_id, context_annotations,
        # conversation_id, created_at, entities, geo, id,
        # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
        # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
        # source, text, and withheld

        url = "{}/2/tweets/search/all?max_results=100&query={} {}&{}&{}{}{}".format(
            self.base_url, query, place_countries, tweet_fields, user_fields, start_time, end_time
        )

        return url
    
    def connect_to_endpoint(self, url, session):
        response = requests.Request("GET", url)
        prep = session.prepare_request(response)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {}).json()
        

    def paginate(self, url, session, next_token=""):
        if next_token:
            full_url = f"{url}&next_token={next_token}"
        else:
            full_url = url
        
        data = self.connect_to_endpoint(full_url, session)
        yield data
        if "next_token" in data.get("meta", {}):
            yield from self.paginate(url, session, data['meta']['next_token'])

    def run(self):
        session = self.get_conn()
        url = self.create_url()
        yield from self.paginate(url, session)


if __name__ == "__main__":
    for pg in TwitterHook("Covid", start_time="2022-11-10T17:00:00.00Z", end_time="2022-11-16T00:00:00.00Z", country="BR").run():
        print(json.dumps(pg, indent=4, sort_keys=True))
