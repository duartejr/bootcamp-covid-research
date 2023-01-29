# twitter_hook - Hook to collect tweets from the Twitter API.
#
# Author: Duarte Junior <duarte.jr105@gmail.com>
#
# Licensed to GNU General Public License under a Contributor Agreement.
import json
import requests
from airflow.providers.http.hooks.http import HttpHook


class TwitterHook(HttpHook):
    """
    This class searches the tweets for the provided text and returns a 
    dataframe with the tweets that contain the text.
    """

    def __init__(self, query: str, conn_id = None, start_time = None,
                 end_time = None, country = None):
        """
        This function initialize a new TwitterHook object.

        Args:
            query (str): A string with the text that the API will search in the tweets.
            conn_id (str, optional): Pamaters to make a new connection with the
                                     Twitter API. Defaults to None.
            start_time (str, optional): First date of the tweets. Defaults to None.
            end_time (str, optional): Last date of the tweets. Defaults to None.
            country (str, optional): This is a parameter that specifies the 
                                     country name of the tweets to be searched.
                                     Defaults to None.
        """
        self.query = query
        self.conn_id = conn_id or "twitter_default"
        self.start_time = start_time
        self.end_time = end_time
        self.country = country
        super().__init__(http_conn_id = self.conn_id)
    
    def create_url(self) -> str:
        """
        This function creates the URL with the search parameters to be used by
        the Twitter API to collect tweets based on the text and country of 
        origin specified.

        Returns:
            str: The URL of the Twitter API.
        """
        query = self.query
        # Tweet fields are adjustable.
        # Options include:
        # attachments, author_id, context_annotations,
        # conversation_id, created_at, entities, geo, id,
        # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
        # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
        # source, text, and withheld
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text,geo"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at,location"
        
        start_time = (
            f'&start_time={self.start_time}'
            if self.start_time
            else ""
        )
        
        end_time = (
            f'&end_time={self.end_time}'
            if self.end_time
            else ""
        )
        
        place_countries = f"place_country:{self.country}"
        url = "{}/2/tweets/search/all?max_results=100&query={} {}&{}&{}{}{}".format(
            self.base_url, query, place_countries, tweet_fields, user_fields, 
            start_time, end_time
        )

        return url
    
    def connect_to_endpoint(self, url: str, session):
        """
        This function stablish a connection with the Twitter API

        Args:
            url (str): Twitter API URL
            session : Parameters to stablish the conenction

        Returns:
            json: The answer of the API resquest.
        """
        response = requests.Request("GET", url)
        prep = session.prepare_request(response)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {}).json()
        

    def paginate(self, url, session, next_token=""):
        """
        This function performs the process of pagination in the API response,
        which means it retrieves data from all pages in the API response one by 
        one. The function will loop through all pages and extract the data until
        there are no more pages to retrieve. This helps in case the API response
        has more than one page of data, which means the data will be collected
        from all pages and not just one.

        Args:
            url (str): Twitter API URL.
            session (API session): The Twitter API session.
            next_token (str, optional): If will get next tokens. Defaults to "".

        Yields:
            str: The tweets collected by the API.
        """
        if next_token:
            full_url = f"{url}&next_token={next_token}"
        else:
            full_url = url
        
        data = self.connect_to_endpoint(full_url, session)
        yield data
        if "next_token" in data.get("meta", {}):
            yield from self.paginate(url, session, data['meta']['next_token'])

    def run(self):
        """
        This funcion is responsable to run all the taks that are necessaary to
        collect the tweets using the API.

        Yields:
            dict: The tweets collected.
        """
        session = self.get_conn()
        url = self.create_url()
        yield from self.paginate(url, session)


if __name__ == "__main__":
    for pg in TwitterHook("Covid", start_time="2022-11-10T17:00:00.00Z", end_time="2022-11-16T00:00:00.00Z", country="BR").run():
        print(json.dumps(pg, indent=4, sort_keys=True))
