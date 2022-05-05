import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from hashlib import sha256

yesterday = datetime.strftime(datetime.utcnow() - timedelta(2), '%Y-%m-%d')
start_time = yesterday + 'T00:00:00'
end_time = yesterday + 'T23:59:59'
file_name = sha256('{0}_{1}'.format(start_time, end_time).encode('utf-8')).hexdigest()

tweet_url = 'https://api.symplur.com/v1/twitter/analytics/content/tweets?databases=126938,38750,126937,34242&start={0}&end={1}&filters[tweet_languages]=en'.format(
    start_time, end_time)
influencers_url = 'https://api.symplur.com/v1/twitter/analytics/people/influencers?databases=126938,38750,126937,34242&start={0}&end={1}&filters[tweet_languages]=en'.format(
    start_time, end_time)
urls_url = 'https://api.symplur.com/v1/twitter/analytics/content/urls?databases=126938,38750,126937,34242&start={0}&end={1}&filters[tweet_languages]=en'.format(
    start_time, end_time)

headers = {
    "Accept": "application/json",
    "Authorization": "Bearer eyJpdiI6IkJtM3B0akJxVFAvMUxTUlpzY2tzWUE9PSIsInZhbHVlIjoiVEtwT3ZSblpFdWM1SUhLSWtUdkIyQ084TStOeXJBVGsrZ1dUVDNBN2pERT0iLCJtYWMiOiIzNmZhY2Y0MWI0NzY0M2JjNjMwZTllOWU0NjZhZGNlYjEwMmE0NWM5YWQ2MzA0YWJhOGUzYzE1ZWM0NDViZDc3In0"
}


def get_tweets(url):
    response = requests.request("GET", url, headers=headers)

    text = json.loads(response.text)
    print(text)
    columns = ['name', 'IDs', 'stakeholder_categories', 'Contents', 'date']
    data = []
    for t in text['tweets']:
        try:
            indiv = []
            tweet = t['tweet'] if 'tweet' in t else {}
            user = tweet['user'] if 'user' in tweet else {}
            indiv.append(user['display_name'] if 'display_name' in user else "" )
            indiv.append('@' + user['screen_name'] if 'screen_name' in user else "")
            stake = None
            if 'stakeholder_categories' in user and len(user['stakeholder_categories']) > 0:
                for category in user['stakeholder_categories']:
                    if stake is None:
                        stake = category['name']
                    else:
                        stake = stake + ' , ' + category['name']
            indiv.append(stake)
            indiv.append(tweet['text'] if 'text' in tweet else "")
            indiv.append(t['timestamp'] if 'timestamp' in t else "")
            data.append(indiv)
        except KeyError:
            print(KeyError)

    df = pd.DataFrame(data, columns=columns)

    df.to_csv('{0}.csv'.format(file_name), encoding='utf-8', index=False)

    # df.write.option("sep","|").mode('overwrite').csv('gs://w2odataengineering/client_work/source/', header=False, index=False)
    # df.to_csv('gs://w2odataengineering/client_work/source/{0}.csv'.format('test1'), encoding='utf-8', sep='|', index=False)


def get_influencers(url):
    response = requests.request("GET", url, headers=headers)

    text = json.loads(response.text)

    columns = ['date', 'influencer_name', 'symplur_rank', 'hsg_score', 'profile_image', 'stakeholder_categories']
    data = []
    for t in text['influencers']:
        try:
            indiv = []
            infleuncer_name = t['user']['display_name'] if 'display_name' in t['user'] else ""
            date = start_time
            hsg_score = t['user']['hsg_score'] if 'hsg_score' in t['user'] else ""
            symplur_rank = t['symplurrank'] if 'symplurrank' in t else ""
            indiv.append(date)
            indiv.append(infleuncer_name)
            indiv.append(symplur_rank)
            indiv.append(hsg_score)
            indiv.append(t['user']['profile_image'] if 'profile_image' in t['user'] else "")
            stake = None
            if 'stakeholder_categories' in t['user'] and len(t['user']['stakeholder_categories']) > 0:
                for category in t['user']['stakeholder_categories']:
                    if stake is None:
                        stake = category['name']
                    else:
                        stake = stake + ' , ' + category['name']
            indiv.append(stake)
            data.append(indiv)
        except KeyError:
            print(KeyError)


    df = pd.DataFrame(data, columns=columns)

    df.to_csv('{0}_influencers.csv'.format(file_name), encoding='utf-8', index=False)

    # df.write.option("sep","|").mode('overwrite').csv('gs://w2odataengineering/client_work/source/', header=False, index=False)
    # df.to_csv('gs://w2odataengineering/client_work/source/{0}.csv'.format('test1'), encoding='utf-8', sep='|', index=False)


def get_urls(url):
    response = requests.request("GET", url, headers=headers)

    text = json.loads(response.text)

    columns = ['date', 'url', 'resolved_url', 'title', 'description']
    data = []
    for t in text['urls']:
        try:
            indiv = []
            url = t['url'] if 'url' in t else ""
            date = start_time
            resolved_rank = t['resolved_url'] if 'resolved_url' in t else ""
            title = t['meta']['title'] if 'title' in t['meta'] else ""
            description = t['meta']['description'] if 'description' in t['meta'] else ""
            indiv.append(date)
            indiv.append(url)
            indiv.append(resolved_rank)
            indiv.append(title)
            indiv.append(description)
            data.append(indiv)
        except KeyError:
            print(KeyError)

    df = pd.DataFrame(data, columns=columns)

    df.to_csv('{0}_urls.csv'.format(file_name), encoding='utf-8', index=False)

    # df.write.option("sep","|").mode('overwrite').csv('gs://w2odataengineering/client_work/source/', header=False, index=False)
    # df.to_csv('gs://w2odataengineering/client_work/source/{0}.csv'.format('test1'), encoding='utf-8', sep='|', index=False)


get_tweets(tweet_url)
get_influencers(influencers_url)
get_urls(urls_url)
