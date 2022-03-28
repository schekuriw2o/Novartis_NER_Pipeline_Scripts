import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from hashlib import sha256

yesterday = datetime.strftime(datetime.utcnow() - timedelta(1), '%Y-%m-%d')
start_time = yesterday + 'T00:00:00'
end_time = yesterday + 'T23:59:59'
file_name = sha256('{0}_{1}'.format(start_time,end_time).encode('utf-8')).hexdigest()

url = 'https://api.symplur.com/v1/twitter/analytics/content/tweets?databases=126938,38750,126937,34242&start={0}&end={1}&filters[tweet_languages]=en'.format(start_time,end_time)

headers = {
    "Accept": "application/json",
    "Authorization": "Bearer eyJpdiI6IkJtM3B0akJxVFAvMUxTUlpzY2tzWUE9PSIsInZhbHVlIjoiVEtwT3ZSblpFdWM1SUhLSWtUdkIyQ084TStOeXJBVGsrZ1dUVDNBN2pERT0iLCJtYWMiOiIzNmZhY2Y0MWI0NzY0M2JjNjMwZTllOWU0NjZhZGNlYjEwMmE0NWM5YWQ2MzA0YWJhOGUzYzE1ZWM0NDViZDc3In0"
}

response = requests.request("GET", url, headers=headers)


text = json.loads(response.text)

columns = ['string_field_0','string_field_1','string_field_2','string_field_3','string_field_4']
data = []
for t in text['tweets']:
    indiv = []
    tweet = t['tweet']
    user = tweet['user']
    indiv.append(user['display_name'])
    indiv.append('@' + user['screen_name'])
    stake = None
    if 'stakeholder_categories' in user and len(user['stakeholder_categories']) > 0:
        for category in user['stakeholder_categories']:
            if stake is None:
                stake = category['name']
            else:
                stake = stake + ' , ' + category['name']
    indiv.append(stake)
    indiv.append(tweet['text'])
    indiv.append(t['timestamp'])
    data.append(indiv)

print(data)
df = pd.DataFrame(data, columns=columns)

df.to_csv('{0}.csv'.format(file_name), encoding='utf-8', index=False)

# df.write.option("sep","|").mode('overwrite').csv('gs://w2odataengineering/client_work/source/', header=False, index=False)
# df.to_csv('gs://w2odataengineering/client_work/source/{0}.csv'.format('test1'), encoding='utf-8', sep='|', index=False)


# In[ ]:




