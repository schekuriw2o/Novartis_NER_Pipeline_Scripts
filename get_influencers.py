import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from hashlib import sha256

yesterday = datetime.strftime(datetime.utcnow() - timedelta(1), '%Y-%m-%d')
start_time = yesterday + 'T00:00:00'
end_time = yesterday + 'T23:59:59'
file_name = sha256('{0}_{1}_influencers'.format(start_time,end_time).encode('utf-8')).hexdigest()

url = 'https://api.symplur.com/v1/twitter/analytics/people/influencers?databases=126938,38750,126937,34242&start={0}&end={1}&filters[tweet_languages]=en'.format(start_time,end_time)

headers = {
    "Accept": "application/json",
    "Authorization": "Bearer eyJpdiI6IkJtM3B0akJxVFAvMUxTUlpzY2tzWUE9PSIsInZhbHVlIjoiVEtwT3ZSblpFdWM1SUhLSWtUdkIyQ084TStOeXJBVGsrZ1dUVDNBN2pERT0iLCJtYWMiOiIzNmZhY2Y0MWI0NzY0M2JjNjMwZTllOWU0NjZhZGNlYjEwMmE0NWM5YWQ2MzA0YWJhOGUzYzE1ZWM0NDViZDc3In0"
}

response = requests.request("GET", url, headers=headers)


text = json.loads(response.text)
print(text)


columns = ['date','influencer_name','symplur_rank','hsg_score']
data = []
for t in text['influencers']:
    indiv = []
    infleuncer_name = t['user']['display_name']
    date = start_time
    hsg_score = t['user']['hsg_score']
    symplur_rank = t['symplurrank']
    indiv.append(date)
    indiv.append(infleuncer_name)
    indiv.append(symplur_rank)
    indiv.append(hsg_score)
    data.append(indiv)

df = pd.DataFrame(data, columns=columns)

df.to_csv('{0}.csv'.format(file_name), encoding='utf-8', index=False)

#df.write.option("sep","|").mode('overwrite').csv('gs://w2odataengineering/client_work/source/', header=False, index=False)
#df.to_csv('gs://w2odataengineering/client_work/source/{0}.csv'.format('test1'), encoding='utf-8', sep='|', index=False)


# In[ ]:



