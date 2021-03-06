
# coding: utf-8

# In[1]:


import json
import boto3
import requests
import datetime
from decimal import Decimal
from time import sleep
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from aws_requests_auth.aws_auth import AWSRequestsAuth


# In[2]:


region = 'us-east-1'
service='es'
aws_access_key_id='********************'
aws_secret_access_key='****************************************'
osEndPoint = 'search-dining-concierge-opensearch-atxceuj374yilh2peqm6gl6qni.us-east-1.es.amazonaws.com'

auth = AWSRequestsAuth(aws_access_key = aws_access_key_id, aws_secret_access_key = aws_secret_access_key,
    aws_region = region, aws_service = service, aws_host = osEndPoint)


# In[3]:


os = OpenSearch(
    hosts = [{'host': osEndPoint, 'port':443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class= RequestsHttpConnection
)


# In[4]:


restaurants_dict = {}
zipCodeDictionary = {
    '10001': 'manhattan', '10002': 'manhattan', '10003': 'manhattan', '10004': 'manhattan', 
    '10005': 'manhattan', '10006': 'manhattan', '10007': 'manhattan', '10009': 'manhattan', 
    '10010': 'manhattan', '10011': 'manhattan', '10012': 'manhattan', '10013': 'manhattan', 
    '10014': 'manhattan', '10015': 'manhattan', '10016': 'manhattan', '10017': 'manhattan', 
    '10018': 'manhattan', '10019': 'manhattan', '10020': 'manhattan', '10021': 'manhattan', 
    '10022': 'manhattan', '10023': 'manhattan', '10024': 'manhattan', '10025': 'manhattan',
    '10026': 'manhattan', '10027': 'manhattan', '10028': 'manhattan', '10029': 'manhattan',
    '10030': 'manhattan', '10031': 'manhattan', '10032': 'manhattan', '10033': 'manhattan',
    '10034': 'manhattan', '10035': 'manhattan', '10036': 'manhattan', '10037': 'manhattan',
    '10038': 'manhattan', '10039': 'manhattan', '10040': 'manhattan', '10041': 'manhattan', 
    '10044': 'manhattan', '10045': 'manhattan', '10048': 'manhattan', '10055': 'manhattan',
    '10060': 'manhattan', '10069': 'manhattan', '10090': 'manhattan', '10095': 'manhattan',
    '10098': 'manhattan', '10099': 'manhattan', '10103': 'manhattan', '10104': 'manhattan',
    '10105': 'manhattan', '10106': 'manhattan', '10107': 'manhattan', '10110': 'manhattan',
    '10111': 'manhattan', '10112': 'manhattan', '10115': 'manhattan', '10118': 'manhattan',
    '10119': 'manhattan', '10120': 'manhattan', '10121': 'manhattan', '10122': 'manhattan',
    '10123': 'manhattan', '10128': 'manhattan', '10151': 'manhattan', '10152': 'manhattan',
    '10153': 'manhattan', '10154': 'manhattan', '10155': 'manhattan', '10158': 'manhattan',
    '10161': 'manhattan', '10162': 'manhattan', '10165': 'manhattan', '10166': 'manhattan',
    '10167': 'manhattan', '10168': 'manhattan', '10169': 'manhattan', '10170': 'manhattan',
    '10171': 'manhattan', '10172': 'manhattan', '10173': 'manhattan', '10174': 'manhattan',
    '10175': 'manhattan', '10176': 'manhattan', '10177': 'manhattan', '10178': 'manhattan',
    '10199': 'manhattan', '10270': 'manhattan', '10271': 'manhattan', '10278': 'manhattan',
    '10279': 'manhattan', '10280': 'manhattan', '10281': 'manhattan', '10282': 'manhattan',
    '10451': 'bronx', '10452': 'bronx', '10453': 'bronx', '10454': 'bronx', '10455': 'bronx',
    '10456': 'bronx', '10457': 'bronx', '10458': 'bronx', '10459': 'bronx', '10460': 'bronx',
    '10461': 'bronx', '10462': 'bronx', '10463': 'bronx', '10464': 'bronx', '10465': 'bronx',
    '10466': 'bronx', '10467': 'bronx', '10468': 'bronx', '10469': 'bronx', '10470': 'bronx',
    '10471': 'bronx', '10472': 'bronx', '10473': 'bronx', '10474': 'bronx', '10475': 'bronx',
    '11201': 'brooklyn', '11203': 'brooklyn', '11204': 'brooklyn', '11205': 'brooklyn', '11206': 'brooklyn',
    '11207': 'brooklyn', '11208': 'brooklyn', '11209': 'brooklyn', '11210': 'brooklyn', '11211': 'brooklyn',
    '11212': 'brooklyn', '11213': 'brooklyn', '11214': 'brooklyn', '11215': 'brooklyn', '11216': 'brooklyn',
    '11217': 'brooklyn', '11218': 'brooklyn', '11219': 'brooklyn', '11220': 'brooklyn', '11221': 'brooklyn',
    '11222': 'brooklyn', '11223': 'brooklyn', '11224': 'brooklyn', '11225': 'brooklyn', '11226': 'brooklyn',
    '11228': 'brooklyn', '11229': 'brooklyn', '11230': 'brooklyn', '11231': 'brooklyn', '11232': 'brooklyn',
    '11233': 'brooklyn', '11234': 'brooklyn', '11235': 'brooklyn', '11236': 'brooklyn', '11237': 'brooklyn',
    '11238': 'brooklyn', '11239': 'brooklyn', '11241': 'brooklyn', '11242': 'brooklyn', '11243': 'brooklyn',
    '11249': 'brooklyn', '11252': 'brooklyn', '11256': 'brooklyn',
    '10301': 'staten', '10302': 'staten', '10303': 'staten', '10304': 'staten', '10305': 'staten', 
    '10306': 'staten', '10307': 'staten', '10308': 'staten', '10309': 'staten', '10310': 'staten',
    '10311': 'staten', '10312': 'staten', '10314': 'staten', 
    '11004': 'queens', '11101': 'queens', '11102': 'queens', '11103': 'queens', '11104': 'queens',
    '11105': 'queens', '11106': 'queens', '11109': 'queens', '11351': 'queens', '11354': 'queens',
    '11355': 'queens', '11356': 'queens', '11357': 'queens', '11358': 'queens', '11359': 'queens',
    '11360': 'queens', '11361': 'queens', '11362': 'queens', '11363': 'queens', '11364': 'queens', 
    '11365': 'queens', '11366': 'queens', '11367': 'queens', '11368': 'queens', '11369': 'queens', 
    '11370': 'queens', '11371': 'queens', '11372': 'queens', '11373': 'queens', '11374': 'queens',
    '11375': 'queens', '11377': 'queens', '11378': 'queens', '11379': 'queens', '11385': 'queens',
    '11411': 'queens', '11412': 'queens', '11413': 'queens', '11414': 'queens', '11415': 'queens', 
    '11416': 'queens', '11417': 'queens', '11418': 'queens', '11419': 'queens', '11420': 'queens', 
    '11421': 'queens', '11422': 'queens', '11423': 'queens', '11426': 'queens', '11427': 'queens', 
    '11428': 'queens', '11429': 'queens', '11430': 'queens', '11432': 'queens', '11433': 'queens', 
    '11434': 'queens', '11435': 'queens', '11436': 'queens', '11691': 'queens', '11692': 'queens', 
    '11693': 'queens', '11694': 'queens', '11697': 'queens'
}


# In[5]:


def addRestaurant(data, cuisineType, restaurants_dict=restaurants_dict):
    for record in data:
        new_record = {}
        try:
            if record['id'] in restaurants_dict.keys():
                continue
            new_record['Cuisine'] = cuisineType
            new_record['Business ID'] = str(record["id"])
            new_record["Zip Code"] = str(record['location']['zip_code'])
            key = new_record['Zip Code']
            if key in zipCodeDictionary.keys():
                new_record['Location'] = zipCodeDictionary[new_record['Zip Code']]
            else:
                new_record['Location'] = 'Unknown'
            sleep(0.001)
            os.index(index='yelp-restaurants', doc_type='Restaurants', body=new_record)
        except Exception as e:
            print(e)


# In[6]:


batch_size = 50
cuisines = ["Italian", "Mexican", "Chinese", "Indian", "Thai"]
headers = {'Authorization': 'Bearer ********************************************************************************************************************************'}


# In[7]:


#using an loop for cuisines doesn't work
cuisine = 'Italian'
location = "New York City"
for i in range(0, 1000, batch_size):
    params = {
        'location': location,
        'offset': i,
        'limit':batch_size,
        'term':cuisine + ' restaurants'
    }
    response = requests.get(
        "https://api.yelp.com/v3/businesses/search",
        headers = headers,
        params = params
    )
    response_batch = response.json()
    #print(response_batch)
    #print(i)
    addRestaurant(response_batch["businesses"], cuisine, restaurants_dict)


# In[8]:


cuisine = 'Mexican'
location = "New York City"
for i in range(0, 1000, batch_size):
    params = {
        'location': location,
        'offset': i,
        'limit':batch_size,
        'term':cuisine + ' restaurants'
    }
    response = requests.get(
        "https://api.yelp.com/v3/businesses/search",
        headers = headers,
        params = params
    )
    response_batch = response.json()
    #print(response_batch)
    addRestaurant(response_batch["businesses"], cuisine, restaurants_dict)


# In[9]:


cuisine = 'Indian'
location = "New York City"
for i in range(0, 1000, batch_size):
    params = {
        'location': location,
        'offset': i,
        'limit':batch_size,
        'term':cuisine + ' restaurants'
    }
    response = requests.get(
        "https://api.yelp.com/v3/businesses/search",
        headers = headers,
        params = params
    )
    response_batch = response.json()
    #print(response_batch)
    addRestaurant(response_batch["businesses"], cuisine, restaurants_dict)


# In[10]:


cuisine = 'Chinese'
location = "New York City"
for i in range(0, 1000, batch_size):
    params = {
        'location': location,
        'offset': i,
        'limit':batch_size,
        'term':cuisine + ' restaurants'
    }
    response = requests.get(
        "https://api.yelp.com/v3/businesses/search",
        headers = headers,
        params = params
    )
    response_batch = response.json()
    #print(response_batch)
    addRestaurant(response_batch["businesses"], cuisine, restaurants_dict)


# In[11]:


cuisine = 'Thai'
location = "New York City"
for i in range(0, 1000, batch_size):
    params = {
        'location': location,
        'offset': i,
        'limit':batch_size,
        'term':cuisine + ' restaurants'
    }
    response = requests.get(
        "https://api.yelp.com/v3/businesses/search",
        headers = headers,
        params = params
    )
    response_batch = response.json()
    #print(response_batch)
    addRestaurant(response_batch["businesses"], cuisine, restaurants_dict)

