import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import random

def lambda_handler(event, context):
    sqs_client = boto3.client('sqs')
    sns_client = boto3.client('sns')
    dynamo_database = boto3.resource('dynamodb')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/931871336172/diningConciergeRequestQueue'
    
    region = 'us-east-1'
    service = 'es'
    aws_access_key_id = '********************'
    aws_secret_access_key='****************************************'
    os_endpoint = 'search-dining-concierge-opensearch-atxceuj374yilh2peqm6gl6qni.us-east-1.es.amazonaws.com'
    
    auth = AWS4Auth(aws_access_key_id, aws_secret_access_key, region, service)
    
    os = OpenSearch(
        hosts = [{
            'host': os_endpoint, 
            'port': '443'
        }],
        http_auth = auth, 
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
        )
    
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        VisibilityTimeout=0,
        MessageAttributeNames=['All'],
        WaitTimeSeconds=0
    )
    #print(response)
    try:
        message = response['Messages'][0]
        cuisine_type = message['MessageAttributes'].get('CuisineType').get('StringValue').lower().capitalize()
        contact_info = '+1' + str(message['MessageAttributes'].get('ContactInfo').get('StringValue'))
        location = message['MessageAttributes'].get('Location').get('StringValue')
        results = os.search(
            index='yelp-restaurants',
            body = {
                'query': {
                    'match' : {
                        'Cuisine': cuisine_type
                    }
                }
            }
        )
        #select top 15 and provide 5 randomly from them
        candidates_ids = []
        for record in results['hits']['hits']:
            candidates_ids.append(record['_source'].get('Business ID'))
        table = dynamo_database.Table('yelp-restaurants')
        randomly_chosen_restaurant_id = random.choice(candidates_ids)
        
        #print("randomly_chosen_restaurant_id and type = ", randomly_chosen_restaurant_id, type(randomly_chosen_restaurant_id))
        chosen_restaurant = table.get_item(
            Key = {'Business ID' : randomly_chosen_restaurant_id}
        )
        #if chosen_restaurant==None:
        #    print("Null returned")
        #else:
        #    print("something was returned")
        #print("chosen_restaurant: ", chosen_restaurant)
        chosen_restaurant = chosen_restaurant["Item"]
        #print("chosen_restaurant item = ", chosen_restaurant)
        id = chosen_restaurant['Business ID']
        name = chosen_restaurant['Name']
        address = chosen_restaurant['Address']
        num_reviews = chosen_restaurant['Number of Reviews']
        rating = chosen_restaurant['Rating']
        
        
        recommendation_message = "Here is the recommendation for you\
        {}. It has an average rating of {} with {} reviews. The address is : {}.\
        This text was sent to: {}".format(name, rating, num_reviews, address, contact_info) 
        #print(recommendation_message)
        messageSent = sns_client.publish(PhoneNumber=contact_info, Message=recommendation_message)
        #print(messageSent)
        receipt_handle = message['ReceiptHandle']
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle = receipt_handle
        )
        
        return{
            'statusCode':200,
            'body': messageSent
        } 
        
    except Exception as e:
        print("Error", e)
        return {
            'statusCode': 200,
            'body': 'None'
        }
