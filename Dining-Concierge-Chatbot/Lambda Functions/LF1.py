import json
import boto3
import re

def checkalldigits(str):
    flag = True
    for ch in str:
        if ch not in ['0', '9', '8', '7', '6', '5', '4', '3', '2', '1']:
            return False
    return flag
    
def checkValidLocation(location):
    if str(location).lower() in ['queens', 'manhattan', 'staten island', 'brooklyn', 'bronx']:
        return True
    return False
       
def checkValidCuisineType(cuisine_type):
    if str(cuisine_type).lower() in ["italian", "mexican", "chinese", "indian", "thai"]:
        return True
    return False

def lambda_handler(event, context):
    # get the type of intent
    intentType = event.get("currentIntent").get("name")

    if intentType == "greetingIntent":
        response = "Hello, I am Dining-Concierge Chatbot.\
                    I can provide you recommendations for NYC restaurants.\
                    Just type 'Show me some restaurants' or 'booking'\n.\
                    What can I do for you?"
        return {
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled",
                "message": {
                    "contentType": "PlainText",
                    "content": response
                }
            }
        }

    if intentType == "diningSuggestionsIntent":
        
        #num_people and time are managed by aws itself 
        contact_info_flag = False
        location_flag = False
        cuisine_type_flag = False
        
        location = event.get("currentIntent").get("slots").get("Location")
        if location_flag==False and checkValidLocation(location):
            location_flag=True
            
        cuisine_type = event.get("currentIntent").get("slots").get("CuisineType")
        if cuisine_type_flag==False and checkValidCuisineType(cuisine_type):
            cuisine_type_flag=True
            
        num_people = event.get("currentIntent").get("slots").get("NumPeople")
        time = event.get("currentIntent").get("slots").get("Time")
        
        
        contact_info = event.get("currentIntent").get("slots").get("ContactInfo")
        if contact_info_flag==False and contact_info:
            contact_info_char_arr = []
            for ch in str(contact_info):
                if ch=='+1':
                    contact_info_char_arr.append(ch)
                if ch in ['0', '9', '8', '7', '6', '5', '4', '3', '2', '1']:
                    contact_info_char_arr.append(ch)
            contact_info = ''.join(contact_info)
            if len(contact_info)==10 and checkalldigits(str(contact_info)):
                contact_info_flag = True
            elif len(contact_info)==12 and contact_info[:2] == '+1' and checkalldigits(str(contact_info[2:])):
                contact_info_flag = True
                contact_info = contact_info[2:]
                
        '''
        while True:
            nonlocal time
            if contact_info_flag==False and len(contact_info)==10 and checkalldigits(str(contact_info)):
                break
            time = None
            time = event.get('currentIntent').get('slots').get('ContactInfo')
        '''

        if location_flag == False:
            return {
                "dialogAction": {
                    "intentName": "diningSuggestionsIntent",
                    "type": "ElicitSlot",
                    "message": {
                        "contentType": "PlainText",
                        "content": "Please tell the location in New York City where you want to dine in?\
                                    Choose one from the following : Queens, Manhattan, Staten Island, Brooklyn, Bronx"
                    },
                    "slots": {
                        "Location": location,
                        "CuisineType": cuisine_type,
                        "NumPeople": num_people,
                        "Time": time,
                        "ContactInfo": contact_info
                    },
                    
                    "slotToElicit": "Location"
                }
            }
        elif cuisine_type_flag == False:
            return {
                "dialogAction": {
                    "intentName": "diningSuggestionsIntent",
                    "type": "ElicitSlot",
                    "slotToElicit": "CuisineType",
                    "message": {
                        "contentType": "PlainText",
                        "content": "What cuisine type are you interested in?\
                                    You can try keywords from the following Mexican,\
                                    Italian, Chinese, Thai and Indian."
                    },
                    "slots": {
                        "Location": location,
                        "CuisineType": cuisine_type,
                        "NumPeople": num_people,
                        "Time": time,
                        "ContactInfo": contact_info
                    }
                }
            }
        elif num_people is None:
            return {
                "dialogAction": {
                    "intentName": "diningSuggestionsIntent",
                    "type": "ElicitSlot",
                    "slotToElicit": "NumPeople",
                    "message": {
                        "contentType": "PlainText",
                        "content": "Please tell the number of people that will be dining?"
                    },
                    "slots": {
                        "Location": location,
                        "CuisineType": cuisine_type,
                        "NumPeople": num_people,
                        "Time": time,
                        "ContactInfo": contact_info
                    }
                }
            }
        elif time is None:
            return {
                "dialogAction": {
                    "intentName": "diningSuggestionsIntent",
                    "type": "ElicitSlot",
                    "slotToElicit": "Time",
                    "message": {
                        "contentType": "PlainText",
                        "content": "Please tell the time you want to dine at."
                    },
                    "slots": {
                        "Location": location,
                        "CuisineType": cuisine_type,
                        "NumPeople": num_people,
                        "Time": time,
                        "ContactInfo": contact_info
                    }
                }
            }
        elif contact_info is None or contact_info_flag==False:
            return {
                "dialogAction": {
                    "intentName": "diningSuggestionsIntent",
                    "type": "ElicitSlot",
                    "slotToElicit": "ContactInfo",
                    "message": {
                        "contentType": "PlainText",
                        "content": "Please enter your 10 digit US phone number"
                    },
                    "slots": {
                        "Location": location,
                        "CuisineType": cuisine_type,
                        "NumPeople": num_people,
                        "Time": time,
                        "ContactInfo": contact_info
                    }
                }
            }
        else:
            sqs_client = boto3.client('sqs')
            queue_url = "https://sqs.us-east-1.amazonaws.com/931871336172/diningConciergeRequestQueue"

            response = sqs_client.send_message(
                QueueUrl=queue_url,
                DelaySeconds=5,
                MessageAttributes={
                    "Location": {
                        "DataType": 'String',
                        "StringValue": location
                    },
                    "CuisineType": {
                        "DataType": 'String',
                        "StringValue": cuisine_type,
                    },
                    "NumPeople": {
                        "DataType": 'Number',
                        "StringValue": str(num_people)

                    },
                    "Time": {
                        "DataType": 'String',
                        "StringValue": time

                    },
                    "ContactInfo": {
                        "DataType": 'Number',
                        "StringValue": str(contact_info)

                    }
                },
                MessageBody=(
                    'Values provided by the customer.'
                )
            )
            confirmation_response = "We have reveived your request. You will receive your suggestions soon via text on {}".format(contact_info)
            return {
                "dialogAction": {
                    "type": "Close",
                    "fulfillmentState": "Fulfilled",
                    "message": {
                        "contentType": "PlainText",
                        "content": confirmation_response
                    }
                }
            }
    if intentType == "thankYouIntent":
        closing_response = "Have a great time!"
        return {
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled",
                "message": {
                    "contentType": "PlainText",
                    "content": closing_response
                }
            }
        }
