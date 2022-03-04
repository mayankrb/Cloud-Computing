import json
import boto3

def lambda_handler(event, context):
    user_input_message = event.get("messages")[0].get("unstructured").get("text")
    
    client = boto3.client('lex-runtime')
    
    response = client.post_text(
        botName = "dcChatbot",
        botAlias = 'dcChat',
        userId = "id",
        inputText = user_input_message
        )
    
    #print(response)
    
    return {
        'headers' : {
            "Access-Control-Allow-Origin": '*'
        },
        'messages': [{
            'type': "unstructured", 
            'unstructured': {
                'text' : response.get("message")
            }
        }]
    }
