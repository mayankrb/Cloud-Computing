Dining Concierge chatbot.

In this project we are trying to give restaurant recommendations to users based on their preferred cuisine.

To perform this task we use the following files:

1. LF0: Function to connect the frontend to the API gateway and Amazon Lex.

2. LF1: Function that gets the response from Amazon Lex and add suggestions to the SQS que

3. LF2: Function that polls for entries in SQS queue and sends message to user after performing query on opensearch index

4. ScrapingForDynamoDb: Populates DynamoDB using Yelp API  

5. Scraping+for+OpenSeach: Used to fill opensearch db with a subset of features of the restaurant data from yelp.

6. lf2_dependies : python library files required to run LF2
