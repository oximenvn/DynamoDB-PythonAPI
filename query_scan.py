from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

#Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, decimal.Decimal):
			if abs(obj)%1 > 0:
				return float(obj)
			else:
				return int(obj)
		return super(DecimalEncoder, self).default(obj)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2',endpoint_url="http://localhost:8000",
							aws_access_key_id='12345',
							aws_secret_access_key= '12345')

table = dynamodb.Table('Movies')

# print("Movies from 1985")

# response = table.query(
#     KeyConditionExpression=Key('year').eq(1985)
# )

# for i in response['Items']:
#     print(i['year'], ":", i['title'])


# print("Movies from 1992 - titles A-L, with genres and lead actor")

# response = table.query(
#     ProjectionExpression="#yr, title, info.genres, info.actors[0]",
#     ExpressionAttributeNames={ "#yr": "year" }, # Expression Attribute Names for Projection Expression only.
#     KeyConditionExpression=Key('year').eq(1992) & Key('title').between('A', 'L')
# )

# for i in response[u'Items']:
#     print(json.dumps(i, cls=DecimalEncoder))



# --------------------------SCAN-------------------------------------------


fe = Key('year').between(1950, 1959)
pe = "#yr, title, info.rating"
# Expression Attribute Names for Projection Expression only.
ean = { "#yr": "year", }
esk = None


response = table.scan(
    FilterExpression=fe,
    ProjectionExpression=pe,
    ExpressionAttributeNames=ean
    )

for i in response['Items']:
    print(json.dumps(i, cls=DecimalEncoder))

print('--------++++++++++++_______++++++++++-------------')

while 'LastEvaluatedKey' in response:
    response = table.scan(
        ProjectionExpression=pe,
        FilterExpression=fe,
        ExpressionAttributeNames= ean,
        ExclusiveStartKey=response['LastEvaluatedKey']
        )

    for i in response['Items']:
        print(json.dumps(i, cls=DecimalEncoder))