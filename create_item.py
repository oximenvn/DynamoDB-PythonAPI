from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from botocore.exceptions import ClientError

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

title = "The big new movie"
year = 2015

response = table.put_item(
	Item={
		'year':year,
		'title':title,
		'info':{
			'plot':"Nothing happens at all",
			'rating': decimal.Decimal(0)
		}
	}
)

print("PutItem succeeded")
print(json.dumps(response, indent=4, cls=DecimalEncoder))

try:
	response = table.get_item(
		Key={
			'year':year,
			'title':title
		}
	)
except ClientError as e:
	print(e.response['Error']['Message'])
else:
	item = response['Item']
	print('GetItem succceede:')
	print(json.dumps(item, indent=4, cls=DecimalEncoder))

response = table.update_item(
	Key={
		'year':year,
		'title':title
	},
	UpdateExpression = "set info.rating = :r, info.plot = :p, info.actors = :a",
	ExpressionAttributeValues={
		':r' : decimal.Decimal(5.5),
		':p': "Everything happens all at once.",
		':a': ["Larry","moe","Curly"]
	},
	ReturnValues="UPDATED_NEW"
)

print("UpdateItem succeeded:")
print(json.dumps(response, indent=4, cls=DecimalEncoder))

response = table.update_item(
	Key={
		'year':year,
		'title':title
	},
	UpdateExpression="set info.rating = info.rating + :val",
	ExpressionAttributeValues={
		':val': decimal.Decimal(1)
	},
	ReturnValues="UPDATED_NEW"
)

print("UpdateItem succeeded:")
print(json.dumps(response, indent=4, cls=DecimalEncoder))

print("Attempting conditional update...")

try:
	response = table.update_item(
		Key={
			'year':year,
			'title':title
		},
		ConditionExpression="size(info.actors) >= :num",
		UpdateExpression="remove info.actors[0]",
		ExpressionAttributeValues={
			':num': 3
		},
		ReturnValues="UPDATED_NEW"
	)
except ClientError as e:
	if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
		print(e.response['Error']['Message'])
	else:
		raise
else:
	print("UpdateItem succeeded")
	print(json.dumps(response, indent=4, cls=DecimalEncoder))

print("Attempting a conditional delete...")

try:
    response = table.delete_item(
        Key={
            'year': year,
            'title': title
        },
        ConditionExpression="info.rating = :val",
        ExpressionAttributeValues= {
            ":val": decimal.Decimal(6.5)
        }
    )
except ClientError as e:
    if e.response['Error']['Code'] == "ConditionalCheckFailedException":
        print(e.response['Error']['Message'])
    else:
        raise
else:
    print("DeleteItem succeeded:")
    print(json.dumps(response, indent=4, cls=DecimalEncoder))