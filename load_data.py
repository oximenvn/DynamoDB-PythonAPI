from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal

dynamodb = boto3.resource('dynamodb', region_name='us-west-2',endpoint_url="http://localhost:8000",
							aws_access_key_id='12345',
							aws_secret_access_key= '12345')

table = dynamodb.Table('Movies')

with open("moviedata.json") as json_file:
	movies = json.load(json_file, parse_float = decimal.Decimal)
	for movie in movies:
		year = int(movie['year'])
		title = movie['title']
		info = movie['info']

		print("Adding movie:", year, title)

		table.put_item(
			Item={
				'year':year,
				'title':title,
				'infp':info,
			}
		)