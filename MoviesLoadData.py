from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal

dynamodb = boto3.resource("dynamodb",region_name="us-east-1")

table = dynamodb.Table("Courses")

with open("courseData.json") as json_file:
    courses = json.load(json_file, parse_float = decimal.Decimal)
    for course in courses:
        
        PhotoNumber = course["PhotoNumber"]
        Description = course["Description"]
        FileName = course["FileName"]
        Photographer = course["Photographer"]
        Date = course["Date"]
        Title = course["Title"]
        Location = course["Location"]
        print("Adding photo metadata:", str(PhotoNumber), Description, FileName, Photographer, Date, Title, Location)

        table.put_item(
            Item={
               "PhotoNumber": str(PhotoNumber),
               "Description": Description,
               "FileName": FileName,
               "Photographer": Photographer,
               "Date": Date,
               "Title": Title,
               "Location": Location
               
            }
        )
        