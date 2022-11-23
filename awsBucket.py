"""This program is for photo storage and metadata tracking.

Written by Zack Mason on 8/8/2021

USER GUIDE FOR THIS APPLICATION

The metadata management program allows a user to not only 
store their photos securely in the cloud but to also keep 
track of their photos’ metadata. The following will help 
the user of this program navigate the program and use it 
effectively.

1. Creating your metadata.
	Open up the JSON file included in the package. Update 
	the metadata accordingly.
	
2. Create your DynamoDB Table
	Run the following program to create your DynamoDB table:
	MoviesCreateTable.py
	
3. Creating a place for your photos.
	Before you do anything, you will need to create a place 
	to store your photos. In order to do this, you will need 
	to start up the program and select the very first option 
	by typing “a.” The option is labeled “create a new storage 
	bucket.” The program will then ask you to input the name 
	of your new bucket. Make sure your name includes only 
	lower case letters and hyphens. The program will then add 
	a suffix of random numbers to make sure that each bucket 
	name is unique.
	
4. Uploading photos
	Once you have a bucket created you can start uploading 
	photos. Select option “b” from the main menu. The program 
	will print the available buckets and ask which bucket you 
	would like to use. Feel free to copy and paste from the 
	command prompt. The program will then ask the user what 
	the name of the photo will be. Input anything that you 
	like. This will be the name of the file once it is 
	uploaded. Then the program will ask for the path to the 
	current file. Input this information and the program will 
	take care of the rest. If successful, the program will 
	print that the object was placed and print the bucket name.
	
5. Removing photos
	To remove a photo, select option “c” from the main menu. 
	The program will ask which bucket the photo is in. It will 
	then ask for the name of the file. Input this information 
	correctly and the program will let you know that the object 
	is deleted.
	
6. Removing a photo bucket
	To remove a photo bucket, select option “d” from the main 
	menu. The program will then ask which bucket you would like 
	to remove. Make sure that this bucket is empty or the delete 
	option will not work. If the bucket is empty, however, the 
	program will let the user know that the bucket has been 
	successfully deleted.
	
7. Copying photos from one bucket to another
	To copy photos select option “e” from the main menu. The 
	program will ask which bucket the photo is in, which photo 
	you would like to copy, and which bucket the photo should be 
	copied to. All of the buckets available will be printed to 
	the screen and the contents of the bucket will also be 
	displayed so that you can see what your options are.
	
8. Downloading your photos
	To download a photo from its storage location select option 
	“f” from the main menu. The program will ask which bucket 
	you would like to access. It will then print the available 
	buckets. Type in the name of the bucket you would like to 
	access. The program will then display the contents of the 
	bucket and ask which object you would like to download. 
	Finally, the program will ask for the file destination. 
	Enter the full path and file name.
	
9. Search for metadata
	To find photo metadata you will need to search by the photo 
	ID. First select option “g” from the main menu. The program 
	will then ask you to input the photo ID of the photo you 
	would like metadata for. Once you do this, it will print 
	out the associated photo metadata.
	
10. Update metadata
	If you have updated your metadata file and want to refresh 
	it before querying your database, enter “h” and the 
	application will update the metadata for you.

11. Exit the program
	To exit the program, choose “j” from the main menu and the 
	program will close.

"""

from __future__ import print_function # Python 2/3 compatibility

import MoviesLoadData

import os
import sys
from random import randint
import logging
from datetime import datetime
import boto3

import json
import decimal
import boto3.dynamodb.conditions
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

def create_new_bucket():
    """Create a new Amazon S3 bucket

    :param bucket_name: string
    :param last_name: string
    :param first_name: string
    :param suffix: string
    :return: List of bucket objects. If error, return None.
    """
    conn = boto3.client('s3')
    suffix = str(randint(100000, 999999))
    first_name = input("Enter your bucket name: ")
    #last_name = input("Enter your last name: ")
    bucket_name = first_name + "-" + suffix
    print("Bucket name = " + bucket_name)
    conn.create_bucket(Bucket=bucket_name)
    show_buckets()
    print(bucket_name + " bucket created!")

def put_object_driver():
    show_buckets() # Calls the show_buckets method
    bucket_selection = input("Which bucket would you like to put the object in?\n")
    dest_object_name = input("What is the name of the object?\n")
    #Option to manually enter data.
    #filename = b'This is the data to store in the S3 object.'
    print("This is your current working directory: " + str(os.getcwd()))
    filename = input ("Enter the path to where the object is now\n")
    # Put the object into the bucket
    success = put_object(bucket_selection, dest_object_name, filename)
    if success:
        logging.info(f'Added {dest_object_name} to {bucket_selection}')
        print("Object Placed in " + bucket_selection)

def put_object(dest_bucket_name, dest_object_name, src_data):
    """Add an object to an Amazon S3 bucket
    The src_data argument must be of type bytes or a string that references
    a file specification.

    :param dest_bucket_name: string
    :param dest_object_name: string
    :param src_data: bytes of data or string reference to file spec
    :return: True if src_data was added to dest_bucket/dest_object, otherwise
    False
    """
    # Confirm data entry
    print("Destination bucket loaded as: " + dest_bucket_name)
    print("Destination object loaded as: " + dest_object_name)
    print("src_data loaded as : " + str(src_data))
    #Check whether data is binary or string path
    if isinstance(src_data, bytes):
        object_data = src_data
    elif isinstance(src_data, str):
        try:
            #Open file in binary mode
            object_data = open(src_data, 'rb')
            # possible FileNotFoundError/IOError exception
        except OSError as bad_kitty:
            logging.error(bad_kitty)
            return False
    else:
        logging.error('Type of ' + str(type(src_data)) +
                      ' for the argument \'src_data\' is not supported.')
        return False

    # Put the object
    conn = boto3.client('s3')
    try:
        conn.put_object(Bucket=dest_bucket_name, Key=dest_object_name, Body=object_data)
    except ClientError as bad_kitty:
        # AllAccessDisabled error == bucket not found
        # NoSuchKey or InvalidRequest error == (dest bucket/obj == src bucket/obj)
        logging.error(bad_kitty)
        return False
    finally:
        if isinstance(src_data, str):
            object_data.close()
    return True

def show_buckets():
    """List the buckets in an Amazon S3 instance"""
    # Create an S3 client
    conn = boto3.client('s3')
    # Call s3 to list current buckets
    response = conn.list_buckets()
    # Get a list of all bucket names from the response
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    # Print out the bucket list
    print ("Bucket List: %s" % buckets)

def delete_object():
    """delete object from an Amazon S3 bucket

    :param bucket_name: string
    :param object_name: string
    """
    #Call the show_buckets method
    show_buckets()
    #get user input
    bucket_name = input("Which bucket is the object in?\n")
    list_bucket_objects(bucket_name)
    object_name = input("Which object would you like to delete?\n")
    conn = boto3.resource('s3')
    #delete the object
    conn.Object(bucket_name, object_name).delete()
    print ("Object Deleted: " + str(object_name))

def delete_bucket():
    """Delete bucket from Amazon S3 instance

    :param bucket_name: string
    :return: boolean objects. If error, return False.
    """
    show_buckets()
    bucket_name = input("Which bucket would you like to delete?\n")
    # Delete the bucket
    conn = boto3.client('s3')
    try:
        conn.delete_bucket(Bucket=bucket_name)
        print("Bucket Deleted")
    except ClientError as bad_kitty:
        logging.error(bad_kitty)
        return False
    return True

def copy_object():
    """Copy an Amazon S3 bucket object

    :param src_bucket_name: string
    :param src_object_name: string
    :param dest_bucket_name: string. Must already exist.
    :param dest_object_name: string. If dest bucket/object exists, it is
    overwritten. Default: src_object_name
    :return: True if object was copied, otherwise False
    """
    #call show_buckets
    show_buckets()
    #get user input
    bucket_name = input("Which bucket is the object in?")
    dest_bucket_name = input("Which bucket is the destination?")
    #list objects in the bucket and get more user input
    list_bucket_objects(bucket_name)
    src_object_name = input("What is the name of the object you would like to copy?")

    # Construct source bucket/object parameter
    copy_source = {'Bucket': bucket_name, 'Key': src_object_name}


    # Copy the object
    conn = boto3.client('s3')
    try:
        conn.copy_object(CopySource=copy_source, Bucket=dest_bucket_name,
                       Key=src_object_name)
    except ClientError as bad_kitty:
        logging.error(bad_kitty)
        return False
    print ("Object Copied")
    return True

def download_object():
    """Download object from an Amazon S3 bucket

    :param bucket_name: string
    :param object_name: string
    :param file_name: string
    :param conn: client
    """
    #call show buckets method and get user input
    show_buckets()
    bucket_name = input("Which bucket is the object in?\n")
    list_bucket_objects(bucket_name)
    object_name = input("What is the name of the object that you want to download?\n")
    file_name = input("What is the destination for this downloaded file?\n")
    conn = boto3.client('s3')
    try:
        conn.download_file(bucket_name, object_name, file_name)
    except ClientError as bad_kitty:
        # AllAccessDisabled error == bucket or object not found
        logging.error(bad_kitty)
    print ("Object Downloaded: " + object_name)

def list_bucket_objects(bucket_name):
    """List the objects in an Amazon S3 bucket

    :param bucket_name: string
    :return: List of bucket objects. If error, return None.
    """
    print("Bucket name imported as: "+ bucket_name)
    print("Listing bucket objects...")
    # Retrieve the list of bucket objects
    conn = boto3.client('s3')
    try:
        response = conn.list_objects_v2(Bucket=bucket_name)
    except ClientError as bad_kitty:
        # AllAccessDisabled error == bucket not found
        logging.error(bad_kitty)
        return None
    return response['Contents']

def get_metadata():
    """class_search method. Gets user input and builds a search query
    based on what the user has entered. Also deals with errors caused
    by erroneous user input.

    :param user_subject: string
    :param user_catalog_nbr: string
    :param subject_length: int
    :param catalog_length: int
    """

    #FileName = input("Enter the filename of the photo: \n").upper()
    #filename_length = len(FileName)
    user_catalog_nbr = input("Enter the photo ID number: \n")
    catalog_length = len(user_catalog_nbr)

   # if (int(filename_length) >= 1 and int(catalog_length >= 1)):
    #    table_name = "Courses"
        
    if (int(catalog_length >= 1)):
        table_name = "Courses"
        
        # Creating the DynamoDB Table Resource
        dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
        table = dynamodb.Table(table_name)

        resp = table.scan(
            FilterExpression=Attr('PhotoNumber').eq(user_catalog_nbr)
            )

        print("The query returned the following items:")
        for item in resp['Items']:
            print(item)
    else:
        print("You need to enter something to search.\n")

def update_metadata():
    print("Getting updated metadata")
    MoviesLoadData
    print("Metadata updated")

def exit_program():
    """Prints datetime and exits program"""
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)
    sys.exit()

def main_menu(argument):
    """Calls function based on user choice.

    :param func: string
    :param switcher: dictionary
    :return: Call function indicated in switcher
    """
    #dictionary that helps act as a switch
    switcher = {
        "a": create_new_bucket,
        "b": put_object_driver,
        "c": delete_object,
        "d": delete_bucket,
        "e": copy_object,
        "f": download_object,
        "g": get_metadata,
        "h": update_metadata,
        "j": exit_program
    }

    # Get the function from switcher dictionary
    func = switcher.get(argument, "nothing")
    # Execute the function
    return func()


def main():
    """Main method. Prints a menu for the user and calls main_menu function.

    :param user_choice: string
    """
    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')
    #print out the main menu

    print ("Welcome to the metadata tracking application. ")
    print("Please choose one of the following options to continue: \n")
    print("a. Create a new storage bucket")
    print("b. Put object in an existing bucket")
    print("c. Delete object from an existing bucket")
    print("d. Delete a bucket")
    print("e. Copy an object from one bucket to another")
    print("f. Download an existing object from a bucket")
    print("g. Search database for object metadata")
    print("h. Update metadata")
    print("j. Exit the program")
    print("\n")
     
    choice_list = ["a","b","c","d","e","f","g","h","j"]

    user_choice = input("Enter the letter of your choice: ")
    user_choice = user_choice.lower()
    if user_choice in choice_list:
        main_menu(user_choice)
    else:
        print("Invalid selection")


if __name__ == '__main__':
    main()
