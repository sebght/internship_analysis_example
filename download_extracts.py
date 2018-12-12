# -*- coding: utf-8 -*-
"""
Written and Designed by Sébastien Gahat
"""
import boto3
import botocore
import os
import time

debut=time.clock()

PATH = os.path.join(os.path.dirname(__file__), 'basedump')
os.makedirs(PATH, exist_ok=True)

ACCESS_ID='id à compléter'
ACCESS_KEY='key à compléter'

s3 = boto3.resource('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key=ACCESS_KEY)
try:
    s3.Bucket('bl-basedumps').download_file('message.txt', os.path.join(PATH,'message.txt'))
    s3.Bucket('bl-basedumps').download_file('message.csv', os.path.join(PATH,'message.csv'))
    s3.Bucket('bl-basedumps').download_file('addresses.csv', os.path.join(PATH,'addresses.csv'))
    s3.Bucket('bl-basedumps').download_file('order-offers.csv', os.path.join(PATH,'order-offers.csv'))
    s3.Bucket('bl-basedumps').download_file('order.csv', os.path.join(PATH,'order.csv'))
    s3.Bucket('bl-basedumps').download_file('product.csv', os.path.join(PATH,'product.csv'))
    s3.Bucket('bl-basedumps').download_file('quotations.csv', os.path.join(PATH,'quotations.csv'))
    s3.Bucket('bl-basedumps').download_file('shop.csv', os.path.join(PATH,'shop.csv'))
    s3.Bucket('bl-basedumps').download_file('users.csv', os.path.join(PATH,'users.csv'))
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise
        
#for object in s3.Bucket('bl-basedumps').objects.all():
#    print(object)
    
fin=time.clock()
print("le telechargement des extracts a pris ",fin-debut," secondes")