#! /home/matteus/Github/PubSubPython/ENV/bin/python
from __future__ import absolute_import
from google.cloud import pubsub
import datetime
import random
import time
import http
# https://googlecloudplatform.github.io/google-cloud-python/stable/pubsub-usage.html
# Instantiates a client
pubsub_client = pubsub.Client().from_service_account_json("LKN Muntstraat-76a6c3b6e46f.json")
my_topic = pubsub_client.topic("dataflow_data")

if my_topic.exists():
    print "Topic exists!"
    while True:
        timestamp = str(datetime.datetime.utcnow())         #timestamp in UTC
        consumption = str(1000.0 * random.random())         #consumption value
        meterId = '8B381DEF-2B6E-4749-BDDE-867D3D490F2A'        #meterID
                                                                #8B381DEF-2B6E-4749-BDDE-867D3D490F2A
        line = timestamp + ';' + consumption + ';' + meterId
        #line = "bllkbefzjbh"
        print line
        #time.sleep(1)        
        
        my_topic.publish(line)      #sending the message to pubsub
else:
    print "Topic not found ..."
    
#Fetch the IAM policy for a topic:
#policy = my_topic.get_iam_policy()

#for topic in pubsub_client.list_topics():   # API request(s)
#    print topic



#subscription = None

#for subs in my_topic.list_subscriptions():   # API request(s)
#        subscription = subs

#pulled = subscription.pull(max_messages=2)
#print pulled[0]