#!/usr/bin/python2.7
#
# Assignment5 Interface
# Name: Weichi Zhao
#

from pymongo import MongoClient
import os
import sys
import json
import math
import re

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    temp = collection.find({"city":re.compile(cityToSearch,re.IGNORECASE)})
    output = open(saveLocation1,'w')
    for i in temp:
        output.write(i["name"].encode('utf-8').encode('string_escape').upper()+
                     "$"+i["full_address"].encode('utf-8').encode('string_escape').upper()+
                     "$"+i["city"].encode('utf-8').encode('string_escape').upper()+
                     "$"+i["state"].encode('utf-8').encode('string_escape').upper()+
                     "\n")
    output.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    temp = collection.find()
    output = open(saveLocation2,'w')
    myLat = float(myLocation[0])
    myLon = float(myLocation[1])
    op = []
    for i in temp:
        categories = i["categories"]
        lat = i["latitude"]
        lon = i["longitude"]

        #Calculate distance
        phi1 = math.radians(myLat)
        phi2 = math.radians(lat)
        delphi = math.radians(lat - myLat)
        dellambda = math.radians(lon - myLon)
        temp1 = math.sin(delphi / 2) * math.sin(delphi / 2) + math.cos(phi1) * math.cos(phi2) * math.sin(dellambda / 2) * math.sin(dellambda / 2)
        temp2 = 2 * math.atan2(math.sqrt(temp1), math.sqrt(1 - temp1))
        this_distance = 3959*temp2

        if maxDistance >= this_distance:
            if len(list(set(categoriesToSearch)& set(categories)))!=0:
                op.append(i["name"].encode('utf-8').upper())
    for i in op:
        output.write(i + "\n")
    output.close()




