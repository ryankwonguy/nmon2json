#!/usr/bin/python3

#  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# YOU MUST AS A MINIUM CHANGE STUFF BELOW FOR INFLUXDB ACCESS
#  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

import sys
import json

def log(string1,string2):
    debug = False
    if debug:
        logger(string1,string2)

def logger(string1,string2):
    with open('nmon_injector.log','a') as f:
        f.write(string1 + ":" + string2 + "\n")
    return

def inject_snapshot(sample):
    timestamp = sample["timestamp"]["datetime"]
    log("timestamp",timestamp)
    try:
        os = sample["identity"]["OS"] 
    except: #Not AIX imples Linux
        os = "Linux " + sample["os_release"]["pretty_name"]
        taglist = {'host': sample['identity']['hostname'], 'os':os } 
    else: # this is AIX and POWER 
        taglist = {'host':       sample['identity']['hostname'], 
                    'os':os, 
                    'serial_no': sample['identity']['serial'],
                    'mtm':       sample['identity']['machine_type'] } 
    log("os",str(os))
    log("taglist",str(taglist))

    for section in sample.keys():
        log("section", section)
        for sub in sample[section].keys():
            log("members are type", str(type(sample[section][sub])))
            if type(sample[section][sub]) is dict:
                measurename = str(section) + "_" + str(sub)
                log("Measurement section and subsection", str(measurename));
                fieldlist = sample[section][sub]
                log("fieldlist", str(fieldlist))
                measure = { 'measurement': measurename, 'tags': taglist, 'time': timestamp, 'fields': fieldlist }
                log("SSS", "MMM")
                log("measure", str(measure))
                entry.append(measure)
            else:
                measurename = str(section)
                log("Measurement section", str(measurename))
                fieldlist = sample[section] 
                log("fieldlist", str(fieldlist))
                measure = { 'measurement': measurename, 'tags': taglist, 'time': timestamp, 'fields': fieldlist }
                log("MMM", "MMM")
                log("measure", str(measure))
                entry.append(measure)
                break
    return sample['identity']['hostname']

def push(host):
    if client.write_points(entry) == False:
        logger("write.points() to Influxdb failed length=", str(len(entry)))            
        logger("FAILED ENTRY",entry)            
    else:
        logger("Injected snapshot " + str(count) + " for " + host + " Database",  str(dbname))
        entry.clear()
    return

#  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# YOU MUST AS A MINIUM CHANGE USER AND SECRET FOR ACCESS TO INFLUXDB
#  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

host="localhost"
port=8086
user = 'USER'
password = 'SECRET'
dbname = 'nmon'

from influxdb import InfluxDBClient
client = InfluxDBClient(host, port, user, password, dbname)

batch=True

count = 0
saving = 0
text = ""
cached = 0
entry = []

for line in sys.stdin:
    log("INPUT line",line)
    if line[0:3] == "  {":
        print("saving")
        saving=1
    if saving and line[0:3] == "  }":
        count=count+1
        saving=0
        text=text + "}"
        log("Sample Dictionary TEXT size ",str(len(text)))
        host = inject_snapshot(json.loads(text))
        if batch:
            cached = cached + 1
            if cached == 500:
                print("push")
                push(host)
                cached=0
        else:
            print("push")
            push(host)
        text=""
    if saving:
        print("saved:%s"%(line))
        text=text+line

push(host)
exit()
