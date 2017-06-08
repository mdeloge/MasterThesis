#!/usr/bin/env python
import urllib2
from xml.dom.minidom import parse, parseString
import subprocess
import pandas as pd

tsv_file = open("/home/matteus/Github/AzureSyncPython/data/list.tsv","w")
tsv_file.write("TsvHttpData-1.0\n")
eof = False
url = urllib2.urlopen('https://energyiddev.blob.core.windows.net/jouleboulevard2?restype=container&comp=list')
dom = parse(url)
marker = dom.childNodes[0].childNodes[1].firstChild.nodeValue
while True:
    for elem in dom.getElementsByTagName("Blob"):
        try:
            file_url = elem.childNodes[1].firstChild.nodeValue
            md5 = elem.childNodes[2].childNodes[6].firstChild.nodeValue
            size = elem.childNodes[2].childNodes[2].firstChild.nodeValue
            tsv_file.write(file_url + "\t" + size + "\t" + md5 + "\n")
        except AttributeError as e:
            print 'Error: ' + str(e)
            
    if not eof:
        url = urllib2.urlopen('https://energyiddev.blob.core.windows.net/jouleboulevard2?restype=container&comp=list&marker='+marker)
        dom = parse(url)
        try:
            marker = dom.childNodes[0].childNodes[2].firstChild.nodeValue
            print 'New marker:\t' + str(marker)
        except AttributeError as e:
            print "Marker is None: " + str(e)
            eof = True
    else:
        break
        
print "end"
tsv_file.close()

print "\n>>>starting metadata sync"
df = pd.read_csv('https://energyiddev.blob.core.windows.net/jouleboulevard2/Jouleboulevard_metadata.csv', delimiter=';')
df.to_csv("/home/matteus/Github/AzureSyncPython/data/Jouleboulevard_metadata.csv", sep=';', index=False)
print ">>>metadata sync complete"

print "\n\n>>>sync with google cloud storage"
subprocess.call("gsutil rsync -d /home/matteus/Github/AzureSyncPython/data gs://jouleboulevard2/sync_list", shell=True)
subprocess.call("gsutil -m acl set -R -a public-read gs://jouleboulevard2/sync_list", shell=True)