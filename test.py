import os
import json
from avro.datafile import DataFileReader
from avro.io import DatumReader

path =  './na'

res = []

def readRawData(fileName):
    reader = DataFileReader(open(fileName, 'rb'), DatumReader())

    dict = {}

    print(fileName)

    for reading in reader:
        parsed_json = json.loads(reading["Body"])

        if 'UserSignOn'not in parsed_json['ScenarioName']:
            continue

        val = []
        for key in parsed_json:
            val.append(parsed_json[key])
        res.append(val)

    reader.close()

def getFiles(dir):
    for home, dirs, files in os.walk(dir):
        for fileName in files:
            readRawData(home + '\\' + fileName)

    csvFile ='./haha.csv'
    csvContent = open(csvFile, "a")
    for item in res:
        csvContent.write(", ".join([str(x) for x in item])+'\n')

getFiles(path)



