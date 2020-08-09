import requests
import json
import pandas


def IndexRepairJob(esHost, oldIndexName, newIndexName, mappingPath, aliasName):
    print("Reading mapping file..")
    with open(mappingPath) as f:
        mapping = json.loads(f.read())
    createIndexUrl = "http://" + esHost + "/" + newIndexName
    print("Creating new index with url = ", createIndexUrl)
    response = requests.put(url=createIndexUrl, json=mapping).json()

    if 'acknowledged' in response and response['acknowledged'] and response['index'] == newIndexName:
        print("New index successfully created with name ", response['index'])
    else:
        print("new index not able created")
        print("aborting operations")
        print("Logging ES Response => ", response)
        return 0

    print("Started data migration from " + oldIndexName + " to " + newIndexName)
    reIndexUrl = "http://" + esHost + "/_reindex"
    reIndexBody = {"source": {"index": oldIndexName}, "dest": {"index": newIndexName}}
    print("request url is ", reIndexUrl, "request body is ", reIndexBody)
    response = requests.post(url=reIndexUrl, json=reIndexBody).json()
    if not response['failures']:
        print("Data Migration Successfully completed")
    else:
        print("Logging es response => ", response)
        print("Data Migration fail")
        rollBackOperations(esHost, newIndexName)
        return 0

    print("Started exchanging aliases")
    aliasReplaceUrl = "http://" + esHost + "/_aliases"
    aliasReplaceBody = {
        "actions": [
            {"remove": {"index": oldIndexName, "alias": aliasName}},
            {"add": {"index": newIndexName, "alias": aliasName}}
        ]
    }
    response = requests.post(url=aliasReplaceUrl, json=aliasReplaceBody).json()
    if 'acknowledged' in response and response['acknowledged']:
        print("All tasks are successfully completed.")
        print("Your application is now pointed to ", newIndexName)
    else:
        print("Logging es response => ", response)
        print("alias exchanging failed")
        rollBackOperations(esHost, newIndexName)
        return 0


def rollBackOperations(esHost, newIndexName):
    print("Rollback started")
    rollBackUrl = "http://" + esHost + "/" + newIndexName
    response = requests.delete(url=rollBackUrl).json()
    if 'acknowledged' in response and response['acknowledged']:
        print("Rollback successfully completed.")
    else:
        print("Logging es response => ", response)
        print("rollback failed")
        return 0


IndexRepairJob("localhost:9200", "indexName_v1", "indexName_v2", "mapping.json", "aliasName")
