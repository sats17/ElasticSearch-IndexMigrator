import requests
import json


def IndexRepairJob(esHost, oldIndexName, newIndexName, mappingPath, aliasName):
    print("Reading mapping file..")
    with open(mappingPath) as f:
        mapping = json.loads(f.read())

    # New Index Creation
    createIndexUrl = esHost + "/" + newIndexName
    print("Creating new index with url = ", createIndexUrl)
    response = requests.put(url=createIndexUrl, json=mapping).json()
    if 'acknowledged' in response and response['acknowledged'] and response['index'] == newIndexName:
        print("New index successfully created with name ", response['index'])
    else:
        print("new index not able created")
        print("aborting operations")
        print("Logging ES Response => ", response)
        return 0

    # Index data migration
    print("Started data migration from " + oldIndexName + " to " + newIndexName)
    reIndexUrl = esHost + "/_reindex"
    reIndexBody = {"source": {"index": oldIndexName}, "dest": {"index": newIndexName}}
    print("Data migration request url is ", reIndexUrl, " || request body is ", reIndexBody)
    response = requests.post(url=reIndexUrl, json=reIndexBody).json()
    if not response['failures']:
        print("Data Migration Successfully completed")
    else:
        print("Logging es response => ", response)
        print("Data Migration fail")
        rollBackOperations(esHost, newIndexName)
        return 0

    # Exchanging aliases
    print("Started exchanging aliases")
    aliasReplaceUrl = esHost + "/_aliases"
    aliasReplaceBody = {
        "actions": [
            {"remove": {"index": oldIndexName, "alias": aliasName}},
            {"add": {"index": newIndexName, "alias": aliasName}}
        ]
    }
    print("Alias exchanging request url is ", aliasReplaceUrl, " || request body is ", aliasReplaceBody)
    response = requests.post(url=aliasReplaceUrl, json=aliasReplaceBody).json()
    if 'acknowledged' in response and response['acknowledged']:
        print("Exchanging aliases successfully completed")
        print("All tasks are successfully completed.")
        print("Your application is now pointed to index", newIndexName)
    else:
        print("Logging es response => ", response)
        print("alias exchanging failed")
        rollBackOperations(esHost, newIndexName)
        return 0


def rollBackOperations(esHost, newIndexName):
    print("Rollback started")
    rollBackUrl = esHost + "/" + newIndexName
    response = requests.delete(url=rollBackUrl).json()
    if 'acknowledged' in response and response['acknowledged']:
        print("Rollback successfully completed.")
    else:
        print("Logging es response => ", response)
        print("rollback failed")
        return 0


if __name__ == "__main__":
    elasticSearchHost = "http://localhost:9200"
    oldIndex = "restaurants_v3"
    newIndex = "restaurants_v4"
    mappingFilePath = "mapping.json"
    alias = "restaurants"
    IndexRepairJob(elasticSearchHost, oldIndex, newIndex, mappingFilePath, alias)
