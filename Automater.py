import requests
import json


def IndexRepairJob(esHost, oldIndexName, newIndexName, mappingPath, aliasName):

    # Pre validation
    getIndices = esHost + "/_cat/indices"
    params = {'format': "json"}
    response = requests.get(url=getIndices, params= params).json()
    for indices in response:
        if indices['index'] == aliasName:
            print("Please check with aliasName, it should not match with any index that present in your elastic search.")
            print("Aborting script")
            return 0

    # Adding alias to oldIndex, ElasticSearch will override the alias so no worry if it already present.
    print("Adding alias to ", oldIndexName)
    addAliasUrl = esHost + "/" + oldIndexName + "/_alias/" + aliasName
    response = requests.put(url=addAliasUrl).json()
    if 'acknowledged' in response and response['acknowledged']:
        print("Alias added to index ", oldIndexName)
    else:
        print("Adding alias name to index fails")
        print("Please check your inputs that is proper or not")
        print("Logging es response => ", response)
        return 0

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
        print("Data Migration fail")
        print("Logging es response => ", response)
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
        print("alias exchanging failed")
        print("Please check with aliasName, it should not match with any index name.")
        print("Logging es response => ", response)
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
    oldIndex = "oldIndexName"
    newIndex = "newIndexName"
    mappingFilePath = "mapping.json"
    alias = "aliasName"
    IndexRepairJob(elasticSearchHost, oldIndex, newIndex, mappingFilePath, alias)
