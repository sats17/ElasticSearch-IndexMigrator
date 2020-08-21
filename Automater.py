import requests
import json

__author__ = "sats17"


def IndexRepairJob(esHost, oldIndexName, newIndexName, indexConfigPath, aliasName, isRemoveOldIndex):
    # Pre validation
    getIndices = esHost + "/_cat/indices"
    params = {'format': "json"}
    print("Pre validation started...")
    try:
        response = requests.get(url=getIndices, params=params).json()
        for indices in response:
            if indices['index'] == aliasName:
                exceptionMessage = ExceptionMessageCreator("Please check your aliasName and indexes, it should not "
                                                           "match with any index that present in your elastic search.")
                raise SystemExit(exceptionMessage)
    except requests.exceptions.RequestException as e:
        customMessage = "Pre validation failed\n"
        errorMessage = "Error Response = {0}".format(str(e))
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        raise SystemExit(errorResponse)

    # Adding alias to oldIndex, ElasticSearch will override the alias so no worry if it already present.
    print("Adding alias to index ", oldIndexName)
    try:
        addAliasUrl = esHost + "/" + oldIndexName + "/_alias/" + aliasName
        response = requests.put(url=addAliasUrl).json()
        if 'acknowledged' in response and response['acknowledged']:
            print("Alias added to index ", oldIndexName)
        else:
            customMessage = "Adding alias name to index fails, Please check your inputs that is proper or not\n"
            errorMessage = "ElasticSearch Response = {0}".format(str(response))
            errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
            raise SystemExit(errorResponse)
    except requests.exceptions.RequestException as e:
        customMessage = "Alias adding to index {0} failed\n".format(oldIndexName)
        errorMessage = "Error Response = {0}".format(str(e))
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        raise SystemExit(errorResponse)

    # Reading JSON file
    print("Reading mapping file..")
    try:
        with open(indexConfigPath) as f:
            mapping = json.loads(f.read())
    except IOError as e:
        customMessage = "Error found at reading index configuration file, Cannot read index configuration file"
        errorMessage = "Error Response = {0}".format(str(e))
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        raise SystemExit(errorResponse)
    except json.decoder.JSONDecodeError as e:
        customMessage = "Error found at reading index configuration file, String could not be converted to JSON."
        errorMessage = "Error Response = {0}".format(str(e))
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        raise SystemExit(errorResponse)

    # New Index Creation
    createIndexUrl = esHost + "/" + newIndexName
    print("Creating new index with url = ", createIndexUrl)
    try:
        response = requests.put(url=createIndexUrl, json=mapping).json()
        if 'acknowledged' in response and response['acknowledged'] and response['index'] == newIndexName:
            print("New index successfully created with name ", response['index'])
        else:
            customMessage = "New index {0} not able created\n".format(newIndexName)
            errorMessage = "ElasticSearch Response = {0}".format(str(response))
            errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
            raise SystemExit(errorResponse)
    except requests.exceptions.RequestException as e:
        customMessage = "Error occurs while creating new index {}".format(newIndexName)
        errorMessage = "Error Response = {0}".format(str(e))
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        raise SystemExit(errorResponse)

    # Index data migration
    print("Started data migration from index {0} to index {1}".format(oldIndexName, newIndexName))
    reIndexUrl = esHost + "/_reindex"
    reIndexBody = {"source": {"index": oldIndexName}, "dest": {"index": newIndexName}}
    print("Data migration request url is {0}\nRequest body is {1}".format(reIndexUrl, reIndexBody))
    try:
        response = requests.post(url=reIndexUrl, json=reIndexBody).json()
        if not response['failures']:
            print(
                "Data Migration Successfully completed from index {0} to index {1}".format(oldIndexName, newIndexName))
        else:
            customMessage = "Data Migration fail\n"
            errorMessage = "ElasticSearch Response = ", str(response)
            errorResponse = customMessage + errorMessage
            rollBackOperations(esHost, newIndexName, errorResponse)
    except requests.exceptions.RequestException as e:
        customMessage = "Error occurs while migrating data from index {0} to index{1}\n".format(oldIndexName,
                                                                                                newIndexName)
        errorMessage = "Error Response = {0}".format(str(e))
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        rollBackOperations(esHost, newIndexName, errorResponse)

    # Exchanging aliases
    print("Started exchanging aliases")
    aliasReplaceUrl = esHost + "/_aliases"
    aliasReplaceBody = {
        "actions": [
            {"remove": {"index": oldIndexName, "alias": aliasName}},
            {"add": {"index": newIndexName, "alias": aliasName}}
        ]
    }
    print("Alias exchanging request url is {0}, and request body is {1}".format(aliasReplaceUrl, aliasReplaceBody))
    try:
        response = requests.post(url=aliasReplaceUrl, json=aliasReplaceBody).json()
        if 'acknowledged' in response and response['acknowledged']:
            print("Exchanging aliases successfully completed")
        else:
            customMessage = "Alias exchanging failed\n"
            errorMessage = "ElasticSearch Response = ", str(response)
            errorResponse = customMessage + errorMessage
            rollBackOperations(esHost, newIndexName, errorResponse)
    except requests.exceptions.RequestException as e:
        customMessage = "Error occurs while adding alias to index {0} and removing alias from index " \
                        "{1}\n".format(oldIndexName, newIndexName)
        errorMessage = "Error Response = {0}".format(str(e))
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        rollBackOperations(esHost, newIndexName, errorResponse)

    # Checking isRemoveOldIndex flag
    if isRemoveOldIndex:
        url = esHost + "/" + oldIndexName
        print("Deleting old index started with url -> {}".format(url))
        try:
            response = requests.delete(url=url).json()
            if 'acknowledged' in response and response['acknowledged']:
                print("Deleting old index {0} successfully completed".format(oldIndexName))
                print("All tasks are successfully completed.")
                print("Your application is now pointed to index {}".format(newIndexName))
            else:
                customMessage = "Error occurs while deleting oldIndex, please try to delete manually\n"
                errorMessage = "ElasticSearch Response = ", str(response)
                errorResponse = customMessage + errorMessage
                raise SystemExit(errorResponse)
        except requests.exceptions.RequestException as e:
            customMessage = "Error occurs while deleting oldIndex, please try to delete manually\n"
            errorMessage = "Error Response = {0}".format(str(e))
            errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
            raise SystemExit(errorResponse)
    else:
        print("All tasks are successfully completed.")
        print("Your application is now pointed to index {}".format(newIndexName))


def rollBackOperations(esHost, newIndexName, errorResponse):
    print("Rollback started..")
    rollBackUrl = esHost + "/" + newIndexName
    try:
        response = requests.delete(url=rollBackUrl).json()
        if 'acknowledged' in response and response['acknowledged']:
            print("Rollback successfully completed.")
            raise SystemExit(errorResponse)
        else:
            customMessage = "Rollback Failed, Please try to delete newIndex manually\n"
            errorMessage = "ElasticSearch Response = ", str(response)
            errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
            raise SystemExit(errorResponse)
    except requests.exceptions.RequestException as e:
        customMessage = "Rollback Failed, Please try to delete newIndex manually\n"
        errorMessage = "Error Response = ", str(e)
        errorResponse = ExceptionMessageCreator(customMessage + errorMessage)
        raise SystemExit(errorResponse)


def ExceptionMessageCreator(Message):
    predecessor = "Custom Error Message = "
    successor = "\nAborting Script...."
    return predecessor + str(Message) + successor


if __name__ == "__main__":
    elasticSearchHost = "http://localhost:9200"
    oldIndex = "oldIndex"
    newIndex = "newIndex"
    mappingFilePath = "config.json"
    alias = "alias"
    isRemoveOldIndex = True
    IndexRepairJob(elasticSearchHost, oldIndex, newIndex, mappingFilePath, alias, isRemoveOldIndex)
