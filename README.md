# ElasticSearch-IndexMigrator
Elastic search not allowed to update existing mappings, to overcome this problem elastic search provided beautiful solution in this [post](https://www.elastic.co/guide/en/elasticsearch/guide/current/index-aliases.html).<br />
In this script we are automating all those steps that elasticsearch provided and giving single click solution.
***

### Prerequisites
##### language
- Python 3+
##### libraries
- requests
- json

### Required variables to run script
###### elasticSearchHost = Your elasticsearch host.
###### oldIndex = Your old index name.
###### newIndex = Your new index name.
###### mappingFilePath = Your correct mapping json that you will use to create new index.
###### alias = Your alias name that you want to relink from old index to new index.
