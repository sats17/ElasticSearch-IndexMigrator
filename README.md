[![Generic badge](https://img.shields.io/badge/built_by-Sats17-brightgreen.svg)](https://GitHub.com/sats17/)
[![Generic badge](https://img.shields.io/badge/built_with-Python-informational.svg)](https://curl.haxx.se/)
# ElasticSearch-IndexMigrator
Elastic search do not allow to update existing mappings, to overcome this issue, we have automated the steps in this script that elastic search has provided in this 
[post](https://www.elastic.co/guide/en/elasticsearch/guide/current/index-aliases.html) and giving a single click solution. <br />
***
- Note: This script is tested on elastic search version 6.8
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
