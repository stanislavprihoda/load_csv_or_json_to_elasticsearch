# load_csv_or_json_to_elasticsearch

python3 load_csv_or_json_to_elasticsearch.py -h     
usage: load_csv_or_json_to_elasticsearch.py [-h] [--es_host ES_HOST]
                                            [--es_id_field ES_ID_FIELD]
                                            [--es_id_start_from ES_ID_START_FROM]
                                            [--delete_index_first {True,False}]
                                            input_file es_index_name

Load data from CSV file or newline delimited JSON file to a running Elasticsearch cluster.

**positional arguments:**     
  input_file            Path to the input csv/json file. Must end with .csv or
                        .json/.log     
  es_index_name         Name of the index in which to index the documents.     

**optional arguments:**     
  -h, --help            show this help message and exit
  --es_host ES_HOST     Hostname and port of an Elasticsearch node. Defaults
                        to 'localhost:9200'.
  --es_id_field ES_ID_FIELD
                        Field consisting unique id of the document/row. If
                        reserved field (_id) present it gets dropped if not
                        specified by this argument.
  --es_id_start_from ES_ID_START_FROM
                        Starting id number if no index_field is provided.
                        Defaults to 1.
  --delete_index_first {True,False}
                        Whether to delete existing index first before loading.

**Example: python3 load_csv_or_json_to_elasticsearch.py my-data.csv my-elastic-index**