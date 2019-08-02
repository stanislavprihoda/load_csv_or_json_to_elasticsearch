#!/usr/bin/env python3
# coding: utf-8

__all__ = ['ElasticDataLoader']
__version__ = '0.1'
__author__ = 'Stanislav Prihoda'

import sys
import os
import csv
import json
import argparse
import time
import elasticsearch


class ElasticDataLoader:
    def __init__(self, **kwargs):
        try:
            # any Elasticsearch py-client arguments can be passed
            self.es = elasticsearch.Elasticsearch(**kwargs)
        except (elasticsearch.ImproperlyConfigured, elasticsearch.ElasticsearchException) as e:
            raise RuntimeError(e) from e

    def load(self, input_file, es_index_name, **kwargs) -> dict:
        """Higher level API to index a given file. If the type is not supported ValueError is raised.

        Returns:
            dict -- item example {'id of the doc':['result of indexing', 1 if indexed 0 if not]}
        """
        file_ext = input_file.split('.')[-1]
        load_type = {'csv': self.load_csv,
                     'json': self.load_json,
                     'log': self.load_json}
        # simple check on the extension - shall be improved in actual use
        if file_ext not in load_type.keys():
            raise ValueError(
                'Given file format is not supported - only .csv or newline-delimited json (.json or .log).')
        if kwargs.pop('delete_index_first', False):
            self.delete_index(es_index_name)
        return load_type[file_ext](input_file, es_index_name, **kwargs)

    def load_csv(self, input_file, es_index_name, es_id_field="_id", es_index_start_from=1, **kwargs) -> dict:
        operations = {}
        with open(input_file) as csv_file:
            csv_dict_reader = csv.DictReader(csv_file)
            for cnt, row in enumerate(csv_dict_reader, start=es_index_start_from):
                _id = row.get(es_id_field, cnt)
                row.pop("_id", 0)
                operations.update(self.index_single_document(
                    row, es_index_name, _id))
        return operations

    def load_json(self, input_file, es_index_name, es_id_field="_id", es_index_start_from=1, **kwargs) -> dict:
        operations = {}
        with open(input_file) as fp:
            for cnt, line in enumerate(fp, start=es_index_start_from):
                row = json.loads(line)
                _id = row.get(es_id_field, cnt)
                row.pop("_id", 0)
                operations.update(self.index_single_document(
                    row, es_index_name, _id))
        return operations

    def index_single_document(self, doc, es_index_name, _id) -> dict:
        res = self.es.index(index=es_index_name,
                            id=_id, body=doc)
        result, doc_load_success = res['result'], 1 if (res['_shards']
                                                        ['successful'] > 0) else 0
        return {_id: [result, doc_load_success]}

    def delete_index(self, es_index_name):
        try:
            res = self.es.indices.delete(index=es_index_name)
        except elasticsearch.NotFoundError:
            print("Index not found.")
        else:
            return res


def main(**kwargs):
    start = time.time()
    try:
        defined_params = {k: v for k, v in kwargs.items() if v is not None}
        es_host = defined_params.pop("es_host", "localhost:9200")
        input_file = defined_params.get("input_file")
        es_loader = ElasticDataLoader(hosts=[es_host])
        results = es_loader.load(**defined_params)
    except (RuntimeError) as e:
        print(
            f"Issue during setup \n{e}")
        return 1
    except (elasticsearch.ElasticsearchException) as e:
        print(
            f"There was an exception during indexing of data \n{e}")
        return 1
    except (csv.Error, json.JSONDecodeError) as e:
        print(
            f"Issue with the contents of the file: {input_file} \n{type(e)} {e}")
        return 1
    except (IOError, ValueError) as e:
        print(
            f"Issue with file: {input_file} \n{e}")
        return 1
    else:
        end = time.time()
        print("========================================")
        print(
            f"Dataset from file {input_file} loaded to Elasticsearch at {es_host}")
        print(f"Execution took {end - start} seconds.")
        print(
            f"Number of docs found: {len(results)}, loaded: {sum(n[1] for n in results.values())}.")
        print(
            f"Performed document operations: {set([value[0] for value in results.values()])}.")
        print("========================================")
        return 0


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load data from CSV file or newline delimited JSON file to a running Elasticsearch cluster.",
        epilog=f"Example: python3 {__file__} my-data.csv my-elastic-index")
    parser.add_argument(
        'input_file', help='Path to the input csv/json file. Must end with .csv or .json/.log')
    parser.add_argument(
        'es_index_name', help='Name of the index in which to index the documents.')
    parser.add_argument('--es_host',
                        help="Hostname and port of an Elasticsearch node. Defaults to 'localhost:9200'.")
    parser.add_argument('--es_id_field',
                        help='Field consisting unique id of the document/row. If reserved field (_id) present it gets dropped if not specified by this argument.')
    parser.add_argument('--es_id_start_from',
                        help='Starting id number if no index_field is provided. Defaults to 1.')
    parser.add_argument('--delete_index_first', choices=[True, False], type=bool,
                        help='Whether to delete existing index first before loading.')
    args = parser.parse_args()
    return vars(args)


if __name__ == "__main__":
    sys.exit(main(**parse_args()))
