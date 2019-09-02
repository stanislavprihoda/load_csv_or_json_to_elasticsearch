#!/usr/bin/env python3
# coding: utf-8

__all__ = ['ElasticDataloader', 'ElasticDataloaderSet',
           'ElasticDataloaderException']
__version__ = '0.1'
__author__ = 'Stanislav Prihoda'

import sys
import os
import csv
import json
import argparse
import time
import elasticsearch
from elasticsearch.helpers import streaming_bulk
import logging
import locale

LOG_LEVEL = logging.DEBUG


class ElasticDataloader(object):
    def __init__(self, **kwargs):
        self._logger = logging.getLogger(__file__)
        self.client = elasticsearch.Elasticsearch(**kwargs)

    def load_dataset(self, es_dataset, chunk_size=500, **kwargs):
        try:
            self._prepare_index(es_dataset)
            bulks_processed = 0
            not_ok = []
            generator = self._csv_generator(
                es_dataset) if es_dataset.ext == "csv" else self._ndjson_generator(es_dataset)
            for cnt, response in enumerate(streaming_bulk(self.client, generator, chunk_size)):
                ok, result = response
                if not ok:
                    not_ok.append(result)
                if cnt % chunk_size == 0:
                    bulks_processed += 1
                    self._logger.debug(
                        f"Bulk number {bulks_processed} processed, already processed docs {cnt}.")
                    if len(not_ok):
                        self._logger.error(
                            f"NOK DOCUMENTS (log limited to 10) in batch {bulks_processed}: {not_ok[-10:]}")
                        not_ok = []
            self._logger.info(
                f"Refreshing index {es_dataset.es_index_name} to make indexed documents searchable.")
            self.client.indices.refresh(index=es_dataset.es_index_name)
        except (elasticsearch.ImproperlyConfigured) as e:
            self._logger.error(
                f"Issue with Elasticsearch client config {e}")
            raise ElasticDataloaderException from e
        except (elasticsearch.ElasticsearchException) as e:
            self._logger.error(
                f"There was an exception during indexing of data {e}")
            raise ElasticDataloaderException from e
        except (csv.Error, json.JSONDecodeError) as e:
            self._logger.error(
                f"Issue with the contents of the file: {es_dataset.input_file} {type(e)} {e}")
            raise ElasticDataloaderException from e
        except (IOError, ValueError) as e:
            self._logger.error(
                f"Issue with file: {es_dataset.input_file} {e}")
            raise ElasticDataloaderException from e
        else:
            return cnt+1

    def _csv_generator(self, es_dataset, **kwargs):
        with open(es_dataset.input_file) as csv_file:
            csv_dict_reader = csv.DictReader(csv_file)
            for cnt, row in enumerate(csv_dict_reader):
                yield self._prepare_document_for_bulk(es_dataset, row, cnt)

    def _ndjson_generator(self, es_dataset, **kwargs):
        with open(es_dataset.input_file) as fp:
            for cnt, line in enumerate(fp):
                row = json.loads(line)
                yield self._prepare_document_for_bulk(es_dataset, row, cnt)

    def _prepare_document_for_bulk(self, es_dataset, row, cnt):
        row["_id"] = row.get(es_dataset.es_id_field, cnt +
                             es_dataset.es_index_start_from)
        row["_index"] = es_dataset.es_index_name
        # row["_type"] = "_doc"
        return row

    def _prepare_index(self, es_dataset):
        if es_dataset.delete_index_first:
            self._logger.debug(
                f"Attemting to delete the index {es_dataset.es_index_name} first as requested.")
            try:
                res = self.client.indices.delete(
                    index=es_dataset.es_index_name)
            except elasticsearch.NotFoundError:
                self._logger.debug("Required index for deletion not found.")
        self._logger.debug(
            f"Making sure index {es_dataset.es_index_name} exists.")
        self.client.indices.create(index=es_dataset.es_index_name, ignore=400)

    # ####### KEPT HERE JUST FOR EVENTUAL TESTING PURPOSES #######
    # def load_csv_doc_by_doc(self, es_dataset, **kwargs) -> dict:
    #     operations = {}
    #     with open(es_dataset.input_file) as csv_file:
    #         csv_dict_reader = csv.DictReader(csv_file)
    #         for cnt, row in enumerate(csv_dict_reader):
    #             _id = row.get(es_dataset.es_id_field, cnt +
    #                           es_dataset.es_index_start_from)
    #             row.pop("_id", 0)
    #             res = self.client.index(index=es_dataset.es_index_name,
    #                                     id=_id, body=row)
    #             result, doc_load_success = res['result'], 1 if (res['_shards']
    #                                                             ['successful'] > 0) else 0
    #             operations[_id] = [result, doc_load_success]
    #     return len(operations)
    # ############################################################


class ElasticDataloaderSet:
    allowed_extensions = ['csv', 'json', 'log']

    def __init__(self, input_file, es_index_name, es_id_field="_id", es_index_start_from=1, delete_index_first=False, **kwargs):
        self.input_file, self.es_index_name, self.es_id_field, self.es_index_start_from, self.delete_index_first = input_file, es_index_name, es_id_field, es_index_start_from, delete_index_first
        # kinda simplistic check on the extension - shall be improved in actual use
        self.ext = self.input_file.split('.')[-1]
        if self.ext not in self.allowed_extensions:
            raise ValueError(
                'Given file format is not supported - only .csv or newline-delimited json (.json or .log).')


class ElasticDataloaderException(Exception):
    pass


def main(**kwargs):
    logger = _get_logger(name=__file__)
    start = time.time()
    logger.info("Script execution STARTED.")
    try:
        defined_params = {k: v for k, v in kwargs.items() if v is not None}
        es_host = defined_params.pop("es_host", "localhost:9200")
        es_loader = ElasticDataloader(hosts=[es_host])
        es_dataset = ElasticDataloaderSet(**defined_params)
        docs_processed = es_loader.load_dataset(es_dataset)
    except (ElasticDataloaderException) as e:
        logger.error(
            f"Script execution FAILED.", exc_info=True)
        return 1
    else:
        end = time.time()
        logger.info(
            f"Script execution FINALIZED in {end - start} seconds. Dataset from file {es_dataset.input_file} loaded to Elasticsearch at {es_host}. Number of documents processed: {docs_processed}.")
        return 0


def _get_logger(name=__name__, handler=logging.StreamHandler(sys.stdout)):
    logger = logging.getLogger(name)
    try:
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    except locale.Error as e:
        logger.debug("Ignoring error %s when setting locale", e)
    try:
        logger.setLevel(LOG_LEVEL)
    except NameError:
        logger.setLevel(logging.INFO)
    if not len(logger.handlers):
        handler = handler
        formater = logging.Formatter(
            "%(asctime)s — %(name)s — %(levelname)s — %(message)s")
        handler.setFormatter(formater)
        logger.addHandler(handler)
    return logger


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Load data from CSV file or newline delimited JSON file to a running Elasticsearch cluster.",
        epilog=f"Example: python3 {__file__} my-data.csv my-elastic-index")
    parser.add_argument(
        'input_file', help='Path to the input csv/json file. Extension check on .csv or .json/.log')
    parser.add_argument(
        'es_index_name', help='Name of the index in which to index the documents.')
    parser.add_argument('--es_host',
                        help="Hostname and port of an Elasticsearch node. Defaults to 'localhost:9200'.")
    parser.add_argument('--es_id_field',
                        help='Field consisting unique id of the document/row. If reserved field (_id) present it gets dropped if not specified by this argument.')
    parser.add_argument('--es_id_start_from',
                        help='Starting id number if no index_field is provided. Defaults to 1.')
    parser.add_argument('--delete_index_first', choices=[True, False], type=bool,
                        help='Whether to clear the index first before loading new data.')
    args = parser.parse_args()
    return vars(args)


if __name__ == "__main__":
    sys.exit(main(**_parse_args()))
