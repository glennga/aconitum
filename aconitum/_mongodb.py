import argparse
import json
import datetime
import timeit
import pymongo
import urllib.parse
import bson.json_util
import pymongo.errors

from aconitum.query import AbstractBenchmarkQueryRunnable, AbstractBenchmarkQuerySuite
from aconitum.executor import AbstractBenchmarkRunnable


class MongoDBBenchmarkQuerySuite(AbstractBenchmarkQuerySuite):
    def __init__(self, database, logger, **kwargs):
        super().__init__(logger=logger, **kwargs)
        self.database = database
        self.logger = logger

    @staticmethod
    def _format_strict(result):
        return json.loads(bson.json_util.dumps(result))

    def execute_select(self, name, count=None, aggregate=None, timeout=None):
        collection = self.database[name]

        if count is None and aggregate is None:
            raise ValueError("Either predicate or aggregate must be specified.")

        elif count is not None and aggregate is not None:
            raise ValueError("Both predicate and aggregate cannot be specified at the same time.")

        try:
            if count is not None:
                t_before = timeit.default_timer()
                query_results = [{'order_count': collection.find(count, max_time_ms=timeout).count()}]
                client_time = timeit.default_timer() - t_before
                status = 'success'

            else:  # aggregate is not None
                t_before = timeit.default_timer()
                query_results = [self._format_strict(r) for r in
                                 collection.aggregate(aggregate, allowDiskUse=True, maxTimeMS=timeout)]
                client_time = timeit.default_timer() - t_before
                status = 'success'

        except pymongo.errors.ExecutionTimeout:
            query_results = None
            client_time = timeout
            status = 'timeout'

        return {'queryResults': query_results, 'clientTime': client_time, 'status': status}

    def query_0_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query0Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                self.query_suite = query_suite
            
            def invoke(self, date_1, date_2, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': {'o_orderline.ol_delivery_d': {'$gte': date_1, '$lte': date_2}},
                    'aggregate': None,
                    'timeout': timeout * 1000
                })
                
        return _Query0Runnable(query_suite=self)
    
    def query_1_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query1Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                self.query_suite = query_suite

            def invoke(self, date_1, date_2, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline.ol_delivery_d': {'$gte': date_1, '$lte': date_2}}
                        },
                        {
                            '$unwind': {'path': '$o_orderline'}
                        },
                        {
                            '$group': {'_id': '$o_orderline.ol_number',
                                       'sum_qty': {'$sum': '$o_orderline.ol_quantity'},
                                       'sum_amount': {'$sum': '$o_orderline.ol_amount'},
                                       'avg_qty': {'$avg': '$o_orderline.ol_quantity'},
                                       'avg_amount': {'$avg': '$o_orderline.ol_amount'},
                                       'count_order': {'$sum': 1}}
                        },
                        {
                            '$sort': {'o_orderline.ol_number': 1}
                        }
                    ],
                    'timeout': timeout * 1000
                })

        return _Query1Runnable(query_suite=self)

    def query_6_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query6Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                self.query_suite = query_suite

            def invoke(self, date_1, date_2, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline.ol_delivery_d': {'$gte': date_1, '$lte': date_2}}
                        },
                        {
                            '$match': {'o_orderline.ol_quantity': {'$gte': 1, '$lte': 100000}}
                        },
                        {
                            '$group': {'_id': None,
                                       'revenue': {'$sum': '$o_orderline.ol_amount'}, }
                        }
                    ],
                    'timeout': timeout * 1000
                })

        return _Query6Runnable(query_suite=self)

    def query_7_factory(self) -> AbstractBenchmarkQueryRunnable:
        return self.NoOpBenchmarkQueryRunnable()

    def query_12_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query12Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                self.query_suite = query_suite

            def invoke(self, date_1, date_2, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline.ol_delivery_d': {'$gte': date_1, '$lte': date_2}}
                        },
                        {
                            '$match': {'$expr': {'$lte': ['$o_entry_d', '$o_orderline.ol_delivery_d']}}
                        },
                        {
                            '$project': {
                                'o_ol_cnt': '$o_ol_cnt',
                                'high_line': {
                                    '$switch': {'branches': [{'case': {'$in': ['$o_carrier_id', [1, 2]]}, 'then': 1}],
                                                'default': 0}
                                },
                                'low_line': {
                                    '$switch': {'branches': [{'case': {'$in': ['$o_carrier_id', [1, 2]]}, 'then': 0}],
                                                'default': 1}
                                }
                            }
                        },
                        {
                            '$group': {'_id': '$o_ol_cnt',
                                       'high_line_count': {'$sum': 'high_line'},
                                       'low_line_count': {'$sum': 'low_line'}}
                        },
                        {
                            '$sort': {'o_ol_cnt': 1}
                        }
                    ],
                    'timeout': timeout * 1000
                })

        return _Query12Runnable(query_suite=self)

    def query_14_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query14Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                self.query_suite = query_suite

            def invoke(self, date_1, date_2, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline.ol_delivery_d': {'$gte': date_1, '$lte': date_2}}
                        },
                        {
                            '$unwind': {'path': '$o_orderline'}
                        },
                        {
                            '$lookup': {'from': 'Item',
                                        'localField': 'o_orderline.ol_i_id',
                                        'foreignField': 'i_id',
                                        'as': 'item'}
                        },
                        {
                            '$project': {
                                'ol_amount_pr': {
                                    '$switch': {
                                        'branches': [{
                                            'case': {'$regexMatch': {'input': {'$first': '$item.i_data'},
                                                                     'regex': '/^pr/'}},
                                            'then': '$o_orderline.ol_amount'
                                        }],
                                        'default': 0
                                    }
                                }
                            }
                        },
                        {
                            '$group': {
                                '_id': None,
                                'ol_amount_sum_pr': {'$sum': '$ol_amount_pr'},
                                'ol_amount_sum': {'$sum': '$o_orderline.ol_amount'}
                            }
                        },
                        {
                            '$project': {
                                'promo_revenue': {
                                    '$divide': [
                                        {'$multiply': [100.0, '$ol_amount_pr']},
                                        {'$add': [1, "$ol_amount_sum"]}
                                    ]
                                }
                            }
                        }
                    ],
                    'timeout': timeout * 1000
                })

        return _Query14Runnable(query_suite=self)

    def query_15_factory(self) -> AbstractBenchmarkQueryRunnable:
        return self.NoOpBenchmarkQueryRunnable()

    def query_20_factory(self) -> AbstractBenchmarkQueryRunnable:
        return self.NoOpBenchmarkQueryRunnable()


class MongoDBBenchmarkRunnable(AbstractBenchmarkRunnable):
    @staticmethod
    def _collect_config(**kwargs):
        parser = argparse.ArgumentParser(description='Benchmark TPC_CH queries on a MongoDB instance.')
        parser.add_argument('--config', type=str, default='config/mongodb.json', help='Path to the config file.')
        parser.add_argument('--datagen', type=str, default='config/tpc_ch.json', help='Path to the datagen file.')
        parser.add_argument('--aconitum', type=str, default='config/aconitum.json', help='Path to the experiment file.')
        parser.add_argument('--notes', type=str, default='', help='Any notes to append to each log entry.')
        parser_args = parser.parse_args()

        # Load all configuration files into a single dictionary.
        with open(parser_args.config) as config_file:
            config_json = json.load(config_file)
        with open(parser_args.datagen) as datagen_file:
            config_json['tpcCH'] = json.load(datagen_file)
        with open(parser_args.aconitum) as experiment_file:
            config_json['experiment'] = json.load(experiment_file)

        # Specify the results directory.
        config_json['resultsDir'] = 'out/' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '-' + \
                                    'MongoDBTPCCHQuery'

        # Return all relevant information to our caller.
        return {'workingSystem': 'MongoDB', 'runtimeNotes': parser_args.notes, **config_json, **kwargs}

    def __init__(self, **kwargs):
        super().__init__(**self._collect_config(**kwargs))

        # Connect to our database.
        self.database_uri = f'mongodb://{urllib.parse.quote_plus(self.config["username"])}:' \
                            f'{urllib.parse.quote_plus(self.config["password"])}@' \
                            f'{self.config["database"]["address"]}' + \
                            f':{self.config["database"]["port"]}'
        self.client = pymongo.MongoClient(self.database_uri)
        self.database = self.client[self.config['database']['name']]

    def perform_benchmark(self):
        for i in range(self.config['experiment']['repeat']):
            for sigma in self.config['experiment']['sigmaValues']:
                for query in MongoDBBenchmarkQuerySuite(
                        database=self.database,
                        logger=self.logger,
                        **self.config['tpcCH']
                ):
                    # Execute the query. Record the client response time.
                    t_before = timeit.default_timer()
                    results = query(sigma=sigma, timeout=self.config['experiment']['timeout'])
                    results['clientTime'] = timeit.default_timer() - t_before
                    results['runNumber'] = i
                    self.log_results(results)


if __name__ == '__main__':
    MongoDBBenchmarkRunnable().invoke()
