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
    def __init__(self, database_factory, logger, **kwargs):
        super().__init__(logger=logger, **kwargs)
        self.database_factory = database_factory
        self.logger = logger

    @staticmethod
    def _format_strict(result):
        return json.loads(bson.json_util.dumps(result))

    def execute_select(self, name, count=None, aggregate=None, timeout=None):
        collection = self.database_factory()[name]

        if count is None and aggregate is None:
            raise ValueError("Either predicate or aggregate must be specified.")

        elif count is not None and aggregate is not None:
            raise ValueError("Both predicate and aggregate cannot be specified at the same time.")

        try:
            if count is not None:
                t_before = timeit.default_timer()
                query_results = [{
                    'order_count': collection.count_documents(count, maxTimeMS=timeout)
                }]
                client_time = timeit.default_timer() - t_before
                status = 'success'
                query = count

            else:  # aggregate is not None
                t_before = timeit.default_timer()
                query_results = [
                    self._format_strict(r) for r in
                    collection.aggregate(aggregate, allowDiskUse=True, maxTimeMS=timeout)
                ]
                client_time = timeit.default_timer() - t_before
                status = 'success'
                query = aggregate

            if len(query_results) == 0:
                self.logger.warning(f'Query has no results.')

        except pymongo.errors.ExecutionTimeout:
            self.logger.warning(f'Query has exceeded the specified runtime of {timeout} milliseconds.')
            query_results = None
            client_time = timeout
            status = 'timeout'
            query = count if count is not None else aggregate

        return {'queryResults': query_results, 'clientTime': client_time, 'status': status, 'query': query}

    def query_a_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryARunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryARunnable, self).__init__('A', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline': {
                                '$elemMatch': {'ol_delivery_d': {'$gte': v0, '$lte': v1}}}
                            }
                        },
                        {
                            '$unwind': {'path': '$o_orderline'}
                        },
                        {
                            '$match': {'o_orderline.ol_delivery_d': {'$gte': v0, '$lte': v1}}
                        },
                        {
                            '$count': 'count_orders'
                        }
                    ],
                    'timeout': timeout * 1000
                })

        return _QueryARunnable(query_suite=self)

    def query_b_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryBRunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryBRunnable, self).__init__('B', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': {'o_orderline': {'$elemMatch': {'ol_delivery_d': {'$gte': v0, '$lte': v1}}}},
                    'aggregate': None,
                    'timeout': timeout * 1000
                })

        return _QueryBRunnable(query_suite=self)

    def query_c_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryCRunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryCRunnable, self).__init__('C', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline': {
                                '$all': [
                                    {'$elemMatch': {'ol_delivery_d': {'$gte': v0, '$lte': v1}}}
                                ],
                                '$exists': True
                            }}
                        },
                        {
                            '$count': 'count_orders'
                        }
                    ],
                    'timeout': timeout * 1000
                })

        return _QueryCRunnable(query_suite=self)

    def query_d_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryDRunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryDRunnable, self).__init__('D', query_suite.generate_items)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Item',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'i_id': {'$gte': v0, '$lte': v1}}
                        },
                        {
                            '$lookup': {'from': 'Orders',
                                        'localField': 'i_id',
                                        'foreignField': 'o_orderline.ol_i_id',
                                        'as': 'orders'}
                        },
                        {
                            '$unwind': {'path': '$orders'}
                        },
                        {
                            '$unwind': {'path': '$orders.o_orderline'}
                        },
                        {
                            '$count': 'count_order_item'
                        }
                    ],
                    'timeout': timeout * 1000
                })

        return _QueryDRunnable(query_suite=self)

    def query_1_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query1Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query1Runnable, self).__init__('1', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline': {
                                '$elemMatch': {'ol_delivery_d': {'$gte': v0, '$lte': v1}}}
                            }
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
                super(_Query6Runnable, self).__init__('6', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline': {
                                '$elemMatch': {'ol_delivery_d': {'$gte': v0, '$lte': v1}}}
                            }
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
        class _Query7Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query7Runnable, self).__init__('7', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline': {
                                '$elemMatch': {'ol_delivery_d': {'$gte': v0, '$lte': v1}}}
                            }
                        },
                        {
                            '$unwind': {'path': '$o_orderline'}
                        },
                        {
                            '$lookup': {'from': 'Stock',
                                        'localField': 'o_orderline.ol_i_id',
                                        'foreignField': 's_i_id',
                                        'as': 'stock'}
                        },
                        {
                            '$unwind': {'path': '$stock'}
                        },
                        {
                            '$match': {'$expr': {
                                '$eq': ['$o_orderline.ol_supply_w_id', '$stock.s_w_id']
                            }}
                        },
                        {
                            '$lookup': {'from': 'Customer',
                                        'localField': 'o_c_id',
                                        'foreignField': 'c_id',
                                        'as': 'customer'}
                        },
                        {
                            '$unwind': {'path': '$customer'}
                        },
                        {
                            '$match': {'$expr': {
                                '$and': [{'$eq': ['$customer.c_w_id', '$o_w_id']},
                                         {'$eq': ['$customer.c_d_id', '$o_d_id']}]
                            }}
                        },
                        {
                            '$addFields': {
                                'supplier_no': {'$mod': [{'$multiply': ['$stock.s_w_id', '$stock.s_i_id']}, 10000]}
                            }
                        },
                        {
                            '$lookup': {'from': 'Supplier',
                                        'localField': 'supplier_no',
                                        'foreignField': 'su_suppkey',
                                        'as': 'supplier'}
                        },
                        {
                            '$unwind': {'path': '$supplier'}
                        },
                        {
                            '$lookup': {'from': 'Nation',
                                        'localField': 'supplier.su_nationkey',
                                        'foreignField': 'n_nationkey',
                                        'as': 'nation1'}
                        },
                        {
                            '$unwind': {'path': '$nation1'}
                        },
                        {
                            '$addFields': {
                                'nationkey': {'$function': {
                                    'body': 'function(inputString) { return inputString.codePointAt(0); }',
                                    'args': [{'$substr': ['$customer.c_state', 1, 1]}],
                                    'lang': 'js'
                                }}
                            }
                        },
                        {
                            '$lookup': {'from': 'Nation',
                                        'localField': 'nationkey',
                                        'foreignField': 'n_nationkey',
                                        'as': 'nation2'}
                        },
                        {
                            '$unwind': {'path': '$nation2'}
                        },
                        {
                            '$match': {'$expr': {
                                '$or': [{'$and': [{'$eq': ['$nation1.n_name', 'Germany']},
                                                  {'$eq': ['$nation2.n_name', 'Cambodia']}]},
                                        {'$and': [{'$eq': ['$nation1.n_name', 'Cambodia']},
                                                  {'$eq': ['$nation2.n_name', 'Germany']}]}]
                            }}
                        },
                        {
                            '$group': {
                                '_id': {
                                    'supp_nation': '$supplier.su_nationkey',
                                    'cust_nation': '$nationkey',
                                    'l_year': {'$substr': ['o_entry_d', 0, 4]}
                                },
                                'revenue': {'$sum': '$o_orderline.ol_amount'}
                            }
                        },
                        {
                            '$sort': {'supp_nation': 1, 'cust_nation': 1, 'l_year': 1}
                        }
                    ],
                    'timeout': timeout * 1000
                })

        return _Query7Runnable(query_suite=self)

    def query_12_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query12Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query12Runnable, self).__init__('12', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline': {
                                '$elemMatch': {'ol_delivery_d': {'$gte': v0, '$lte': v1}}}
                            }
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
                super(_Query14Runnable, self).__init__('14', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline': {
                                '$elemMatch': {'ol_delivery_d': {'$gte': v0, '$lte': v1}}}
                            }
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
                            '$group': {'_id': None,
                                       'ol_amount_sum_pr': {'$sum': '$ol_amount_pr'},
                                       'ol_amount_sum': {'$sum': '$o_orderline.ol_amount'}}
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
        class _Query15Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query15Runnable, self).__init__('15', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline': {
                                '$elemMatch': {'ol_delivery_d': {'$gte': v0, '$lte': v1}}}
                            }
                        },
                        {
                            '$unwind': {'path': '$o_orderline'}
                        },
                        {
                            '$lookup': {'from': 'Stock',
                                        'localField': 'o_orderline.ol_i_id',
                                        'foreignField': 's_i_id',
                                        'as': 'stock'}
                        },
                        {
                            '$unwind': {'path': '$stock'}
                        },
                        {
                            '$match': {'$expr': {
                                '$eq': ['$o_orderline.ol_supply_w_id', '$stock.s_w_id']
                            }}
                        },
                        {
                            '$project': {
                                'supplier_no': {'$mod': [{'$multiply': ['$stock.s_w_id', '$stock.s_i_id']}, 10000]},
                                'ol_amount': '$o_orderline.ol_amount'
                            }
                        },
                        {
                            '$group': {'_id': '$supplier_no',
                                       'total_revenue': {'$sum': '$ol_amount'}}
                        },
                        {
                            '$lookup': {'from': 'Supplier',
                                        'localField': '_id',
                                        'foreignField': 'su_suppkey',
                                        'as': 'supplier'}
                        },
                        {
                            '$unwind': {'path': '$supplier'}
                        },
                        {
                            '$group': {'_id': None,
                                       'data': {'$push': '$$ROOT'},
                                       'max_revenue': {'$max': '$total_revenue'}}
                        },
                        {
                            '$unwind': '$data'
                        },
                        {
                            '$match': {'$expr': {
                                '$eq': ['$data.total_revenue', '$max_revenue']
                            }}
                        },
                        {
                            '$project': {
                                'su_suppkey': '$data.supplier.su_suppkey',
                                'su_name': '$data.supplier.su_name',
                                'su_address': '$data.supplier.su_address',
                                'su_phone': '$data.supplier.su_phone',
                                'total_revenue': '$data.total_revenue'
                            }
                        },
                        {
                            '$sort': {'su_suppkey': 1}
                        }
                    ],
                    'timeout': timeout * 1000
                })

        return _Query15Runnable(query_suite=self)

    def query_20_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query20Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query20Runnable, self).__init__('20', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_select(**{
                    'name': 'Orders',
                    'count': None,
                    'aggregate': [
                        {
                            '$match': {'o_orderline': {
                                '$elemMatch': {'ol_delivery_d': {'$gte': v0, '$lte': v1}}}
                            }
                        },
                        {
                            '$unwind': {'path': '$o_orderline'}
                        },
                        {
                            '$lookup': {'from': 'Stock',
                                        'localField': 'o_orderline.ol_i_id',
                                        'foreignField': 's_i_id',
                                        'as': 'stock'}
                        },
                        {
                            '$unwind': {'path': '$stock'}
                        },
                        {
                            '$lookup': {'from': 'Item',
                                        'localField': 'stock.s_i_id',
                                        'foreignField': 'i_id',
                                        'as': 'item'}
                        },
                        {
                            '$unwind': {'path': '$item'}
                        },
                        {
                            '$match': {'item.i_data': {'$regex': '^co'}}
                        },
                        {
                            '$group': {'_id': {'s_i_id': '$stock.s_i_id',
                                               's_w_id': '$stock.s_w_id',
                                               's_quantity': '$stock.s_quantity'},
                                       'total_quantity': {'$sum': '$o_orderline.ol_quantity'}}
                        },
                        {
                            '$match': {'$expr': {
                                '$gt': [{'$multiply': [100, '$_id.s_quantity']}, '$total_quantity']
                            }}
                        },
                        {
                            '$project': {
                                'supplier_no': {'$mod': [{'$multiply': ['$_id.s_w_id', '$_id.s_i_id']}, 10000]},
                            }
                        },
                        {
                            '$lookup': {'from': 'Supplier',
                                        'localField': 'supplier_no',
                                        'foreignField': 'su_suppkey',
                                        'as': 'supplier'}
                        },
                        {
                            '$unwind': {'path': '$supplier'}
                        },
                        {
                            '$lookup': {'from': 'Nation',
                                        'localField': 'su_nationkey',
                                        'foreignField': 'n_nationkey',
                                        'as': 'nation'}
                        },
                        {
                            '$unwind': {'path': '$nation'}
                        },
                        {
                            '$match': {'n_name': 'Germany'}
                        },
                        {
                            '$project': {
                                'su_name': '$supplier.su_name',
                                'su_address': '$supplier.su_address'
                            }
                        },
                        {
                            '$sort': {'su_name': 1}
                        }
                    ],
                    'timeout': timeout * 1000
                })

        return _Query20Runnable(query_suite=self)


class MongoDBBenchmarkRunnable(AbstractBenchmarkRunnable):
    @staticmethod
    def _collect_config(**kwargs):
        parser = argparse.ArgumentParser(description='Benchmark TPC_CH queries on a MongoDB instance.')
        parser.add_argument('--config', type=str, default='config/mongodb.json', help='Path to the config file.')
        parser.add_argument('--tpcch', type=str, default='config/tpc_ch.json', help='Path to the TPC_CH file.')
        parser.add_argument('--aconitum', type=str, default='config/aconitum.json', help='Path to the experiment file.')
        parser.add_argument('--notes', type=str, default='', help='Any notes to append to each log entry.')
        parser_args = parser.parse_args()

        # Load all configuration files into a single dictionary.
        with open(parser_args.config) as config_file:
            config_json = json.load(config_file)
        with open(parser_args.tpcch) as tpcch_file:
            config_json['tpcCH'] = json.load(tpcch_file)
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
        self.database_factory = lambda: pymongo.MongoClient(self.database_uri)[self.config['database']['name']]
        self.exclude_set = set()

    def perform_benchmark(self):
        for i in range(self.config['experiment']['repeat']):
            for sigma in self.config['experiment']['sigmaValues']:
                for query in MongoDBBenchmarkQuerySuite(
                    database_factory=self.database_factory,
                    logger=self.logger,
                    **self.config['tpcCH']
                ):
                    # Check if these current parameters exist in the exclude set.
                    if (sigma, str(query),) in self.exclude_set:
                        continue

                    # Execute the query. Record the client response time.
                    self.logger.info(f'Executing query {query} with sigma {sigma} @ run {i + 1}.')
                    t_before = timeit.default_timer()
                    results = query(sigma=sigma, timeout=self.config['experiment']['timeout'])
                    results['clientTime'] = timeit.default_timer() - t_before
                    results['runNumber'] = i
                    self.log_results(results)

                    # If this query was not successful, add the query + parameter to the exclude set.
                    if results['status'] != 'success':
                        self.logger.warning('Query was not successful. No longer running (>= sigma) + query.')
                        for excluded_sigma in self.config['experiment']['sigmaValues']:
                            if excluded_sigma >= sigma:
                                self.exclude_set.add((excluded_sigma, str(query),))
                        self.logger.info('Restarting the MongoDB instance.')
                        self.call_subprocess(self.config['restartCommand'])


if __name__ == '__main__':
    MongoDBBenchmarkRunnable().invoke()
