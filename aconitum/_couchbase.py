import argparse
import json
import datetime
import timeit

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, ClusterOptions

from aconitum.query import AbstractBenchmarkQueryRunnable, AbstractBenchmarkQuerySuite
from aconitum.executor import AbstractBenchmarkRunnable


class CouchbaseBenchmarkQuerySuite(AbstractBenchmarkQuerySuite):
    def __init__(self, cluster, bucket_name, logger, **kwargs):
        super().__init__(logger=logger, **kwargs)
        self.cluster = cluster
        self.keyspace_prefix = f'{bucket_name}._default'
        self.logger = logger

    def execute_n1ql(self, statement, timeout=None):
        query_parameters = {} if timeout is None else {'timeout': datetime.timedelta(seconds=timeout)}
        lean_statement = ' '.join(statement.split())
        try:
            response_iterable = self.cluster.query(lean_statement, **query_parameters)
            response_json = {'statement': lean_statement, 'results': []}
            response_json = {**response_json, **response_iterable.meta}
            for record in response_iterable:
                response_json['results'].append(record)

        except Exception as e:
            self.logger.warning(f'Status of executing statement {statement} not successful, but instead {e}.')
            response_json = {'statement': lean_statement, 'results': [], 'error': str(e), 'status': 'timeout'}

        return response_json

    def query_a_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryARunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryARunnable, self).__init__('A', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_n1ql(f"""
                    FROM        {self.query_suite.keyspace_prefix}.Orders O
                    UNNEST      O.o_orderline OL
                    WHERE       OL.ol_delivery_d BETWEEN "{v0}" AND "{v1}"
                    SELECT      COUNT(*) AS count_order;
                """, timeout=timeout)

        return _QueryARunnable(query_suite=self)

    def query_b_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryBRunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryBRunnable, self).__init__('B', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_n1ql(f"""
                    FROM        {self.query_suite.keyspace_prefix}.Orders O
                    WHERE       SOME OL IN O.o_orderline
                                SATISFIES OL.ol_delivery_d BETWEEN "{v0}" AND "{v1}"
                                END
                    SELECT      COUNT(*) AS count_order;
                """, timeout=timeout)

        return _QueryBRunnable(query_suite=self)

    def query_c_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryCRunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryCRunnable, self).__init__('C', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_n1ql(f"""
                    FROM        {self.query_suite.keyspace_prefix}.Orders O
                    WHERE       SOME AND EVERY OL IN O.o_orderline
                                SATISFIES OL.ol_delivery_d BETWEEN "{v0}" AND "{v1}"
                                END
                    SELECT      COUNT(*) AS count_order;
                """, timeout=timeout)

        return _QueryCRunnable(query_suite=self)

    def query_d_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryDRunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryDRunnable, self).__init__('D', query_suite.generate_items)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_n1ql(f"""
                    FROM       {self.query_suite.keyspace_prefix}.Item I
                    JOIN       {self.query_suite.keyspace_prefix}.Orders O
                    ON         ANY OL IN O.o_orderline 
                               SATISFIES OL.ol_i_id = I.i_id END
                    WHERE      I.i_id BETWEEN {v0} AND {v1}
                    SELECT     COUNT(*) AS count_order_item;
                """, timeout=timeout)

        return _QueryDRunnable(query_suite=self)

    def query_1_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query1Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query1Runnable, self).__init__('1', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_n1ql(f"""
                    FROM        {self.query_suite.keyspace_prefix}.Orders O
                    UNNEST      O.o_orderline OL
                    WHERE       OL.ol_delivery_d BETWEEN "{v0}" AND "{v1}"
                    GROUP BY    OL.ol_number
                    SELECT      OL.ol_number, SUM(OL.ol_quantity) AS sum_qty, SUM(OL.ol_amount) AS sum_amount,
                                AVG(OL.ol_quantity) AS avg_qty, AVG(OL.ol_amount) AS avg_amount, 
                                COUNT(*) AS count_order
                    ORDER BY    OL.ol_number;
                """, timeout=timeout)

        return _Query1Runnable(query_suite=self)

    def query_6_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query6Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query6Runnable, self).__init__('6', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_n1ql(f"""
                    FROM    {self.query_suite.keyspace_prefix}.Orders O
                    UNNEST  O.o_orderline OL
                    WHERE   OL.ol_delivery_d BETWEEN "{v0}" AND "{v1}" AND 
                            OL.ol_quantity BETWEEN 1 AND 100000
                    SELECT  SUM(OL.ol_amount) AS revenue;
                """, timeout=timeout)

        return _Query6Runnable(query_suite=self)

    def query_7_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query7Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query7Runnable, self).__init__('7', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_n1ql(f"""
                    FROM        {self.query_suite.keyspace_prefix}.Supplier SU
                    JOIN        {self.query_suite.keyspace_prefix}.Stock S
                    ON          ((S.s_w_id * S.s_i_id) % 10000) = SU.su_suppkey
                    JOIN        (
                        FROM    {self.query_suite.keyspace_prefix}.Orders O
                        UNNEST  O.o_orderline OL
                        WHERE   OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}'
                        SELECT  OL.ol_supply_w_id, OL.ol_i_id, O.o_entry_d, OL.ol_amount, O.o_c_id, O.o_w_id, O.o_d_id
                    ) AS OLO
                    ON          OLO.ol_supply_w_id = S.s_w_id AND 
                                OLO.ol_i_id = S.s_i_id
                    JOIN        {self.query_suite.keyspace_prefix}.Customer C
                    ON          C.c_id = OLO.o_c_id AND
                                C.c_w_id = OLO.o_w_id AND 
                                C.c_d_id = OLO.o_d_id
                    JOIN        {self.query_suite.keyspace_prefix}.Nation N1
                    ON          SU.su_nationkey = N1.n_nationkey
                    JOIN        {self.query_suite.keyspace_prefix}.Nation N2
                    ON          (stringToCodepoint(SUBSTR(C.c_state, 1, 1)))[0] = N2.n_nationkey
                    WHERE       ( ( N1.n_name = 'Germany' AND N2.n_name = 'Cambodia' ) OR
                                  ( N1.n_name = 'Cambodia' AND N2.n_name = 'Germany' ) )
                    GROUP BY    SU.su_nationkey, (stringToCodepoint(SUBSTR(C.c_state, 1, 1)))[0], 
                                SUBSTR(OLO.o_entry_d, 0, 4)
                    SELECT      SU.su_nationkey AS supp_nation, 
                                (stringToCodepoint(SUBSTR(C.c_state, 1, 1)))[0] AS cust_nation,
                                SUBSTR(OLO.o_entry_d, 0, 4) AS l_year, SUM(OLO.ol_amount) AS revenue
                    ORDER BY    SU.su_nationkey, cust_nation, l_year;
                """, timeout=timeout)

        return _Query7Runnable(query_suite=self)

    def query_12_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query12Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query12Runnable, self).__init__('12', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_n1ql(f"""
                    FROM        {self.query_suite.keyspace_prefix}.Orders O
                    UNNEST      O.o_orderline OL
                    WHERE       O.o_entry_d <= OL.ol_delivery_d AND 
                                OL.ol_delivery_d BETWEEN "{v0}" AND "{v1}"
                    GROUP BY    O.o_ol_cnt
                    SELECT      O.o_ol_cnt, 
                                SUM(CASE WHEN O.o_carrier_id = 1 OR O.o_carrier_id = 2 
                                         THEN 1 ELSE 0 END) AS high_line_count,
                                SUM(CASE WHEN O.o_carrier_id <> 1 OR O.o_carrier_id <> 2 
                                         THEN 1 ELSE 0 END) AS low_line_count
                    ORDER BY    O.o_ol_cnt;
                """, timeout=timeout)

        return _Query12Runnable(query_suite=self)

    def query_14_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query14Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query14Runnable, self).__init__('14', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_n1ql(f"""
                    FROM    {self.query_suite.keyspace_prefix}.Orders O
                    UNNEST  O.o_orderline OL
                    JOIN    {self.query_suite.keyspace_prefix}.Item I
                    ON      I.i_id = OL.ol_i_id
                    WHERE   OL.ol_delivery_d BETWEEN "{v0}" AND "{v1}"
                    SELECT  100.00 * SUM(CASE WHEN I.i_data LIKE 'pr%' THEN OL.ol_amount ELSE 0 END) / 
                                (1 + SUM(OL.ol_amount)) AS promo_revenue;
                """, timeout=timeout)

        return _Query14Runnable(query_suite=self)

    def query_15_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query15Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query15Runnable, self).__init__('15', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_n1ql(f"""
                    WITH        Revenue AS (
                                FROM        {self.query_suite.keyspace_prefix}.Orders O
                                UNNEST      O.o_orderline OL
                                JOIN        {self.query_suite.keyspace_prefix}.Stock S
                                ON          OL.ol_i_id = S.s_i_id AND OL.ol_supply_w_id = S.s_w_id
                                WHERE       OL.ol_delivery_d BETWEEN "{v0}" AND "{v1}"
                                GROUP BY    ((S.s_w_id * S.s_i_id) % 10000)
                                SELECT      ((S.s_w_id * S.s_i_id) % 10000) AS supplier_no, 
                                            SUM(OL.ol_amount) AS total_revenue
                    )
                    FROM        Revenue R
                    JOIN        {self.query_suite.keyspace_prefix}.Supplier SU
                    ON          SU.su_suppkey = R.supplier_no
                    WHERE       R.total_revenue = ( 
                                FROM        Revenue M
                                SELECT      VALUE MAX (M.total_revenue)
                    )[0]
                    SELECT      SU.su_suppkey, SU.su_name, SU.su_address, SU.su_phone, R.total_revenue
                    ORDER BY    SU.su_suppkey;
                """, timeout=timeout)

        return _Query15Runnable(query_suite=self)

    def query_20_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query20Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query20Runnable, self).__init__('20', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                # Note: we cannot switch Stock and Orders here. This would require another index on Stock.
                return self.query_suite.execute_n1ql(f"""
                    FROM          {self.query_suite.keyspace_prefix}.Supplier SU
                    JOIN          {self.query_suite.keyspace_prefix}.Nation N
                    ON            SU.su_nationkey = N.n_nationkey
                    JOIN          (
                        FROM      {self.query_suite.keyspace_prefix}.Stock S
                        JOIN      (
                            FROM    {self.query_suite.keyspace_prefix}.Orders O
                            UNNEST  O.o_orderline OL
                            WHERE   OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}'
                            SELECT  OL.ol_i_id, OL.ol_quantity
                        ) AS OLO  
                        ON        OLO.ol_i_id = S.s_i_id
                        JOIN      (
                            FROM    {self.query_suite.keyspace_prefix}.Item I
                            WHERE   I.i_data LIKE 'co%'
                            SELECT  I.i_id
                        ) AS II
                        ON        II.i_id = S.s_i_id
                        GROUP BY  S.s_i_id, S.s_w_id, S.s_quantity
                        HAVING    (100 * S.s_quantity) > SUM(OLO.ol_quantity)
                        SELECT    ((S.s_w_id * S.s_i_id) % 10000) AS suppkey
                    ) AS SOOL
                    ON            SOOL.suppkey = SU.su_suppkey
                    WHERE         N.n_name = 'Germany'
                    SELECT        SU.su_name, SU.su_address
                    ORDER BY      SU.su_name;
                  """, timeout=timeout)

        return _Query20Runnable(query_suite=self)


class CouchbaseBenchmarkRunnable(AbstractBenchmarkRunnable):
    @staticmethod
    def _collect_config(**kwargs):
        parser = argparse.ArgumentParser(description='Benchmark TPC_CH queries on an Couchbase instance.')
        parser.add_argument('--config', type=str, default='config/couchbase.json', help='Path to the config file.')
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
                                    'CouchbaseTPCCHQuery'

        # Return all relevant information to our caller.
        return {'workingSystem': 'Couchbase', 'runtimeNotes': parser_args.notes, **config_json, **kwargs}

    def __init__(self, **kwargs):
        super().__init__(**self._collect_config(**kwargs))

        # Connect to our Couchbase cluster.
        self.cluster_uri = 'couchbase://' + self.config['cluster']['address']
        self.bucket_name = self.config['cluster']['bucket']
        self.cluster = Cluster.connect(self.cluster_uri, ClusterOptions(PasswordAuthenticator(
            username=self.config['username'],
            password=self.config['password']
        )))
        self.exclude_set = set()

    def perform_benchmark(self):
        for i in range(self.config['experiment']['repeat']):
            for sigma in self.config['experiment']['sigmaValues']:
                self.logger.info('Restarting the Couchbase instance.')
                self.call_subprocess(self.config['restartCommand'])

                for query in CouchbaseBenchmarkQuerySuite(
                    cluster=self.cluster,
                    bucket_name=self.bucket_name,
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

                    # If this query has timed out, add the query + parameter to the exclude set.
                    if results['status'] == 'timeout':
                        self.logger.warning('Query has timed out. No longer running working sigma + query.')
                        self.exclude_set.add((sigma, str(query),))


if __name__ == '__main__':
    CouchbaseBenchmarkRunnable().invoke()
