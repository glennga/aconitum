import argparse
import json
import datetime
import timeit
import requests
import time

from aconitum.query import AbstractBenchmarkQueryRunnable, AbstractBenchmarkQuerySuite
from aconitum.executor import AbstractBenchmarkRunnable


class AsterixDBBenchmarkQuerySuite(AbstractBenchmarkQuerySuite):
    def __init__(self, nc_uri, logger, **kwargs):
        super().__init__(logger=logger, **kwargs)
        self.query_prefix = kwargs['query_prefix']
        self.join_hint = kwargs['join_hint']
        self.nc_uri = nc_uri
        self.logger = logger

    def execute_sqlpp(self, statement, timeout=None):
        lean_statement = ' '.join(statement.split())
        query_parameters = {'statement': lean_statement}

        # Retry the query until success.
        while True:
            try:
                self.logger.debug(f'Issuing query "{lean_statement}" to cluster.')
                t_before = timeit.default_timer()
                response_json = requests.post(self.nc_uri, query_parameters, timeout=timeout).json()
                response_json['clientTime'] = timeit.default_timer() - t_before
                break
            except requests.exceptions.RequestException as e:
                if timeout is not None and isinstance(e, requests.exceptions.ReadTimeout):
                    self.logger.warning(f'Statement {statement} has run longer than the specified timeout {timeout}.')
                    response_json = {'status': f'Timeout. Exception: {str(e)}'}
                    break
                else:
                    self.logger.warning(f'Exception caught: {str(e)}. Restarting the query in 5 seconds...')
                    time.sleep(5)

        if response_json['status'] != 'success':
            self.logger.warning(f'Status of executing statement {statement} not successful, '
                                f'but instead {response_json["status"]}.')
            self.logger.warning(f'JSON dump: {response_json}')

        # Add the query to response.
        response_json['statement'] = lean_statement
        return response_json

    def query_a_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryARunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryARunnable, self).__init__('A', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_sqlpp(f"""
                    {self.query_suite.query_prefix}
                    FROM       Orders O, O.o_orderline OL
                    WHERE      OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}'
                    SELECT     COUNT(*);
                """, timeout=timeout)

        return _QueryARunnable(query_suite=self)

    def query_b_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryBRunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryBRunnable, self).__init__('B', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_sqlpp(f"""
                    {self.query_suite.query_prefix}
                    FROM       Orders O
                    WHERE      SOME OL IN O.o_orderline 
                               SATISFIES  OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}'
                    SELECT     COUNT(*) AS count_order;
                """, timeout=timeout)

        return _QueryBRunnable(query_suite=self)

    def query_c_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryCRunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryCRunnable, self).__init__('C', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_sqlpp(f"""
                    {self.query_suite.query_prefix}
                    FROM       Orders O
                    WHERE      SOME AND EVERY OL IN O.o_orderline 
                               SATISFIES OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}'
                    SELECT     COUNT(*) AS count_order;
                """, timeout=timeout)

        return _QueryCRunnable(query_suite=self)

    def query_d_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _QueryDRunnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_QueryDRunnable, self).__init__('D', query_suite.generate_items)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_sqlpp(f"""
                    {self.query_suite.query_prefix}
                    FROM       Item I, Orders O, O.o_orderline OL
                    WHERE      I.i_id BETWEEN {v0} AND {v1} AND 
                               TO_BIGINT(I.i_id) {self.query_suite.join_hint} = OL.ol_i_id
                    SELECT     COUNT(*) AS count_order_item;
                """, timeout=timeout)

        return _QueryDRunnable(query_suite=self)

    def query_1_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query1Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query1Runnable, self).__init__('1', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_sqlpp(f"""
                    {self.query_suite.query_prefix}
                    FROM        Orders O, O.o_orderline OL
                    WHERE       OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}'
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
                return self.query_suite.execute_sqlpp(f"""
                    {self.query_suite.query_prefix}
                    FROM    Orders O, O.o_orderline OL
                    WHERE   OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}' AND 
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
                return self.query_suite.execute_sqlpp(f"""
                    {self.query_suite.query_prefix}        
                    FROM        Orders O, O.o_orderline OL, Stock S, Customer C, Supplier SU, Nation N1, Nation N2
                    LET         s_suppkey = ((S.s_w_id * S.s_i_id) % 10000), 
                                c_nationkey = STRING_TO_CODEPOINT(SUBSTR(C.c_state, 1, 1))[0]
                    WHERE       S.s_w_id {self.query_suite.join_hint} = TO_BIGINT(OL.ol_supply_w_id) AND
                                S.s_i_id {self.query_suite.join_hint} = TO_BIGINT(OL.ol_i_id) AND
                                C.c_id {self.query_suite.join_hint} = TO_BIGINT(O.o_c_id) AND
                                C.c_w_id {self.query_suite.join_hint} = TO_BIGINT(O.o_w_id) AND
                                C.c_d_id {self.query_suite.join_hint} = TO_BIGINT(O.o_d_id) AND
                                SU.su_suppkey {self.query_suite.join_hint} = TO_BIGINT(s_suppkey) AND
                                N1.n_nationkey {self.query_suite.join_hint} = TO_BIGINT(SU.su_nationkey) AND
                                N2.n_nationkey {self.query_suite.join_hint} = TO_BIGINT(c_nationkey) AND
                                ( ( N1.n_name = 'Germany' AND N2.n_name = 'Cambodia' ) OR
                                  ( N1.n_name = 'Cambodia' AND N2.n_name = 'Germany' ) ) AND
                                OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}'
                    GROUP BY    SU.su_nationkey, c_nationkey, SUBSTR(O.o_entry_d, 0, 4)
                    SELECT      SU.su_nationkey AS supp_nation, 
                                c_nationkey AS cust_nation,
                                SUBSTR(O.o_entry_d, 0, 4) AS l_year, 
                                SUM(OL.ol_amount) AS revenue
                    ORDER BY    SU.su_nationkey, cust_nation, l_year;
                """, timeout=timeout)

        return _Query7Runnable(query_suite=self)

    def query_12_factory(self) -> AbstractBenchmarkQueryRunnable:
        class _Query12Runnable(AbstractBenchmarkQueryRunnable):
            def __init__(self, query_suite):
                super(_Query12Runnable, self).__init__('12', query_suite.generate_dates)
                self.query_suite = query_suite

            def invoke(self, v0, v1, timeout) -> dict:
                return self.query_suite.execute_sqlpp(f"""
                    {self.query_suite.query_prefix}
                    FROM        Orders O, O.o_orderline OL
                    WHERE       O.o_entry_d <= OL.ol_delivery_d AND 
                                OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}'
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
                return self.query_suite.execute_sqlpp(f"""
                    {self.query_suite.query_prefix}
                    FROM    Orders O, O.o_orderline OL, Item I
                    WHERE   OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}' AND 
                            I.i_id {self.query_suite.join_hint} = TO_BIGINT(OL.ol_i_id) 
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
                return self.query_suite.execute_sqlpp(f"""
                    {self.query_suite.query_prefix}
                    WITH        Revenue AS (
                                FROM        Orders O, O.o_orderline OL, Stock S
                                LET         supplier_no = ((S.s_w_id * S.s_i_id) % 10000)
                                WHERE       S.s_i_id {self.query_suite.join_hint} = TO_BIGINT(OL.ol_i_id) AND 
                                            S.s_w_id {self.query_suite.join_hint} = TO_BIGINT(OL.ol_supply_w_id) AND
                                            OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}'
                                GROUP BY    supplier_no
                                SELECT      supplier_no,
                                            SUM(OL.ol_amount) AS total_revenue
                    )
                    FROM        Revenue R, Supplier SU
                    WHERE       SU.su_suppkey {self.query_suite.join_hint} = TO_BIGINT(R.supplier_no) AND 
                                R.total_revenue = ( 
                                    FROM    Revenue    
                                    SELECT  VALUE MAX(total_revenue) 
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
                return self.query_suite.execute_sqlpp(f"""
                    {self.query_suite.query_prefix}
                    WITH        SupplierKeys AS (
                                FROM        Orders O, O.o_orderline OL, Stock S, Item I
                                WHERE       OL.ol_i_id = S.s_i_id AND
                                            I.i_id {self.query_suite.join_hint} = TO_BIGINT(S.s_i_id) AND
                                            I.i_data LIKE 'co%' AND 
                                            OL.ol_delivery_d BETWEEN '{v0}' AND '{v1}'
                                GROUP BY    S.s_i_id, S.s_w_id, S.s_quantity
                                HAVING      (100 * S.s_quantity) > SUM(OL.ol_quantity)
                                SELECT      VALUE ((S.s_w_id * S.s_i_id) % 10000)
                    )
                    FROM        SupplierKeys SK, Supplier SU, Nation N
                    WHERE       SU.su_suppkey {self.query_suite.join_hint} = TO_BIGINT(SK) AND
                                N.n_nationkey {self.query_suite.join_hint} = TO_BIGINT(SU.su_nationkey) AND 
                                N.n_name = 'Germany'
                    SELECT      SU.su_name, SU.su_address
                    ORDER BY    SU.su_name;
                """, timeout=timeout)

        return _Query20Runnable(query_suite=self)


class AsterixDBBenchmarkRunnable(AbstractBenchmarkRunnable):
    @staticmethod
    def _collect_config(**kwargs):
        parser = argparse.ArgumentParser(description='Benchmark TPC_CH queries on an AsterixDB instance.')
        parser.add_argument('--config', type=str, default='config/asterixdb.json', help='Path to the config file.')
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
                                    'AsterixDBTPCCHQuery'

        # Return all relevant information to our caller.
        return {'workingSystem': 'AsterixDB', 'runtimeNotes': parser_args.notes, **config_json, **kwargs}

    def __init__(self, **kwargs):
        super().__init__(**self._collect_config(**kwargs))

        # Determine our API entry-point.
        self.nc_uri = self.config['clusterController']['address'] + ':' + \
                      str(self.config['clusterController']['port'])
        self.nc_uri = 'http://' + self.nc_uri + '/query/service'
        self.exclude_set = set()

    def perform_benchmark(self):
        for i in range(self.config['experiment']['repeat']):
            for sigma in self.config['experiment']['sigmaValues']:
                for query in AsterixDBBenchmarkQuerySuite(
                    query_prefix=self.config['queryPrefix'],
                    join_hint=self.config['joinHint'],
                    nc_uri=self.nc_uri,
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
                        self.logger.info('Restarting the AsterixDB instance.')
                        self.call_subprocess(self.config['restartCommand'])


if __name__ == '__main__':
    AsterixDBBenchmarkRunnable().invoke()
