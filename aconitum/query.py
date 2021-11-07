import datetime
import abc
import random
import math
import faker
import natsort

from dateutil import relativedelta


class AbstractBenchmarkQueryRunnable(abc.ABC):
    def __init__(self, query_name, generator):
        self.query_name = query_name
        self.generator = generator

    def __str__(self):
        return self.query_name

    @abc.abstractmethod
    def invoke(self, v0, v1, timeout) -> dict:
        pass


class AbstractBenchmarkQuerySuite(abc.ABC):
    def generate_dates(self, sigma):
        """ Generate a random range between the start and end order dates. """
        benchmark_run_date = datetime.datetime.strptime(self.config['runDate'], '%Y-%m-%d %H:%M:%S')
        benchmark_start_date = benchmark_run_date - relativedelta.relativedelta(years=7)
        benchmark_end_date = benchmark_run_date - relativedelta.relativedelta(days=1) - datetime.timedelta(days=151)

        # Determine the desired delta using the given sigma.
        desired_delta = (sigma / 100.0) * (benchmark_end_date - benchmark_start_date)

        # Generate the range. Ensure that the generated end date does not go past the benchmark end date.
        generated_start_date, generated_end_date = benchmark_start_date, benchmark_end_date
        while generated_end_date >= benchmark_end_date:
            generated_start_date = self.faker.date_time_between(
                start_date=benchmark_start_date, end_date=benchmark_end_date)
            generated_end_date = generated_start_date + desired_delta

        self.logger.debug(f'Generated dates: [{generated_start_date}, {generated_end_date}]')
        return generated_start_date.strftime('%Y-%m-%d %H:%M:%S'), generated_end_date.strftime('%Y-%m-%d %H:%M:%S')

    def generate_items(self, sigma):
        """ Generate a random range between the start and end item IDs. """
        benchmark_start_id = 1
        benchmark_end_id = 100000

        # Determine the desired delta using the given sigma.
        desired_delta = (sigma / 100.0) * (benchmark_end_id - benchmark_start_id)

        # Generate the range. Ensure that the generated end ID does not go past the benchmark end ID.
        generated_start_id = random.randint(benchmark_start_id, math.ceil(benchmark_end_id - desired_delta))
        generated_end_id = math.ceil(generated_start_id + desired_delta)

        self.logger.debug(f'Generated item IDs: [{generated_start_id}, {generated_end_id}]')
        return generated_start_id, generated_end_id

    def __init__(self, **kwargs):
        self.config = kwargs
        self.faker = faker.Faker()
        self.factory_pointer = 0
        self.logger = kwargs['logger']

        exclude_queries_set = set([q.capitalize() for q in kwargs['excludeQueries']])
        all_queries_set = set([(m.removeprefix('query_').removesuffix('_factory').capitalize(), m)
                               for m in dir(AbstractBenchmarkQuerySuite) if m.startswith('query')])
        for query, factory_name in natsort.natsorted(all_queries_set, key=lambda a: a[0]):
            if query not in exclude_queries_set:
                if hasattr(self, 'factory_list'):
                    self.factory_list.append(getattr(AbstractBenchmarkQuerySuite, factory_name))
                else:
                    self.factory_list = [getattr(AbstractBenchmarkQuerySuite, factory_name)]

    def __iter__(self):
        return self

    def __next__(self):
        try:
            # Determine our working factory. If this fails, then we have exhausted all queries in our suite.
            working_factory = self.factory_list[self.factory_pointer]
            self.factory_pointer += 1

            # Generate the runnable that accepts a selectivity value for use with our queries.
            class QueryRunnableAcceptingSigma:
                def __init__(self, query_suite):
                    self.query_runnable = working_factory()
                    self.query_suite = query_suite

                def __str__(self):
                    return self.query_runnable.__str__()

                def __call__(self, *args, **kwargs):
                    v0, v1 = self.query_runnable.generator(kwargs['sigma'])
                    results = self.query_runnable.invoke(v0=v0, v1=v1, timeout=kwargs['timeout'])
                    results['generator'] = str(self.query_runnable.generator)
                    results['valueRange'] = {'v0': v0, 'v1': v1}
                    results['sigma'] = kwargs['sigma']
                    results['query'] = str(self)
                    return results

            return QueryRunnableAcceptingSigma(query_suite=self)

        except IndexError:
            raise StopIteration

    @abc.abstractmethod
    def query_a_factory(self) -> AbstractBenchmarkQueryRunnable:
        """
        This query counts all orderlines with a delivery date between a specific time period.
        """
        pass

    @abc.abstractmethod
    def query_b_factory(self) -> AbstractBenchmarkQueryRunnable:
        """
        This query counts all orders that have an orderline with a delivery date between a specific time period.
        """
        pass

    @abc.abstractmethod
    def query_c_factory(self) -> AbstractBenchmarkQueryRunnable:
        """
        This query counts all orders that have every orderline with a delivery date between a specific time period.
        """
        pass

    @abc.abstractmethod
    def query_d_factory(self) -> AbstractBenchmarkQueryRunnable:
        """
        This query counts all orderlines of an item between a specific ID range.
        """
        pass

    @abc.abstractmethod
    def query_1_factory(self) -> AbstractBenchmarkQueryRunnable:
        """
        This query reports the total amount and quantity of all shipped orderlines given by a specific time period.
        Additionally it informs about the average amount and quantity plus the total count of all these orderlines
        ordered by the individual orderline number.
        """
        pass

    @abc.abstractmethod
    def query_6_factory(self) -> AbstractBenchmarkQueryRunnable:
        """
        This query lists the total amount of archived revenue from orderlines which were delivered in a specific
        period and with a certain quantity.
        """
        pass

    @abc.abstractmethod
    def query_7_factory(self) -> AbstractBenchmarkQueryRunnable:
        """
        This query shows the bi-directional trade volume between two given nations sorted by their names and the
        considered years.
        """
        pass

    @abc.abstractmethod
    def query_12_factory(self) -> AbstractBenchmarkQueryRunnable:
        """
        This query counts the amount of orders grouped by the number of orderlines in each order attending the number
        of orders which are shipped with a higher or lower order priority.
        """
        pass

    @abc.abstractmethod
    def query_14_factory(self) -> AbstractBenchmarkQueryRunnable:
        """
        This query reports the percentage of the revenue in a period of time which has been realized from promotional
        campaigns.
        """
        pass

    @abc.abstractmethod
    def query_15_factory(self) -> AbstractBenchmarkQueryRunnable:
        """
        This query finds the top supplier or suppliers who contributed the most to the overall revenue for items
        shipped during a given period of time.
        """
        pass

    @abc.abstractmethod
    def query_20_factory(self) -> AbstractBenchmarkQueryRunnable:
        """
        This query lists suppliers in a particular nation having selected parts that may be candidates for a
        promotional offer if the quantity of these items is more than 50 percent of the total quantity which has been
        ordered since a certain date.
        """
        pass


class NoOpBenchmarkQueryRunnable(AbstractBenchmarkQueryRunnable):
    """ To be used when a child class cannot implement the TPC-CH query. """
    def __init__(self, query_name):
        super(NoOpBenchmarkQueryRunnable, self).__init__(query_name, lambda a: (0, 0,))

    def invoke(self, v0, v1, timeout) -> dict:
        return {'status': 'success', 'detail': 'Not implemented.'}
