import datetime
import random
import abc
import faker

from dateutil import relativedelta


class AbstractBenchmarkQueryRunnable(abc.ABC):
    @abc.abstractmethod
    def invoke(self, date_1, date_2, timeout) -> dict:
        pass


class AbstractBenchmarkQuerySuite(abc.ABC):
    class NoOpBenchmarkQueryRunnable(AbstractBenchmarkQueryRunnable):
        """ To be used when a child class cannot implement the TPC-CH query. """
        def invoke(self, date_1, date_2, timeout) -> dict:
            return {'status': 'success', 'detail': 'Not implemented.'}

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

    def __init__(self, **kwargs):
        self.config = kwargs
        self.faker = faker.Faker()
        self.factory_pointer = 0
        self.factory_list = [self.query_1_factory, self.query_6_factory, self.query_7_factory, self.query_12_factory,
                             self.query_14_factory, self.query_15_factory, self.query_20_factory]
        self.logger = kwargs['logger']

    def __iter__(self):
        return self

    def __next__(self):
        try:
            # Determine our working factory. If this fails, then we have exhausted all queries in our suite.
            working_factory = self.factory_list[self.factory_pointer]
            self.factory_pointer += 1

            # Generate the runnable that accepts a selectivity value for use with our date range queries.
            class QueryRunnableAcceptingSigma:
                def __init__(self, query_suite):
                    self.query_runnable = working_factory()
                    self.query_suite = query_suite

                def __str__(self):
                    return self.query_runnable.__str__()

                def __call__(self, *args, **kwargs):
                    date_1, date_2 = self.query_suite.generate_dates(kwargs['sigma'])
                    results = self.query_runnable.invoke(date_1=date_1, date_2=date_2, timeout=kwargs['timeout'])
                    results['dateRange'] = {'date_1': date_1, 'date_2': date_2}
                    results['sigma'] = kwargs['sigma']
                    return results

            return QueryRunnableAcceptingSigma(query_suite=self)

        except IndexError:
            raise StopIteration

    @abc.abstractmethod
    def query_0_factory(self) -> AbstractBenchmarkQueryRunnable:
        pass

    @abc.abstractmethod
    def query_1_factory(self) -> AbstractBenchmarkQueryRunnable:
        pass

    @abc.abstractmethod
    def query_6_factory(self) -> AbstractBenchmarkQueryRunnable:
        pass

    @abc.abstractmethod
    def query_7_factory(self) -> AbstractBenchmarkQueryRunnable:
        pass

    @abc.abstractmethod
    def query_12_factory(self) -> AbstractBenchmarkQueryRunnable:
        pass

    @abc.abstractmethod
    def query_14_factory(self) -> AbstractBenchmarkQueryRunnable:
        pass

    @abc.abstractmethod
    def query_15_factory(self) -> AbstractBenchmarkQueryRunnable:
        pass

    @abc.abstractmethod
    def query_20_factory(self) -> AbstractBenchmarkQueryRunnable:
        pass
