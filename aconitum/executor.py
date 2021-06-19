import logging.config
import datetime
import logging
import uuid
import os
import subprocess
import abc
import json


class AbstractBenchmarkRunnable(abc.ABC):
    def call_subprocess(self, command, is_log=True):
        """ Run a subprocess and return its output. """
        subprocess_pipe = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )

        resultant = ''
        for stdout_line in iter(subprocess_pipe.stdout.readline, ""):
            if stdout_line.strip() != '':
                resultant += stdout_line
                for line in resultant.strip().split('\n'):
                    if is_log:
                        self.logger.debug(line)
        subprocess_pipe.stdout.close()
        return resultant

    def __init__(self, **kwargs):
        # Create our results directory if it does not already exist.
        os.mkdir(os.getcwd() + '/' + kwargs['resultsDir'])

        # Initialize our logger.
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        # We will log to the console and to a file.
        fh = logging.FileHandler(f"{kwargs['resultsDir']}/aconitum.log")
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

        # Results will be recorded to a separate file (in lines-JSON format).
        self.results_fp = open(kwargs['resultsDir'] + '/' + 'results.json', 'w')

        self.logger.info(f'Using the following configuration: {kwargs}')
        self.working_system = kwargs['workingSystem']
        self.execution_id = str(uuid.uuid4())
        self.config = kwargs

    def log_results(self, results):
        results['logTime'] = str(datetime.datetime.now())
        results['executionID'] = self.execution_id
        results['workingSystem'] = self.working_system
        results['runtimeNotes'] = self.config['runtimeNotes']

        # To the results file.
        self.logger.debug('Recording result to disk.')
        json.dump(results, self.results_fp)
        self.results_fp.write('\n')

        # To the console.
        self.logger.debug('Writing result to console.')
        self.logger.debug(json.dumps(results))

    @abc.abstractmethod
    def perform_benchmark(self):
        pass

    def perform_post(self):
        pass

    def invoke(self):
        self.logger.info(f'Working with execution id: {self.execution_id}.')

        # Perform the benchmark.
        self.logger.info('Executing the benchmark.')
        self.perform_benchmark()

        # Perform any post action.
        self.logger.info('Flushing the logger.')
        [h.flush() for h in self.logger.handlers]

        self.perform_post()
        self.results_fp.close()
        self.logger.info('Benchmark has finished executing.')
