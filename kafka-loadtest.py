from __future__ import print_function
from __future__ import division
import json
import subprocess
import argparse
import requests
import csv
import re
import time
import datetime
import collections
from sys import exit
from multiprocessing import Process, Queue


class TestResults:
    def __init__(self):
        self.reports = []
        self.worker_results = {}

    def create_worker_list(self, current_index):
        self.worker_results[current_index] = collections.OrderedDict()

    def add_kafka_worker_results(self, load_test_results, worker, current_index):
        self.worker_results[current_index][worker] = collections.OrderedDict({
            "records_sent": load_test_results[0],
            "records_per_sec": load_test_results[1],
            "MB_per_sec": load_test_results[2],
            "ms_avg_latency": load_test_results[3],
            "ms_max_latency": load_test_results[4],
            "ms_50th": load_test_results[5],
            "ms_95th": load_test_results[7],
            "ms_99th": load_test_results[9],
            "ms_99.9th": load_test_results[11]
        })

    def print_worker_dict(self):
        print("printing worker dict: %s" % self.worker_results)


def arg_parse():
    parser = argparse.ArgumentParser(description='used to create a rebalanced json file.')
    parser.add_argument('--loadTests', default='laptop-load-test.conf', help='the load test config')
    parser.add_argument('--influxUrl', default='http://localhost:8086',
                        help='influxdb url (ex. http://localhost:8086')
    parser.add_argument('--influxdb', default='kafka_broker',
                        help='influxdb database (ex. kafka_broker')
    parser.add_argument('--kafkaPerfTest', default='/usr/src/kafka_2.12-0.11.0.0/bin/kafka-producer-perf-test.sh',
                        help='the full path of the kafka producer perf test script')
    parser.add_argument('--kafkaBootstrapBroker', default='localhost:9092',
                        help='the bootstrap broker to run the tests against')
    parser.add_argument('--outputFile', default='{}-kafka-load-test-results.csv'.format(datetime.datetime.strftime(datetime.datetime.utcnow(), "%Y-%m-%d_%H-%M-%S")),
                        help='the csv output file of the tests')
    parser.add_argument('--fast', action='store_true', help='reduces sleep times for testing')

    args = parser.parse_args()

    return args


def setup_and_run_load_test(args, test_params, test_results):
    test_params_mapped = csv_params_to_dict(test_params)
    throughput = None
    run_test = True
    env = {}
    env['application'] = 'kafka'

    load_test_iterations_and_duration(test_params_mapped)

    grafana_start = int(time.time() * 1000) - 60000

    while run_test:
        print("test params: {}".format(test_params_mapped))
        throughput = get_throughput(test_params_mapped, throughput)

        if (test_params_mapped['ending_throughput'] and throughput <= test_params_mapped['ending_throughput']) or \
                        test_params_mapped['ending_throughput'] is False:
            # Check the status of the cluster and wait if it is in an unhealthy state
            query_start = int(time.time() * 1000 - 60000)
            query_end = int(time.time() * 1000)
            influx_query = "SELECT count(\"under_replicated_partitions\") FROM jolokia WHERE " \
                           "under_replicated_partitions > 0 AND time > %sms AND time < %sms" \
                           % (query_start, query_end)
            under_replicated_partitions = query_influxdb_and_return_dict(args.influxUrl, args.influxdb, influx_query)
            while under_replicated_partitions is not None:
                print("Kafka unhealthy. Under Replicated count: {}".format(under_replicated_partitions['count']))
                time.sleep(60)
                query_start = int(time.time() * 1000 - 60000)
                query_end = int(time.time() * 1000)
                influx_query = "SELECT count(\"under_replicated_partitions\") FROM jolokia WHERE " \
                               "under_replicated_partitions > 0 AND time > %sms AND time < %sms" \
                               % (query_start, query_end)
                under_replicated_partitions = query_influxdb_and_return_dict(args.influxUrl, args.influxdb, influx_query)

            test_results.reports.append(test_params_mapped)
            current_index = len(test_results.reports) - 1
            test_results.create_worker_list(current_index)
            test_results.reports[-1].update({'current_throughput': throughput})
            test_start = int(time.time() * 1000)
            run_load_test(throughput, args, test_params_mapped, test_results)
            test_end = int(time.time() * 1000)
            # print("test results: {}".format(test_results.worker_results))

            aggregate_last_worker_data(test_results, test_params_mapped)

            get_influxdb_test_results(args, env, test_end, test_results, test_start)
            grafana_link = {
                "grafana": "https://localhost:3000/dashboard/db/kafka-brokers?orgId=1&from={}&to={}&refresh=1m".format(
                    test_start - 10000, test_end + 25000)}
            test_results.reports[-1].update(grafana_link)

            write_result_to_csv_file(args, test_results)

            print(grafana_link['grafana'])
            if args.fast:
                time.sleep(1)
            else:
                print("Waiting 60 seconds between iterations.")
                time.sleep(60)

            query_start = int(test_end - 60000)
            query_end = int(test_end)
            influx_query = "SELECT count(\"under_replicated_partitions\") FROM jolokia WHERE " \
                           "under_replicated_partitions > 0 AND time > %sms AND time < %sms" \
                           % (query_start, query_end)
            under_replicated_partitions = query_influxdb_and_return_dict(args.influxUrl, args.influxdb, influx_query)

            if under_replicated_partitions > 0.0:
                print("kafka ded!")
                break

            percentage_of_throughput = (test_results.reports[-1]['current_throughput'] -
                                        test_results.reports[-1]['worker_records_per_sec']) / \
                                        test_results.reports[-1]['current_throughput']
            if percentage_of_throughput > 0.1:
                print("The producer is falling behind or Kafka can't handle the throughput")
                break

        else:
            break

    grafana_end = int(time.time() * 1000) + 60000

    grafana_link = {"grafana": "https://localhost:3000/dashboard/db/kafka-brokers?orgId=1&from={}&to={}&refresh=1m".format(
        grafana_start - 10000, grafana_end + 25000)}

    print("ending test results: {}".format(test_results.reports[-1]))
    print(grafana_link["grafana"])

    return test_results


def get_throughput(test_params_mapped, throughput):
    if throughput is None:
        throughput = test_params_mapped['starting_throughput']
    else:
        throughput = throughput + test_params_mapped['incrementer']
    return throughput


def get_influxdb_test_results(args, env, test_end, test_results, test_start):
    # check total cluster messages/s
    influx_query = "select median(msg_per_sec) AS \"cluster_msg/s\" from (SELECT non_negative_derivative(sum(messages_in_per_second_Count)" \
                   ", 1s) AS msg_per_sec FROM jolokia WHERE " \
                   "time > %sms AND time < %sms GROUP BY time(10s))" \
                   % (test_start, test_end)
    influxdb_response_cluster_msg_per_sec = query_influxdb_and_return_dict(args.influxUrl, args.influxdb, influx_query)
    test_results.reports[-1].update(influxdb_response_cluster_msg_per_sec)

    influxdb_total_throughput = test_results.reports[-1]['worker_MB_per_sec'] * test_results.reports[-1]['workers']
    test_results.reports[-1].update({"total_MB_per_sec": influxdb_total_throughput})
    # query for under-replicated partitions
    influx_query = "SELECT count(\"under_replicated_partitions\") FROM jolokia WHERE " \
                   " under_replicated_partitions > 0 AND time > %sms AND time < %sms" \
                   % (test_start, test_end)
    influxdb_response_under_rep_part = query_influxdb_and_return_dict(args.influxUrl, args.influxdb, influx_query)

    influx_query = "SELECT count(\"under_replicated_partitions\") FROM jolokia WHERE time > %sms AND time < %sms" \
                   % (test_start, test_end)
    influxdb_response_under_rep_part_total = query_influxdb_and_return_dict(args.influxUrl, args.influxdb, influx_query)

    if influxdb_response_under_rep_part:
        percentage = influxdb_response_under_rep_part['count'] / influxdb_response_under_rep_part_total['count']
        test_results.reports[-1].update(dict({"under_replicated_percentage": percentage}))
    else:
        test_results.reports[-1].update(dict({"under_replicated_percentage": 0.0}))

    influx_query = "SELECT (100 - mean(usage_idle)) * .01 AS cpu_usage FROM cpu WHERE cpu = 'cpu-total' AND application =~ /%s/ AND time > %sms AND time < %sms" \
                   % (env['application'], test_start, test_end)
    influxdb_response_cpu = query_influxdb_and_return_dict(args.influxUrl, args.influxdb, influx_query)
    test_results.reports[-1].update(influxdb_response_cpu)

    influx_query = "SELECT mean(usage_iowait) * .01 AS cpu_iowait FROM cpu WHERE cpu = 'cpu-total' " \
                   " AND application =~ /%s/ AND time > %sms AND time < %sms" \
                   % (env['application'], test_start, test_end)
    influxdb_response_iowait = query_influxdb_and_return_dict(args.influxUrl, args.influxdb, influx_query)
    test_results.reports[-1].update(influxdb_response_iowait)

    influx_query = "SELECT mean(\"diskio_pct\") AS diskio_pct FROM (SELECT non_negative_derivative(max(\"io_time\"), 1ms) as diskio_pct FROM diskio WHERE \"name\" <> 'xvda' " \
                   " AND application =~ /%s/ AND time > %sms AND time < %sms GROUP BY time(10s))" \
                   % (env['application'], test_start, test_end)
    influxdb_response_diskio = query_influxdb_and_return_dict(args.influxUrl, args.influxdb, influx_query)
    if influxdb_response_diskio is None:
        test_results.reports[-1].update({'diskio_pct': 'UNKNOWN'})
    else:
        test_results.reports[-1].update(influxdb_response_diskio)

    influx_query = "SELECT mean(network_processor_idle_pct) * -1 as net_usage FROM jolokia WHERE" \
                   " application =~ /%s/ AND time > %sms AND time < %sms" \
                   % (env['application'], test_start, test_end)
    influxdb_response_net_usage = query_influxdb_and_return_dict(args.influxUrl, args.influxdb, influx_query)
    influxdb_response_net_usage['net_usage'] = round(influxdb_response_net_usage['net_usage'] + 1, 4)
    test_results.reports[-1].update(influxdb_response_net_usage)


def load_test_iterations_and_duration(test_params_mapped):
    if test_params_mapped['ending_throughput']:
        iterations = (test_params_mapped['ending_throughput'] - test_params_mapped['starting_throughput']) / \
                     test_params_mapped['incrementer'] + 1
    else:
        kafka_estimated_msg_limit = 100000 * test_params_mapped['partitions']
        iterations = (kafka_estimated_msg_limit - test_params_mapped['starting_throughput']) / \
                     test_params_mapped['incrementer']
    tests_duration = round(float(iterations) * float(test_params_mapped['duration']) / 60.0, 1)
    if tests_duration > 60.0:
        tests_duration /= 60.0
        print('The current loadtest could run for up to %s iterations and up to %s hours'
              % (iterations, tests_duration))
    else:
        print('The current loadtest could run for up to %s iterations and up to %s minutes'
              % (iterations, tests_duration))


def run_load_test(throughput, args, test_params_mapped, test_results):
    num_records = throughput * test_params_mapped['duration']
    current_index = len(test_results.reports) - 1

    # kafka-producer-perf-test.sh --topic loadtest2 --num-records 80000000 --record-size 50 --throughput 400000
    # --producer-props bootstrap.servers=localhost:9092
    shell_command = "%s --topic %s --num-records %s --record-size %s --throughput %s --producer-props " \
                    "bootstrap.servers=%s batch.size=%s linger.ms=%s acks=%s compression.type=%s buffer.memory=%s" % \
                    (args.kafkaPerfTest, test_params_mapped['topic'], num_records, test_params_mapped['record_size'],
                     throughput, args.kafkaBootstrapBroker, test_params_mapped['batch.size'],
                     test_params_mapped['linger.ms'], test_params_mapped['acks'],
                     test_params_mapped['compression.type'], test_params_mapped['buffer.memory'])
    print('shell command: {}'.format(shell_command))
    start_time = time.time()
    jobs = []
    q = Queue()
    for i in range(test_params_mapped['workers']):
        p = Process(target=run_test_in_shell, args=(i, shell_command, q,))
        jobs.append(p)
        p.start()

    for p in jobs:
        p.join()

    while not q.empty():
        for worker, stats in q.get().iteritems():
            test_results.add_kafka_worker_results(stats, worker, current_index)

    end_time = time.time()
    test_duration = round(end_time - start_time, 2)
    print("Test Duration: {}".format(datetime.timedelta(seconds=test_duration)))

    return


def run_test_in_shell(worker, shell_command, q):
    for line in execute(shell_command):
        print('.', end='')
        # print("worker: %s :: %s" % (worker, line), end="")
        last_line = line

    load_test_results = re.findall('\d+[.\d]*', last_line)
    print("\n%s: %s" % (worker, load_test_results))
    load_test_results_dict = {worker: load_test_results}

    #test_results.add_kafka_test_results(load_test_results, worker)

    #test_results.print_dict()

    # return test_results.results
    q.put(load_test_results_dict)


def query_influxdb_and_return_dict(url, db, query):
    query_url = "%s/query?db=%s&q=%s" % (url, db, query)
    influx_request = requests.request("get", query_url)

    if influx_request.status_code != 200:
        raise Exception("Influx Get Failed: ", "Status code:", influx_request.status_code, " Status text: ",
                        influx_request.text)
    else:
        # print(influx_request.content)
        influx_response = collections.OrderedDict(json.loads(influx_request.content)['results'][0])
        null = influx_response.pop('statement_id', None)
        del null
        if len(influx_response) == 0:
            return None

        influx_response_dict = {}
        for response in influx_response['series']:
            influx_next_result = 0
            for idx, result in enumerate(response['values']):
                if result[1]:
                    influx_next_result = idx
                    break
            # print(zip(response['columns'], response['values'][0]))
            for item in zip(response['columns'], response['values'][influx_next_result]):
                if item[0] != "time":
                    if type(item[1]) is float:
                        influx_response_dict[item[0]] = round(item[1], 4)
                    else:
                        influx_response_dict[item[0]] = item[1]
        return influx_response_dict


def execute(cmd):
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True, shell=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)


def csv_params_to_dict(test_params):
    # topic, partition count, worker threads, test length in seconds, record size, starting throughput,
    # incrementing throughput, optional ending throughput(otherwise stops if any under-replicated partitions
    type_mapping = ['topic', 'partitions', 'workers', 'duration', 'acks', 'compression.type',
                    'buffer.memory', 'record_size', 'batch.size', 'linger.ms',
                    'starting_throughput', 'incrementer', 'ending_throughput']
    test_params_mapped = collections.OrderedDict()
    for item in type_mapping:
        if test_params:
            if re.search('^[-+]?[0-9]+$', test_params[0]):
                test_params_mapped[item] = int(test_params.pop(0))
            else:
                test_params_mapped[item] = test_params.pop(0)
        else:
            test_params_mapped[item] = None
    return test_params_mapped


def aggregate_last_worker_data(test_results, test_params_mapped):
    worker_result_agg = collections.OrderedDict()
    current_index = len(test_results.worker_results) - 1
    worker_result_names = [key for key in test_results.worker_results[current_index][0].keys()]
    for result_name in worker_result_names:
        list_of_single_result = []
        for values_of_single_worker_results in test_results.worker_results[current_index].values():
            list_of_single_result.append(float(values_of_single_worker_results[result_name]))
        name = "worker_{}".format(result_name)
        worker_result_agg[name] = round(sum(list_of_single_result) / int(test_params_mapped['workers']), 4)
    test_results.reports[-1].update(worker_result_agg)
    return


def write_result_to_csv_file(args, loadtest_result):
    current_index = len(loadtest_result.reports) - 1
    loadtest_iteration_dict = collections.OrderedDict({'test': current_index})
    loadtest_iteration_dict.update(loadtest_result.reports[current_index])

    with open(args.outputFile, 'ab') as f:
        w = csv.writer(f)
        headers = []
        for key in loadtest_iteration_dict.keys():
            headers.append(key)
        if current_index == 0:
            w.writerow(headers)
        target_row = []
        for key in headers:
            target_row.append(loadtest_iteration_dict[key])
        w.writerow(target_row)


def write_to_csv_file(args, loadtest_result):
    flat_test_results = []
    for idx, loadtest_iteration in enumerate(loadtest_result.reports):
        loadtest_iteration_dict = collections.OrderedDict({'test': idx})
        loadtest_iteration_dict.update(loadtest_iteration)
        flat_test_results.append(loadtest_iteration_dict)
    del flat_test_results[0]

    with open(args.outputFile, 'wb') as f:
        w = csv.writer(f)
        headers = []
        for key in flat_test_results[0]:
            headers.append(key)
        w.writerow(headers)
        for row in flat_test_results:
            target_row = []
            for key in headers:
                target_row.append(row[key])
            w.writerow(target_row)


def main():
    args = arg_parse()
    with open(args.loadTests, 'rb') as f:
        reader = csv.reader(f)
        loadtests = list(reader)

    if args.fast:
        time.sleep(1)
    else:
        print("Waiting 30 seconds to let the cluster recover from any other load tests...")
        time.sleep(30)

    test_results = TestResults()
    for test_params in loadtests:
        if test_params[0][0] == '#':
            continue
        setup_and_run_load_test(args, test_params, test_results)
        if args.fast:
            time.sleep(1)
        else:
            print("Waiting 180 seconds between each set of load tests...")
            time.sleep(180)
    #print("final loadtest results: {}".format(loadtest_result.reports))

    # write_to_csv_file(args, loadtest_result)
    exit()


if __name__ == "__main__":
    main()