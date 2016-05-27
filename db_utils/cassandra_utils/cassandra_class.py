"""
The MIT License (MIT)
Copyright (c) Datos IO, Inc. 2015.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""


import sys
import time
import threading
from random import randint


import common.common
from common.common import report, rpc, scp, pause_execution_for_input
from db_utils.database import DatabaseCluster
from common.network_traffic_control import NetworkTrafficControl
from db_utils.cassandra_utils.failures import CassandraFailures
from db_utils.cassandra_utils.data_population import CassandraTestingCluster, do_insert


class Cassandra(DatabaseCluster):  # TODO: (Aaron) Handle non service installs.
    def __init__(self, source_params):
        DatabaseCluster.__init__(self)

        self.source_params = source_params
        self.type = 'cassandra'
        self.payload = False

        # Require some params. Fail early if not in configuration file.
        required_params = [
            'source-name',
            'username',
            'password',
            'key',
            'ips',
            'data_dir',
        ]

        for param in required_params:
            assert(param in source_params), 'Param not found: %s' % param

        self.installed_as_service = True  # TODO: (Aaron) Need to work on this to be flexible for non service.
        self.name = source_params['source-name']
        self.ip = source_params['ips'][0]  # For convenience.
        self.ips = source_params['ips']
        self.username = source_params['username']
        self.password = source_params['password']
        self.key = source_params['key']
        self.data_dir = source_params['data_dir']
        self.commitlog_dir = source_params['commit_dir'] if 'commit_dir' in source_params else source_params['data_dir'] + '/../commitlog'
        self.restore_dir = source_params['restore_dir'] if 'restore_dir' in source_params else ''
        self.cassandra_dir = source_params['cass_dir'] if 'cass_dir' in source_params else source_params['data_dir']
        self.db_user = source_params['db_user'] if 'db_user' in source_params else ''
        self.db_pass = source_params['db_pass'] if 'db_pass' in source_params else ''

        # Create a faults instance.
        self.failures = CassandraFailures(self)

        # Create a network control instance.
        self.network = NetworkTrafficControl(self.ips, self.username, self.password, self.key)

        self.do_delta_population = False
        self.do_mass_population = False

    def _deliver_payload(self):
        """
        Delivers population scripts and other goodies to the cassandra source cluster. Most stored in ~/.geppetto/
        """
        common_script_path = '%s/common/common.py' % (common.common.global_vars['geppetto_install_dir'])
        population_script_path = '%s/db_utils/cassandra_utils/data_population.py' % (common.common.global_vars['geppetto_install_dir'])
        schema_folder_path = '%s/db_utils/cassandra_utils/schema' % (common.common.global_vars['geppetto_install_dir'])
        for ip in self.ips:
            report('Updating Geppetto payload on {%s}.' % ip)
            to_path = '%s@%s:~/.geppetto/' % (self.username, ip)
            # rpc(ip, 'rm -rf ~/.geppetto', self.username, self.password, self.key, suppress_output=True)
            rpc(ip, 'mkdir -p ~/.geppetto/common', self.username, self.password, self.key, suppress_output=True)
            rpc(ip, 'touch ~/.geppetto/common/__init__.py', self.username, self.password, self.key, suppress_output=True)
            scp(common_script_path, '%s/common/' % to_path, self.password, self.key, suppress_output=True)
            scp(population_script_path, to_path, self.password, self.key, suppress_output=True)
            scp(schema_folder_path, to_path, self.password, self.key, is_dir=True, suppress_output=True)

        self.payload = True
        return True

    def nodetool_status(self):
        # Cycle through the nodes until we get a result from nodetool status.
        for ip in self.source_params['ips']:
            out, err = rpc(ip, "nodetool status | grep 'UN\|UL\|UJ\|UM\|DN\|DL\|DJ\|DM\|===='", self.username, self.password, self.key, suppress_output=True)
            if any(x in out for x in ['UN', 'UL', 'UJ', 'UM', 'DN', 'DL', 'DJ', 'DM']):
                return out
        response = pause_execution_for_input('No status received from Cassandra Nodetool', level='info')
        if response == 'r':
            self.nodetool_status()

    def cfstats(self):
        # Cycle through the nodes until we get a result from nodetool cfstats.
        for ip in self.ips:
            out, _ = rpc(ip, "nodetool cfstats", self.username, self.password, self.key)
            # TODO: (Aaron) finish ...
            return out

    def status(self):
        return self.nodetool_status()

    def db_stop(self, ip):
        rpc(ip, 'sudo service cassandra stop', self.username, self.password, self.key, timeout=60*2)

    def db_start(self, ip):
        rpc(ip, 'sudo service cassandra start', self.username, self.password, self.key, timeout=60*2)

    def node_reboot(self, ip):
        rpc(ip, 'sudo reboot now', self.username, self.password, self.key, timeout=60*2)

    def node_shutdown(self, ip):
        rpc(ip, 'sudo halt', self.username, self.password, self.key, timeout=60*2)

    def node_restore(self, ip):
        pass

    def shutdown(self):
        """
        Shutdown the whole db cluster.
        """
        for ip in self.ips:
            rpc(ip, 'sudo service cassandra stop', self.username, self.password, self.key, timeout=60*2)

    def query(self, query, no_pause=False, suppress_reporting=False, retries=5):
        """
        Performs a cql query on the database.
        """
        assert(retries >= 0)

        # Format the query and make sure we have trailing ';'
        query = query.strip(' ')

        if not query:
            return

        if query[-1] != ';':
            query += ' ;'

        cluster = CassandraTestingCluster(self.ips, self.db_user, self.db_pass)
        if not cluster.connect():
            report('Error cannot connect to Cassandra cluster', 'critical')
            if not no_pause:
                response = pause_execution_for_input('Error cannot connect to Cassandra cluster.')
                if response == 'r':
                    result, success = self.query(query)
                else:
                    return '', False
            else:
                return '', False
        else:
            # Persistent retry, then prompt use for action if still error.
            i = 0
            wait_times = [0, 5, 15, 60, 60,]
            result, success = '', False
            while i <= retries:
                if not suppress_reporting:
                    report(query)
                result, success = cluster.runQuery(query)

                if success or i >= retries:
                    break

                if not suppress_reporting:
                    report(result, 'warning')
                    report(success, 'warning')

                retry_time = wait_times[min(i, len(wait_times) - 1)]
                if not suppress_reporting:
                    report('Retrying in %s seconds' % retry_time)
                time.sleep(retry_time)
                i += 1

            # If retries did not produce successful query, then prompt user for input if we allow pausing.
            if not success and not no_pause:
                response = pause_execution_for_input('Error')
                if response == 'r':  # 'retry'.
                    result, success = self.query(query, retries=0)  # Only try once on manual retries.

        cluster.disconnect()

        return result, success

    def insert(self, mgmt_object, schema_file, record_size, start_record, record_count, uuid4=None, suppress_reporting=False, cluster=None):
        """
        Does batch inserts into db from geppetto node.
        """
        if not cluster:
            cluster = CassandraTestingCluster(self.ips, self.db_user, self.db_pass)
            if not cluster.connect():
                report('ERROR: cannot connect to Cassandra cluster', 'critical')
                sys.exit(-1)

        if uuid4:
            if not suppress_reporting : report('%s do_insert(%s, %s, %s, %s, %s, %s, %s)' % (self.name, 'cluster', mgmt_object, schema_file, record_size, start_record, record_count, uuid4))
            do_insert(cluster, mgmt_object, schema_file, record_size, start_record, record_count, uuid4, suppress_output=suppress_reporting)
        else:
            if not suppress_reporting : report('%s do_insert(%s, %s, %s, %s, %s, %s)' % (self.name, 'cluster', mgmt_object, schema_file, record_size, start_record, record_count))
            do_insert(cluster, mgmt_object, schema_file, record_size, start_record, record_count, suppress_output=suppress_reporting)

        if not cluster:
            cluster.disconnect()

    def mass_population(self, schema_file='~/.geppetto/schema/schema1.txt', record_size=1024, start_record=1, record_count=50, mgmt_object='ks1.table1', replication=3, on_max_nodes=3, async=True):
        """
        Sets mass population on the cassandra cluster. Runs a script on multiple nodes.

        """
        if 'geppetto/schema' not in schema_file:
            schema_file = '~/.geppetto/schema/cassandra/' + schema_file

        if not self.payload:
            self._deliver_payload()

        # Need to start separate thread ...
        self.do_mass_population = True

        population_ips = self.ips[:on_max_nodes]

        def mass_worker():
            record_count_per_node = int(record_count / len(population_ips))
            node_start_record = start_record

            auth_string = ''
            if self.db_user:
                auth_string = '--db_user %s --db_pass %s' % (self.db_user, self.db_pass)

            for ip in population_ips:
                report('Setting mass population on cluster {%s} node {%s}.' % (self.name, ip), 'warning')

                # Clean log first.
                cmd = 'sudo rm /tmp/mass_population.log'
                rpc(ip, cmd, self.username, self.password, self.key)

                cmd = '(python ~/.geppetto/data_population.py ' \
                      '%s %s %s ' \
                      'insert ' \
                      '-r %s ' \
                      '-s %s ' \
                      '-n %s ' \
                      '-t %s ' \
                      '--replication %s ' \
                      ') > /tmp/mass_population.log &' % \
                      (ip, schema_file, auth_string,
                       record_size,
                       node_start_record,
                       record_count_per_node,
                       mgmt_object,
                       replication)

                node_start_record += record_count_per_node

                rpc(ip, cmd, self.username, self.password, self.key, no_tty=True)  # No tty so we can run as bg & disconnect.

            if not async:
                cmd = 'ps -ef | grep geppetto | grep -v grep | wc -l'
                cmd2 = 'tail -1 /tmp/mass_population.log'
                while True:
                    try:
                        report('Populating ...')

                        processes_running = 0
                        for ip in population_ips:
                            out, err = rpc(ip, cmd, self.username, self.password, self.key, suppress_output=True)
                            out2, err2 = rpc(ip, cmd2, self.username, self.password, self.key, suppress_output=True)
                            report('<%s> %s' % (ip, out2))
                            try:
                                processes_running += int(out)
                            except Exception as e:
                                report(e, 'critical')
                                raise
                        if processes_running == 0:
                            break
                    except Exception as e:
                        report(e, 'critical')
                        break

                    time.sleep(15)

        mass_worker()

    def delta_population(self, schema_file='~/.geppetto/schema/schema1.txt', record_size=1024, start_record=1, mgmt_object='ks1.table1', insert_percentage=70, bytes_per_hour=1, replication=3):
        """
        Creates a delta population on the cassandra cluster. Runs a script on one cassandra node and checks status.
        :param record_size: Record size.
        :param start_record: Starting record number.
        :param mgmt_object: keyspace.table format.
        :param insert_percentage: Insert percentage.
        :return:
        """

        DELAY_MS = 1000
        LOOP_MIN = 5  # Minimum of 1

        if not self.payload:
            self._deliver_payload()

        workload_ip = self.ips[randint(0, len(self.ips) - 1)]

        # Need to start separate thread ...
        self.do_delta_population = True

        # Do rate calculations.
        records_per_hour = max(1, bytes_per_hour / record_size)
        records_per_min = max(1, records_per_hour / 60)
        records_per_interval = records_per_min * LOOP_MIN

        # Batch calculations.
        num_batches = max(60 * 1, 60 * (LOOP_MIN - 1))  # We keep an empty minute at the end for everything to complete and reduce stress on system.
        batch_size = max(1, records_per_interval / num_batches)

        # If we don't need that many batches, recalculate the num_batches and batch_size.
        if records_per_interval < num_batches:
            num_batches = records_per_interval  # We know this should be done 1 minute before next loop.
            batch_size = 1

        # Build command to stop previous delta populations.
        cmd1 = '''ps -ef | grep gepp | grep -v grep | grep %s | grep "\-b" | tr -s " " | cut -d" " -f2 | xargs kill''' % mgmt_object

        auth_string = ''
        if self.db_user:
            auth_string = '--db_user %s --db_pass %s' % (self.db_user, self.db_pass)

        cmd2 = '(python ~/.geppetto/data_population.py ' \
               '%s %s %s ' \
               'update ' \
               '-r %s ' \
               '-s %s ' \
               '-t %s ' \
               '-i %s ' \
               '-b %s ' \
               '-d %s ' \
               '-c %s ' \
               '--replication %s ' \
               ') > /tmp/delta_updater.log &' % \
               (workload_ip, schema_file, auth_string,
                record_size,
                start_record,
                mgmt_object,
                insert_percentage,
                batch_size,
                DELAY_MS,
                num_batches,
                replication)

        def delta_worker():
            # Loop every 5 minutes and reinitialize delta.
            while self.do_delta_population:
                # Stop previous populations, in the case they are still going.
                rpc(workload_ip, cmd1, self.username, self.password, self.key)
                time.sleep(2)

                # Start new batch of populations.
                rpc(workload_ip, cmd2, self.username, self.password, self.key, no_tty=True)  # No tty so we can run as bg & disconnect.
                report('{%s} delta population set on node %s.' % (mgmt_object, workload_ip))
                time.sleep(60 * LOOP_MIN)  # Sleep LOOP_MIN min, allow delta to complete and settle, then cycle again. (A more dependable way)

        t = threading.Thread(target=delta_worker)
        t.setDaemon(True)
        t.start()

    def stop_mass_population(self):
        self.do_mass_population = False
        cmd = '''ps -ef | grep -v grep | grep geppetto | awk '{print $2}' | xargs kill -9'''
        for ip in self.ips:
            rpc(ip, cmd, self.username, self.password, self.key)

    def stop_delta_population(self):
        self.do_delta_population = False

    def stop_population(self):
        """
        Stops both delta and mass population on this cluster.
        """
        self.stop_mass_population()
        self.stop_delta_population()

    def clean(self):
        """
        Caution! Empties database directories and commit logs for all nodes in db.
        :return:
        """
        report('Cleaning data and commitlog directories for cluster {%s}' % (self.name), 'warning')
        cmd = 'sudo service cassandra stop'
        for ip in self.ips:
            rpc(ip, cmd, self.username, self.password, self.key)

        time.sleep(10)

        cmd_list = [
            'rm -f ~/.__jmxcmd*',
            'sudo rm -rf %s/*' % self.data_dir,
            'sudo rm -rf %s/*' % self.commitlog_dir,
            'sudo service cassandra start',
        ]
        for ip in self.ips[:1]:
            for cmd in cmd_list:
                rpc(ip, cmd, self.username, self.password, self.key)

        time.sleep(30)

        for ip in self.ips[1:]:
            for cmd in cmd_list:
                rpc(ip, cmd, self.username, self.password, self.key)

        time.sleep(30)
        report('Status cluster {%s} \n %s' % (self.name, self.status()))

    def remove(self, ks, table=None):
        """
        Removes ks or table from db.
        :param table: If provided, only drops this table from given keyspace else drops whole keyspace.
        """
        if table:
            # TODO: (Aaron) Fix check for if it exists.
            # cql = "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = '%s' ;" % (ks)
            # result, success = self.query(cql, no_pause=True)

            # If the table does not exist yet short circuit, else we can continue to drop that table.
            # if not table in str(result):
            #     return

            cql = 'DROP TABLE %s.%s ;' % (ks, table)
        else:
            # TODO: (Aaron) Fix check for if it exists.
            # Check to see if the KS exists, if so do nothing.
            # cql = 'SELECT * FROM system.schema_keyspaces;'  # Grab keyspaces.
            # result, success = self.query(cql, no_pause=True)

            # if not ks in str(result[0]):
            #     return
            cql = 'DROP KEYSPACE %s ;' % ks

        result, success = self.query(cql, no_pause=True)
        return result, success

    def set_compaction_strategy(self, strategy, ks, table):
        """
        Sets the compaction strategy on a cassandra table for a given cluster.
        :param strategy: Cassandra compaction strategy to use. Valid strategies are: 'STCS', 'DTCS', 'LCS'.
        :param ks: Keyspace.
        :param table: Table.
        :return: None
        """
        if strategy == 'STCS':
            strategy = 'SizeTieredCompactionStrategy'
        elif strategy == 'DTCS':
            strategy = 'DateTieredCompactionStrategy'
        elif strategy == 'LCS':
            strategy = 'LeveledCompactionStrategy'
        assert(strategy in ['SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy', 'LeveledCompactionStrategy'])

        cql = ''

        if strategy == 'SizeTieredCompactionStrategy':
            cql = "ALTER TABLE %s.%s WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 6 } ;" % (ks, table)
        elif strategy == 'DateTieredCompactionStrategy':
            cql = "ALTER TABLE %s.%s WITH compaction = {'class' : 'DateTieredCompactionStrategy' } ;" % (ks, table)
        elif strategy == 'LeveledCompactionStrategy':  # Explicit with this else.
            cql = "ALTER TABLE %s.%s WITH compaction = { 'class' :  'LeveledCompactionStrategy'  } ;" % (ks, table)

        self.query(cql)

    def get_compaction_history(self):
        cmd = 'nodetool compactionhistory'
        for ip in self.ips:
            try:
                out, err = rpc()
                return out
            except:
                pass
        return ''
