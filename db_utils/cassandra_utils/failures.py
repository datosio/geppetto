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


import time
import random
import collections


import common
from common.common import report, rpc, add_test_note


def pick_x_different_num(x, min_, max_):
    assert(x <= (max_ - min_ + 1))
    picks = []
    while len(picks) < x:
        candidate = random.randint(min_, max_)
        if candidate not in picks:
            picks.append(candidate)
    return picks


class CassandraFailures(object):
    def __init__(self, cassandra):
        self.cassandra = cassandra

    def single_random_db_failure(self, wait_time_min=0, run_time_min=10, time_length_of_failure=5, max_failure_repeats=1, randomness_time_injection=90):
        """
        Shuts down a random cassandra node for a given time with some randomness thrown in for timing.
        """
        assert(wait_time_min >= 0)
        assert(run_time_min >= 0.1)
        assert(time_length_of_failure >= 0.2)
        assert(max_failure_repeats > 0)

        self.cassandra.status()

        start_time = time.time()
        time.sleep(60 * wait_time_min)

        pick = pick_x_different_num(1, 0, len(self.cassandra.ips) - 1)[0]
        for _ in xrange(max_failure_repeats):
            time.sleep(random.randint(0, randomness_time_injection))  # Randomize time that db fails.
            currently_down = collections.deque()

            try:
                # Bring down the db node for some time.
                currently_down.append(pick)
                add_test_note('%s' % (self.cassandra.ips[pick]))
                self.cassandra.db_stop(self.cassandra.ips[pick])
                self.cassandra.status()  # Let's see the db state.
                time.sleep(60 * random.randint(time_length_of_failure * 3/4, time_length_of_failure))

                # Bring db node back up.
                self.cassandra.db_start(self.cassandra.ips[pick])
                currently_down.popleft()
                time.sleep(20)
                self.cassandra.status()

            except(KeyboardInterrupt, SystemExit) as e:
                # Do some clean up (restore db nodes) and some reporting, then re-raise exception.
                report('Exit detected ... restoring db state', 'critical')
                for i in currently_down:
                    self.cassandra.db_start(self.cassandra.ips[pick])
                time.sleep(20)
                self.cassandra.status()  # Logs will capture output.
                global global_vars
                global_vars['test_status'] = 'Aborted'
                add_test_note(e)
                raise e

            # Exit failure loop if we've reached max time.
            if (time.time() + wait_time_min * 60 - start_time >= run_time_min * 60):
                break


    def single_random_node_failure(self, wait_time_min=0, run_time_min=10, time_length_of_failure=5, max_failure_repeats=1, randomness_time_injection=90):
        """
        Shuts down a random cassandra node for a given time with some randomness thrown in for timing.
        """
        assert(wait_time_min >= 0)
        assert(run_time_min >= 0.1)
        assert(time_length_of_failure >= 0.2)
        assert(max_failure_repeats > 0)
        assert(randomness_time_injection >= 0)

        self.cassandra.status()

        start_time = time.time()
        time.sleep(60 * wait_time_min)

        pick = pick_x_different_num(1, 0, len(self.cassandra.ips) - 1)[0]
        for _ in xrange(max_failure_repeats):
            time.sleep(random.randint(0, randomness_time_injection))  # Randomize time that db fails.
            currently_down = collections.deque()

            try:
                # Bring down the db node for some time.
                currently_down.append(pick)

                try:
                    note = ''
                    add_test_note(note)
                    rpc(self.cassandra.ips[pick], '(nohup sudo ifdown eth0; sleep %s ; sudo ifup eth0 ; ) > /tmp/datos_failure.log &' % (time_length_of_failure * 60), self.cassandra.username, self.cassandra.password, self.cassandra.key)
                except:
                    report('Could not connect to node {%s}.' % db.ips[pick], 'warning')

                self.cassandra.status()  # Let's see the db state.
                time.sleep(60 * time_length_of_failure + 60)

                # Bring db node back up.
                self.cassandra.node_restore(self.cassandra.ips[pick])  # Currently we don't have good way to restore so this does nothing.
                currently_down.popleft()
                time.sleep(20)
                self.cassandra.status()

            except(KeyboardInterrupt, SystemExit) as e:
                # Do some clean up (restore db nodes) and some reporting, then re-raise exception.
                report('Exit detected ... restoring db state', 'critical')
                for i in currently_down:
                    self.cassandra.node_restore(db.ips[pick])
                time.sleep(20)
                self.cassandra.status()  # Logs will capture output.
                global global_vars
                global_vars['test_status'] = 'Aborted'
                add_test_note(e)
                raise e

            # Exit failure loop if we've reached max time.
            if (time.time() + wait_time_min * 60 - start_time >= run_time_min * 60):
                break


    def random_db_failures(self, wait_time_min=0, run_time_min=10, max_num_failed=1, max_failure_repeats=1):
        """
        Shuts down cassandra nodes for given time, in a random pattern within specifications.
        """
        assert(max_num_failed > 0)
        assert(run_time_min >= 0.1)
        assert(max_num_failed > 0)
        assert(max_failure_repeats > 0)

        self.cassandra.status()  # Logs will capture output.

        start_time = time.time()
        time.sleep(60 * wait_time_min)
        for _ in xrange(max_failure_repeats):
            picks = pick_x_different_num(max_num_failed, 0, len(self.cassandra.ips) - 1)

            currently_down = collections.deque()

            try:
                # First bring down those db's with some randomness thrown in.
                for i in xrange(len(picks)):
                    currently_down.append(i)
                    self.cassandra.db_stop(self.cassandra.ips[picks[i]])
                    time.sleep(random.randint(0, 60))  # TODO: (Aaron) Can make this more sophisticated.

                # Let them be down for a random period.
                time.sleep(random.randint(0, 60 * 2))  # TODO: (Aaron) Can make this more sophisticated.

                # Let's stay advised with what's down.
                self.cassandra.status()  # Logs will capture output.

                # Now bring back up, with some randomness thrown in.
                for i in xrange(len(picks)):
                    self.cassandra.db_start(self.cassandra.ips[picks[i]])
                    currently_down.popleft()
                    time.sleep(random.randint(0, 30))  # TODO: (Aaron) Can make this more sophisticated.

                time.sleep(20)  # Need to let Nodes rejoin cluster properly.
                # Let's stay advised with what's up again.
                self.cassandra.status()  # Logs will capture output.

                # Sleep random time before next cycle.
                time.sleep(0)

            except(KeyboardInterrupt, SystemExit) as e:
                # Do some clean up (restore db nodes) and some reporting, then re-raise exception.
                report('Exit detected ... restoring db state', 'critical')
                for i in currently_down:
                    self.cassandra.db_start(self.cassandra.ips[picks[i]])
                time.sleep(20)
                self.cassandra.status()  # Logs will capture output.
                global global_vars
                global_vars['test_status'] = 'Aborted'
                add_test_note(e)

                raise e

            # Exit failure loop if we've reached max time.
            if (time.time() + wait_time_min * 60 - start_time >= run_time_min * 60):
                break


    def random_node_failures(self, wait_time_min=0, run_time_min=10, max_num_failed=1, max_failure_repeats=1):
        """
        Simulates a node failure via rebooting a node. # TODO: (Aaron) currently assumes that node reboots to working condition. (need mounts in fstab and firewalls down and cass start as service.)
        """
        assert(max_num_failed > 0)
        report(self.cassandra.status())

        start_time = time.time()
        time.sleep(60 * wait_time_min)
        for _ in xrange(max_failure_repeats):
            picks = pick_x_different_num(max_num_failed, 0, len(self.cassandra.ips) - 1)
            currently_down = collections.deque()

            try:
                # First bring down those db's with some randomness thrown in.
                for i in xrange(len(picks)):
                    currently_down.append(i)
                    self.cassandra.node_reboot(self.cassandra.ips[picks[i]])  # TODO: (Aaron) Let's do this with a ifdown etc like above.
                    time.sleep(random.randint(0, 30))  # TODO: (Aaron) Can make this more sophisticated.

                # Let's stay advised with what's down.
                report(self.cassandra.status())

                # This is for future node failure implementation when we have a way to reboot like wake on lan.
                # Let them be down for a random period ... but long enough to reboot.
                time.sleep(random.randint(60, 60*2))  # TODO: (Aaron) Can make this more sophisticated.

                # Now bring back up, with some randomness thrown in.
                for i in xrange(len(picks)):
                    self.cassandra.node_restore(self.cassandra.ips[picks[i]])
                    currently_down.popleft()
                    time.sleep(random.randint(0, 30))  # TODO: (Aaron) Can make this more sophisticated.

                time.sleep(60)  # Need to let Nodes rejoin cluster properly.
                # Let's stay advised with what's up again.
                report(self.cassandra.status())

            except(KeyboardInterrupt, SystemExit) as e:
                # Do some clean up (restore db nodes) and some reporting, then re-raise exception.
                report('Exit detected ... restoring db state', 'critical')
                for i in currently_down:
                    self.cassandra.db_start(self.cassandra.ips[picks[i]])
                self.cassandra.status()  # Logs will capture output.
                global global_vars
                global_vars['test_status'] = 'Aborted'
                add_test_note(e)
                raise e

            # Exit failure loop if we've reached max time.
            if (time.time() + wait_time_min * 60 - start_time >= 60 * run_time_min):
                break


    def db_remove_random_node(self, wait_time_min=0, run_time_min=10, randomness_time_injection=0):
        """
        Removes node from cassandra cluster for given time.
        :param db:
        :param wait_time_min:
        :param run_time_min:
        :param randomness_time_injection:
        :return:
        """
        assert(wait_time_min >= 0)
        assert(run_time_min >= 0)
        assert(randomness_time_injection >= 0)

        time.sleep(wait_time_min * 60)
        time.sleep(random.randint(0, randomness_time_injection))

        pick = pick_x_different_num(1, 0, len(self.cassandra.ips) - 1)[0]
        ip = self.cassandra.ips[pick]
        down_time = int(max(0, run_time_min * 60 - 45))  # We subtract about 45 seconds due to reboot time at the end.

        note= 'Chose {%s} node to be removed for %s seconds.' % (ip, down_time)
        add_test_note(note)

        cmd = '(nohup sudo ifdown eth0; sleep %s ; sudo ifup eth0 ; sudo reboot now) > /tmp/datos_failure.log &' % (down_time)  # disable eth0 then reboot at end to simulate failure.
        rpc(ip, cmd, self.cassandra.username, self.cassandra.password, self.cassandra.key)

        ip2 = self.cassandra.ips[(pick + 1) % len(self.cassandra.ips)]
        cmd = 'nodetool removenode %s' % ip
        rpc(ip2, cmd, self.cassandra.username, self.cassandra.password, self.cassandra.key)


    def db_add_random_node(self, wait_time_min=0, randomness_time_injection=0):
        """
        Removes node from db instantly and then "adds" the node again at the given time.
        :param db:
        :param wait_time_min:
        :param randomness_time_injection:
        :return:
        """
        assert(wait_time_min >= 0)
        assert(randomness_time_injection >=0)

        pick = pick_x_different_num(1, 0, len(self.cassandra.ips) - 1)[0]
        ip = self.cassandra.ips[pick]
        down_time = wait_time_min * 60 + random.randint(0, randomness_time_injection)

        note = 'Chose {%s} node to be added after %s seconds.' % (ip, down_time)
        add_test_note(note)
        cmd = '(nohup sudo ifdown eth0; sleep %s ; sudo ifup eth0 ; sudo reboot now) > /tmp/datos_failure.log &' % (down_time)  # disable eth0 then reboot at end to simulate failure.
        rpc(ip, cmd, self.cassandra.username, self.cassandra.password, self.cassandra.key)

        ip2 = self.cassandra.ips[(pick + 1) % len(self.cassandra.ips)]
        cmd = 'nodetool removenode %s' % ip
        rpc(ip2, cmd, self.cassandra.username, self.cassandra.password, self.cassandra.key)



