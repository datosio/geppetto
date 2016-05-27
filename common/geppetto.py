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


import os
import re
import time
import atexit
import inspect
import datetime
import textwrap
import platform
import multiprocessing


import common
from common import report, email, update_status, seconds_to_days_hours_min_sec_string


class Geppetto(object):
    """
    Parent class of Test Runner. Implements many instance variable and Geppetto utils.
    """
    def __init__(self):
        self.test_name = ''
        self.config_name = ''
        self.configuration_dict = ''
        self.checkpoints = []
        self.start_time = time.time()
        self.upload = ''
        self.collect_logs = ''
        atexit.register(self.handle_sysexit)

        # Record where Geppetto gets installed.
        curr_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        common.global_vars['geppetto_install_dir'] = curr_dir[:-7]

        # Make a logs dir, will move everything into this directory on test completion.
        log_dir = os.path.join('./logs', str(datetime.datetime.now().strftime("%b_%d-%Y@%H:%M")))
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
            os.chmod(log_dir, 0777)
        common.global_vars['save_dir'] = log_dir

        self.log_dir = log_dir

        # Do any previous test cleanups.
        self._do_init_clean()

        # Do an initial validation of the systems.
        self._do_init_check()

    def set_init_params(self, configuration_dict, args, test_name, config_name):
        self.test_name = test_name
        self.config_name = config_name
        self.configuration_dict = configuration_dict
        self.start_time = time.time()
        self.upload = args.upload
        self.collect_logs = args.collect_logs

        # Require certain parameters to be in configuration file.  Do all parsing here!!!!! EARLLLYYYY !!!
        required_params = [

        ]

        for param in required_params:
            assert(param in self.configuration_dict), 'Param not found: %s' % param

        # Check that email given is valid.
        if args.email:
            if self._check_email_format(args.email):
                common.global_vars['email'] = args.email
            else:
                report('Invalid email address given. Emails disabled.', 'critical')

    def _do_init_clean(self):
        pass

    def _do_init_check(self):
        pass

    def do_start_message(self):
        msg = '...\n%s\n' % ('*' * 70)
        msg += '*\tTest:   %s\n' % self.test_name
        msg += '*\tConfig: %s\n' % self.config_name
        msg += '*\tLog:    %s\n' % self.log_dir
        email = common.global_vars['email']
        if email:
            msg += '*\tEmails: %s\n' % email
        else:
            msg += '*\tEmails: Disabled.\n'
        msg += '%s' % ('*' * 70)
        report(msg)

    def _check_email_format(self, email):
        if email[0] == '.' or email[-1] == '.':
            return False
        if re.match(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", email):
            return True
        return False

    def handle_sysexit(self):
        """
        Handle any sys exit exceptions ... tear down any processes necessary.
        :return:
        """
        report(' ', no_date=True, no_level=True)
        update_status('Test Complete')
        self.complete()

    def send_update(self, datos=None):
        """
        Sends an email update with status of run.
        :return:
        """
        note_queue = common.global_vars['test_notes']
        notes = ''
        while not note_queue.empty():
            notes += note_queue.get()

        checkpoint_results = ''
        for checkpoint, result in common.global_vars['checkpoints']:
            checkpoint_results += '\n+ %s: %s' % (result, checkpoint)

        # Format completed message.
        msg = 'Test Update.\n\n'
        msg += '    Name:      %s\n' % self.test_name
        msg += '    Config:    %s\n' % self.config_name
        msg += '    Log:       %s\n' %  self.log_dir
        msg += '    Time:      %s\n' % seconds_to_days_hours_min_sec_string(time.time() - self.start_time)
        if checkpoint_results:
            msg += '\nCheckpoints:\n%s\n' % checkpoint_results
        if notes:
            msg += '\nMessages:\n%s' % notes

        # Playing with the formatting a bit here to create better looking reports.
        msg = '*    %s' % msg.strip('\n').replace('\n', '\n*    ')
        report(msg, no_date=True, no_level=True)

        if common.global_vars['email']:
            email(msg, common.global_vars['email'], subject='Geppetto Test {%s} Update' % self.test_name)

    def complete(self):
        """
        Completely wrap up the test run. Do log moving, emailing, etc.
        """
        note_queue = common.global_vars['test_notes']
        notes = ''
        while not note_queue.empty():
            notes += note_queue.get()

        checkpoint_results = ''
        for checkpoint, result in common.global_vars['checkpoints']:
            checkpoint_results += '\n+ %s: %s' % (result, checkpoint)

        # Set the overall test status if not set already. (Can also be set anywhere for test faults etc)
        if not common.global_vars['test_status']:
            common.global_vars['test_status'] = 'Passed'
            for checkpoint, status in common.global_vars['checkpoints']:
                if status.lower() != 'passed':
                    common.global_vars['test_status'] = 'Failed'
                    break

        # Format completed message.
        msg = '  Status:    %s\n' % common.global_vars['test_status'] #self.passed
        msg += '  Name:      %s\n' % self.test_name
        msg += '  Config:    %s\n' % self.config_name
        msg += '  Log:       %s\n' %  self.log_dir
        msg += '  Time:      %s\n' % seconds_to_days_hours_min_sec_string(time.time() - self.start_time)

        if common.global_vars['commit_id']:
            msg += '  Commit:    %s\n' % common.global_vars['commit_id']
            msg += '  Traces:    %s\n' % common.global_vars['traceback_count']
            msg += '  Errors:    %s\n' % common.global_vars['error_count']

        if checkpoint_results:
            msg += '\nCheckpoints:\n%s\n' % checkpoint_results

        if notes:
            msg += '\nMessages:\n%s' % notes

        # Playing with the formatting a bit here to create better looking reports.
        msg = '*  %s' % msg.strip('\n').replace('\n', '\n*    ')

        msg = '*' * 70 + '\n' + msg + '\n' + '*' * 70

        # Move geppetto logs to log dir.
        os.system('mv ./logs/*.log* %s/' % self.log_dir)

        if common.global_vars['email']:
            email(msg, common.global_vars['email'], subject='Geppetto Test {%s} Results' % self.test_name)

        # Do final printout of test to terminal.
        report(msg, no_date=True, no_level=True)
