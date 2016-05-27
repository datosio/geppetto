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


import datetime
import os
import sys
import time
import json
import errno
import signal
import smtplib
import logging
import logging.handlers
import textwrap
import paramiko
import traceback
import subprocess
import multiprocessing
# from scp import SCPClient  # TODO: (Aaron) Need to add this in when switch over to new scp.
from functools import wraps


global_vars = {
    'geppetto_install_dir': None,
    'test_status': '',
    'test_notes': multiprocessing.Queue(),
    'email': None,
    'log_file': None,
    'last_email_time': None,
    'save_dir': None,
    'traceback_count': 0,
    'error_count': 0,
    'pause_reporting': multiprocessing.Event(),
    'pause_level': 'debug',
    'commit_id': '',
    'datos_ip': '',
    'datos_install_dir': '',
    'checkpoints': [],}


class DummyLock():
    def acquire(self): pass
    def release(self): pass


print_lock = DummyLock() # multiprocessing.RLock()
cli_lock = print_lock  # multiprocessing.RLock()


# Make the logs directory if it does not exist. Not included in git repository.
directory = 'logs'
if not os.path.exists(directory):
    os.makedirs(directory)


os.system('rm %s/*.log >/dev/null 2>&1' % directory)  # Make sure we're starting with clean log file.
LOG_FILENAME = '%s/geppetto.log' % directory
logging.getLogger('cassandra').setLevel(logging.CRITICAL)
logging.getLogger('transport').setLevel(logging.CRITICAL)
logging.getLogger('paramiko').setLevel(logging.CRITICAL)
formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(module)s] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1024*1024*20, backupCount=7,)
handler.setFormatter(formatter)
g_logger = logging.getLogger('GLogger')
g_logger.setLevel(logging.INFO)
g_logger.addHandler(handler)


class TimeoutException(Exception):
    pass


class Timeout(object):
    """
    Provides a timeout class. Enter <= 0 minutes to disable timeout.
    Use with a "with" statement, or as a decorator.
    Notice: this can only be used in the main thread.
    """
    def __init__(self, minutes=1, error_message=None):
        """
        :param minutes:  Timeout after x minutes. Enter <= 0 minutes to disable timeout.
        :param error_message: Message upon timeout.
        :return:
        """
        self.minutes = minutes
        self.error_message = error_message if error_message else 'Timed out after %s minutes.' % minutes

    @staticmethod
    def decorator(minutes=1, error_message=os.strerror(errno.ETIME)):
        def dec(func):
            def _handle_timeout(signum, frame):
                msg = 'Timeout Error: %s' % (error_message)
                add_test_note(msg)
                raise TimeoutException(error_message)

            def wrapper(*args, **kwargs):
                if minutes > 0:
                    signal.signal(signal.SIGALRM, _handle_timeout)
                    signal.alarm(int(minutes * 60))
                try:
                    result = func(*args, **kwargs)
                finally:
                    signal.alarm(0)
                return result

            return wraps(func)(wrapper)

        return dec

    def _handle_timeout(self, signum, frame):
        msg = 'Timeout Error: %s' % (self.error_message)
        add_test_note(msg)
        raise TimeoutException(msg)

    def __enter__(self):
        if self.minutes > 0:
            signal.signal(signal.SIGALRM, self._handle_timeout)
            signal.alarm(int(60 * self.minutes))

    def __exit__(self, type, value, traceback):
        if self.minutes > 0:
            signal.alarm(0)


def capture_exception_and_abort(func):
    """
    Put @capture_exception_and_abort above the main run function as a decorator to capture unhandled exceptions
    and set the test status to aborted.
    :param func:
    :return:
    """
    def dec(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            global_vars['test_status'] = 'Aborted'
            add_test_note('Test aborted due to exception: \n%s' % traceback.print_exc())
            sys.exit()
    return dec


def set_test_status(status):
    """
    Sets the global test status.
    :param status:  Status of whole test.
    :return:
    """
    global_vars['test_status'] = status
    report('Set test status %s' % status, 'critical')


def set_checkpoint_status(checkpoint_name, status):
    """
    Sets the status of a checkpoint in this test.
    :param checkpoint_name: Name of checkpoint.
    :param status: 'Passed' or 'Failed'
    :return:
    """
    # Colorize these outputs for easy readability.
    COLOR = '\033[91m'
    END_COLOR = '\033[0m'
    if status.lower() == 'passed':
        COLOR = '\033[36m'

    report('@@@@@@@ Checkpoint %s: %s %s %s' % (checkpoint_name, COLOR, status, END_COLOR))
    global_vars['checkpoints'] += [(checkpoint_name, status)]


def add_test_note(note):
    """
    Adds a note the the set of notes that are reported at the end of a test run and sent in the email to the user.
    :param note: Note to add.
    :return: None
    """
    global_vars['test_notes'].put("\n+ %s" % note)
    report('[NOTE] %s' % note, 'important', no_level=True)


def timestamp():
    return time.strftime("%m/%d/%Y %I:%M:%S %p", time.localtime())


def report(msg, level="info", no_date=False, no_level=False):
    """
    Reports message to log file and prints colorized output to terminal.
    """
    # Colorize output to terminal.
    WARNING_COLOR = '\33[0;35;33m'
    ERROR_COLOR = '\033[93m'  # '\33[1;31;31m'
    CRITICAL_COLOR = '\033[91m'  # '\33[1;33;37m'
    END_COLOR = '\033[0m'

    # Is reporting to terminal disabled by geppetto?
    pause_reporting = global_vars['pause_reporting'].is_set()

    msg = str(msg).strip('\n').strip('\r')

    if not msg:
        return

    if no_date:
        time_string = ''
    else:
        time_string = '[%s] ' % timestamp()


    # Handle the different report levels differently.
    if level == 'debug':
        g_logger.debug(msg)
        if not pause_reporting:
            if no_level:
                level_str = ''
            else:
                level_str = '[DEBUG] '
            sys.stdout.write('\r' + time_string + level_str + msg + '\n') ; sys.stdout.flush()  # print(time_string + '[DEBUG] ' + msg)
    elif level == 'info':
        g_logger.info(msg)
        if not pause_reporting:
            if no_level:
                level_str = ''
            else:
                level_str = '[INFO] '
            sys.stdout.write('\r' + time_string + level_str + msg + '\n') ; sys.stdout.flush()  # print(time_string + '[INFO] ' + msg)
    elif level == 'warning' or level == 'important':
        g_logger.warning(msg)
        if not pause_reporting:
            if no_level:
                level_str = ''
            else:
                level_str = '[IMPORTANT] '
            sys.stdout.write(WARNING_COLOR + time_string + level_str + msg + END_COLOR + '\n') ; sys.stdout.flush()  # print(WARNING_COLOR + time_string + '[WARNING] ' + msg + END_COLOR)
    elif level == 'error':
        g_logger.error(msg)
        if not pause_reporting:
            if no_level:
                level_str = ''
            else:
                level_str = '[ERROR] '
            sys.stdout.write(ERROR_COLOR + time_string + level_str + msg + END_COLOR + '\n') ; sys.stdout.flush()  # print(ERROR_COLOR + time_string + '[ERROR] ' + msg + END_COLOR)
    else:
        g_logger.critical(msg)
        if not pause_reporting:
            if no_level:
                level_str = ''
            else:
                level_str = '[CRITICAL] '
            sys.stdout.write(CRITICAL_COLOR + time_string + level_str + msg + END_COLOR + '\n') ; sys.stdout.flush()  # print(CRITICAL_COLOR + time_string + '[CRITICAL] ' + msg + END_COLOR)


def shell(command, workdir=os.getcwd(), timeout=60*20, retries=0, suppress_output=False, suppress_errors=False, print_real_time=False):
    """
    Run shell command locally on Geppetto host.
    """
    assert(retries >= 0)
    assert(timeout >= 0)

    for i in xrange(retries + 1):
        try:
            if not suppress_output:
                report('[Try #%s] Shell: %s' % (i + 1, command))
            p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=workdir)

            # Because Python 2.6 subprocess does not support timeout.
            # Here we're going to create and process a timeout for the shell command.
            start = time.time()
            deadline = start + timeout

            # If print real time, (maybe you don't want to so everything lumped with logging.)
            if print_real_time and not suppress_output:
                for line in iter(p.stdout.readline, b''):
                    if line.strip('\n'):
                        report(line.strip('\n'))

            # Poll for results, print status bar if taking long time.
            while time.time() < deadline and p.poll() == None:
                time.sleep(1)

            if p.poll() == None:
                raise TimeoutException('Timeout Exception \n\t%s' % command)

            out, err = p.communicate() # Grab output.
            out, err = out.strip('\n').strip('\r'), err.strip('\n').strip('\r')

            # Print command and try # if there is an error and we weren't printing information already.
            if suppress_output and err and not suppress_errors:
                report('[Try #%s] %s' % (i + 1, command), level='warning')

            # Also print elapsed time ...
            if not suppress_output:
                report(out)
            if not suppress_errors:
                report(err, level='warning')
            return out, err

        except TimeoutException as te:
            report(te, 'critical')

    return '', ''


def rpc(ip, command, user, password=None, key=None, timeout=60*20, retries=1, no_tty=False, suppress_output=False, suppress_errors=False, print_real_time=False):
    """
    Easy shell call on remote host.
    :param ip: IP of remote host.
    :param command: Command to be run.
    :param user: Username for remote host.
    :param password: Password for remote host.
    :param key: Path to pem key file.
    :param timeout: Length of command timeout in seconds.
    :param retries: Number of retries.
    :param no_tty: Disables Paramiko get_pty. You may need TTY to run sudo commands if the sudoers file requires it.
    :param suppress_output: Does not print command output.
    :param print_real_time:
    :return: (std, err)
    """
    assert(retries >= 0)
    assert(timeout >= 0)

    if key:
        key = os.path.expanduser(key)
    else:
        key = None

    for i in xrange(retries + 1):
        try:
            if not suppress_output:
                report('[Try #%s] RPC: {%s} %s' % (i + 1, ip, command))

            start = time.time()

            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=ip, username=user, password=password, key_filename=key, look_for_keys=True, port=22, timeout=timeout)
            std_in, out, err = ssh.exec_command(command, get_pty= not no_tty)
            std_in.close()

            out, err = out.read().strip('\n').strip('\r'), err.read().strip('\n').strip('\r')

            if 'Connection to %s closed' % ip in err:
                err = ''

            # Print command and try # if there is an error and we weren't printing information already.
            if suppress_output and err and not suppress_errors:
                report('[Try #%s] RPC: {%s} %s' % (i + 1, ip, command), level='warning')

            if not suppress_output:
                report(out)
            if not suppress_errors:
                report(err, level='warning')

            ssh.close()
            return out, err

        except Exception:
            report(traceback.format_exc(), level='important')
            time.sleep(3)

    report('Error Connecting.', 'warning')
    return '', 'Error Connecting.'


def scp(from_path, to_path, password=None, key=None, is_dir=False, timeout=60*20, retries=0, suppress_output=False, suppress_errors=False, print_real_time=False):
    """
    Easily scp file to remote host.
    :param from_path: Should include user@ip if remote host, else just local file path with -r if dir.
    :param to_path: Should include user@ip if remote host, else just local file path with -r if dir.
    :param key: Path to pem key file.
    :return: (std, err)
    """
    assert(retries >= 0)
    assert(timeout >= 0)

    if is_dir:
        dir_flag = '-r'
    else:
        dir_flag = ''

    if password:
        command = '''sshpass -p %s scp -o "StrictHostKeyChecking no" %s %s %s''' % (password, dir_flag, from_path, to_path)
    elif key:
        command = '''scp -i %s -o "StrictHostKeyChecking no" %s %s %s''' % (key, dir_flag, from_path, to_path)
    else:
        command = '''scp -o "StrictHostKeyChecking no" %s %s %s''' % (dir_flag, from_path, to_path)

    for i in xrange(retries + 1):
        try:
            if not suppress_output:
                report('[Try #%s] %s' % (i + 1, command))
            p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Because Python 2.6 subprocess does not support timeout.
            # Here we're going to create and process a timeout for the shell command.
            start = time.time()
            deadline = start + timeout

            # If print real time, (maybe you don't want to so everything lumped with logging.)
            if print_real_time and not suppress_output:
                for line in iter(p.stdout.readline, b''):
                    if line.strip('\n'):
                        report(line.strip('\n'))

            # Poll for results, print status bar if taking long time.
            while time.time() < deadline and p.poll() == None:
                time.sleep(1)

            if p.poll() == None:
                if i >= retries:
                    raise TimeoutException('Timeout Exception: \n\t%s' % command)
                continue

            out, err = p.communicate()  # Grab output.
            out, err = out.strip('\n').strip('\r'), err.strip('\n').strip('\r')

            # Print command and try # if there is an error and we weren't printing information already.
            if suppress_output and err and not suppress_errors:
                report('[Try #%s] %s' % (i + 1, command), level='warning')

            if not suppress_output:
                report(out)
            report(err, level='warning')
            if not suppress_errors:
                return out, err

        except TimeoutException as te:
            report(te, level='critical')

    return '', ''


# New scp coming in soon.

# def scp(mode='put', local_path='.', remote_path='.', ip='127.0.0.1', user=getpass.getuser(), password=None, key=None, timeout=60*60):
#     """
#
#     :param mode:
#     :param local_path:
#     :param remote_path:
#     :param ip:
#     :param user:
#     :param password:
#     :param key:
#     :param timeout:
#     :return: None
#     """
#     assert(timeout >= 0)
#     assert(mode in ['get', 'put'])
#
#     ssh = paramiko.SSHClient()
#     ssh.load_system_host_keys()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     ssh.connect(hostname=ip, username=user, password=password, key_filename=key, look_for_keys=True, port=22, timeout=timeout)
#     with SCPClient(ssh.get_transport()) as scp:
#         if mode == 'put':
#             print('[SCP PUT] %s to <%s>:%s' % (local_path, ip, remote_path))
#             scp.put(files=local_path, remote_path=remote_path, recursive=True)
#         else:
#             print("[SCP GET] <%s>:%s to %s" % (ip, remote_path, local_path))
#             scp.get(local_path=local_path, remote_path=remote_path, recursive=True)
#
#     ssh.close()


def convert_datos_output_to_json(cli_out):
    try:
        return json.loads(cli_out)
    except:
        return {'status': False, 'data': {}}


def seconds_to_days_hours_min_sec_string(seconds):
    secs = seconds % 60
    mins = (seconds % 3600) // 60
    hours = (seconds % 86400) // 3600
    days = seconds // 86400
    return '%i days, %i hours, %i mins, %.2f secs' % (days, hours, mins, secs)


def get_hostname_ip():
    out, err =  shell('hostname -I', suppress_output=True, suppress_errors=True)  # Does not require hostname to be set.

    # Check if err, try another method.
    if 'illegal option' in err:
        out, err = shell("ifconfig | grep inet | grep -v '::' | grep -v '127' | cut -d' ' -f2 | head -n1", suppress_output=True)
    else:
        out = out.split()[0]
    return out


def get_remote_hostname(ip, user, password, key):
    out, _ =  rpc(ip, 'hostname', user, password, key)
    return out.strip('\n').strip('\r')


def get_remote_hostname_ip(ip, user, password, key):
    out, _ =  rpc(ip, 'hostname -I', user, password, key)
    return out.strip('\n').strip('\r')


def update_status(status):
    """
    Outputs stage (status) to file, so outside scripts can keep track of Geppetto progress if desired.
    Also updates log and prints to terminal with bold separators.
    :param status: (string) status of Geppetto run.
    :return: None
    """
    msg = '*' * 70
    msg += '\n*    STATUS: %s\n' % status
    msg += '*' * 70
    report(msg, 'important', no_date=True, no_level=True)
    with open('./.status', 'w') as f:
        f.write(status)


def email(body, toaddrs, subject="Geppetto Results"):
    """
    Sends an email
    """
    fromaddr = '_from_addr_'
    msg = "\r\n".join(["From: %s" % fromaddr, "To: %s" % toaddrs, "Subject: %s" % subject, "", body])

    try:
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.starttls()
        server.login("_username_", "_pass_")
        server.sendmail(fromaddr, toaddrs, msg)
        server.quit()
    except:
        report('Error sending email. Please set proper email settings.', 'critical')


def pause_execution_for_input(message='Paused for input', level='critical', no_email=False):
    """
    Pauses execution to wait for input, possibly to fix error or check trace or error warnings. Pausing levels can also
    be set in the global vars. This works very similar to logging levels but pauses on greater than or equal to the pausing level.
    :param message:
    :return: input x from user if x != a (Abort) which is handled instead by aborting test run.
    """
    # We'll treat this as a static var, so don't have to keep resetting.
    if not hasattr(pause_execution_for_input, 'my_ip'):
        pause_execution_for_input.my_ip = get_hostname_ip()

    my_ip = pause_execution_for_input.my_ip

    levels = {
        'debug': 0,
        'info': 1,
        'warning': 2,
        'error': 3,
        'critical': 4,
    }

    # Only pause for levels greater or equal to the set global pause level.
    if levels[level] < levels[global_vars['pause_level']]:
        return

    update_status('Paused: Action Required!')

    header = '************************** Run Paused ***************************\n'
    footer = '\n* Enter (r) to retry. Enter (c) to continue. Enter (a) to abort *' \
             '\n*****************************************************************'

    message = textwrap.fill(message, 60)
    message = message.strip('\n')
    message_print_version = '*\t%s' % message.replace('\n', '\n*\t')

    print(header + message_print_version + footer)

    # Send rescue request email.  # TODO (Aaron) Fix the email info.
    if global_vars['email'] and not no_email:
        # Also, to be not annoying ... throttle the emails. If you've had an email in the last X min and are retrying, you're probably looking at this service.
        email_time_threshold = 60*3
        last_email_time = global_vars['last_email_time']
        if not last_email_time or time.time() - last_email_time >= email_time_threshold:
            name = global_vars['email'].split('@')[0].split('.')[0]
            email_body = "Help me, %s. You're my only hope.\n\n" \
                         "Geppetto\n" \
                         "%s\n" \
                         "%s\n\n" \
                         "Messages:\n" \
                         "%s" % (name.capitalize(), my_ip, time.strftime("%m/%d/%Y %I:%M:%S %p", time.localtime()), message_print_version)

            email(email_body, global_vars['email'], 'Geppetto SOS !!!')
            global_vars['last_email_time'] = time.time()

    # Disable reporting to terminal from other processes while this is in effect.
    pause_reporting = global_vars['pause_reporting']
    pause_reporting.set()

    i = 0
    while True:
        print(' >>> '),

        try:
            x = raw_input()
        except EOFError:
            i += 1
            if i < 5:
                continue
            pause_reporting.clear()
            raise

        if x in ['c', 'r']:
            pause_reporting.clear()
            return x
        elif x == 'a':
            global_vars['test_status'] = 'Aborted'
            pause_reporting.clear()
            raise Exception('User aborted.')
        else:
            print('Error: Input not recognized: %s' % (x))


class Process(multiprocessing.Process):
    """
    Subclass Process from multiprocessing and auto handle any queue and event instance passing.
    """

    def __init__(self, *args, **kwargs):
        test_notes = global_vars['test_notes']
        pause_reporting = global_vars['pause_reporting']

        def wrapper(func, test_notes, pause_reporting, **kwargs):
            """

            :param func: function to pass to multiprocessing.Process.
            :param test_notes: multiprocessing Queue() instance. Allows us to add notes to
            :param disable_reporting: multiprocessing Event() instance. Turns off reporting to terminal when input needed.
            :param kwargs: dictionary that contains all args and kwargs being sent to wrapped function.
            :return:
            """
            global_vars['test_notes'] = test_notes
            global_vars['pause_reporting'] = pause_reporting
            args_ = kwargs['args'] if 'args' in kwargs else ()
            kwargs_ = kwargs['kwargs'] if 'kwargs' in kwargs else {}
            return func(*args_, **kwargs_)

        wrapper_args = [kwargs['target'], test_notes, pause_reporting]
        wrapper_kwargs = kwargs

        multiprocessing.Process.__init__(self, target=wrapper, args=wrapper_args, kwargs=wrapper_kwargs)


def is_ssh_ready(ip, username, password=None, key=None):
    """
    Checks if it is possible to connect to the ip using the credentials.
    :param ip: ip to check for ssh connectivity
    :param username: username
    :param password: password
    :param key: pass to *.pem key
    :return: boolean of the connection state
    """
    try:
        private_key = paramiko.RSAKey.from_private_key_file(os.path.expanduser(key))
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip, username=username, password=password, pkey=private_key)
        ssh.close()
        return True
    except Exception:
        report("SSH is not responsive for %s:\n%s" % (ip, traceback.format_exc()))
        return False


def wait_for_ssh_ready(ip, username, password=None, key=None, timeout=600):
    """
    Checks if it is possible to connect to the ip using the credentials. Will be retrying until timeout is reach.
    :param ip: ip to check for ssh connectivity
    :param username: username
    :param password: password
    :param key: pass to *.pem key
    :param timeout: try for timeout seconds
    :return: boolean of the connection state
    """
    start = time.time()
    while(True):
        if is_ssh_ready(ip, username, password, key):
            report("SSH for %s is working" % ip, level="important")
            return True
        elapsed = time.time() - start
        if elapsed > timeout:
            return False
        time.sleep(20)


def get_file_content(filename):
    """
    Get the content of a non-binary file.
    :param filename: file name
    :return: file.readlines() output
    """
    f = open(filename, "r")
    new_lines = f.readlines()
    f.close()
    return new_lines


def set_file_content(filename, new_file_content, write_mode="w"):
    """
    Set the content of a non-binary file.
    :param filename: file name
    :param new_file_content: string content
    :param write_mode: writting mode, optional - "w" is default
    :return:
    """
    f = open(filename, write_mode)
    f.write(new_file_content)
    f.close()


def print_time():
    d = datetime.datetime.now()
    t = time.time()
    report("Time: %s\nStart_time: %s\n" % (str(d), int(t)))


def bytes_to_str(num):
    """
    Convert bytes to string of [1-9]['','K','M','G','T','P','E','Z']B.
    :param num: bytes as float
    :return: string of [1-9]['','K','M','G','T','P','E','Z']B
    """
    num = float(num)
    suffix = "B"
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)


def str_to_bytes(sbytes):
    """
    Convert bytes from string of [1-9]['','K','M','G','T','P','E','Z']B to float
    :param sbytes: bytes in format [1-9]['','K','M','G','T','P','E','Z']B 
    :return: bytes as float
    """
    if type(sbytes) is not str:
        report("str_to_bytes: Input must be string. You have: %s !!!" % sbytes)
        return None
    suffix = sbytes[-2].upper()
    if not sbytes.endswith("B") or (suffix not in ['','K','M','G','T','P','E','Z'] and not suffix.isdigit()):
        report("str_to_bytes: Expected format is [1-9]['','K','M','G','T','P','E','Z']B !!!. You have: %s !!!" % sbytes)
        return None
    if suffix.isdigit():
        return abs(float(sbytes[:-1]))
    elif suffix == "K":
        power = 1
    elif suffix == "M":
        power = 2
    elif suffix == "G":
        power = 3
    elif suffix == "T":
        power = 4
    elif suffix == "P":
        power = 5
    else:
        power = 6
    return abs(float(sbytes[:-2]))*pow(1024, power)


def seconds_to_str(num):
    """
    Convert seconds to [1-9]['S','M','H'] format.
    :param num: seconds as float
    :return: string of [1-9]['S','M','H']
    """
    num = float(num)
    for unit in ['S','M','H']:
        if abs(num) < 60.0:
            return "%3.1f%s" % (num, unit)
        num /= 60.0
    return "%.1f%s" % (num, 'S')


def str_to_seconds(sdate):
    """
    Convert string in [1-9]['S','M','H'] format to seconds.
    :param sdate: date in [1-9]['S','M','H'] format
    :return: seconds as float
    """
    if type(sdate) is not str:
        report("str_to_seconds: Input must be string. You have: %s !!!" % sdate)
        return None
    t_def = sdate[-1].upper()
    if not t_def in ["S", "M", "H", "D", "W"]:
        report("str_to_seconds: Expected format [1-9]['S', 'M', 'H']. You have: %s !!!" % sdate)
        return None
    if t_def == "S":
        sec = 1
    elif t_def == "M":
        sec = 60
    elif t_def == "H":
        sec = 60*60
    elif t_def == "D":
        sec = 60*60*24
    else:
        sec = 60*60*24*7
    return abs(float(sdate[:-1]))*sec


def str_to_percents(spercent):
    """
    Convert string in [1-9]% format to float.
    :param spercent: string in [1-9]% format
    :return: percent as float
    """
    if type(spercent) is not str:
        report("str_to_percents: Input must be string You have: %s !!!" % spercent)
        return None
    if not spercent.endswith("%") or not spercent[:-1].isdigit():
        report("str_to_percents: Expected format is [1-9]%% You have: %s !!!" % spercent)
        return None
    return abs(float(spercent[:-1])*0.01)


def is_host_alive(host):
    """
    Checks if given host is alive.
    :param host: Host to check.
    :return: boolean status
    """
    o, _ = shell("ping -c 10 %s" % host)
    return not ("100% packet loss" in o)


def wait_for_host(host, timeout=600):
    """
    Checks if given host is alive.
    :param host: Host to check.
    :param timeout: How long to wait.
    :return: boolean status
    """
    start = time.time()
    while True:
        if is_host_alive(host):
            return True
        elapsed = time.time() - start
        if elapsed > timeout:
            return False
        time.sleep(20)
