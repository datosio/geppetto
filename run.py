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
import sys
import argparse
import textwrap
import traceback


from common.common import report, capture_exception_and_abort
from common.geppetto import Geppetto


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--test_file', default="tests/demos/demo.py", help="Test file.")
    parser.add_argument('-c', '--config', default="configs/demo_config.py", help="Configuration file.")
    parser.add_argument('-e', '--email', default=None, help="Email to send results to.")
    parser.add_argument('-u', '--upload', action='store_true')
    parser.add_argument('--collect_logs', action='store_true')
    return parser.parse_args()


def do_welcome():
    title = """
        _____                       _   _
       / ____|                     | | | |
      | |  __  ___ _ __  _ __   ___| |_| |_ ____
      | | |_ |/ _ \ '_ \| '_ \ / _ \ __| __/ _  |
      | |__| |  __/ |_) | |_) |  __/ |_| || (_) |
       \_____|\___| .__/| .__/ \___|\__|\__\___/
                  | |   | |
                  |_|   |_|  The Cloud Maestro
    """

    license = """THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE."""
    license = '%s\n%s\n%s' % ('*' * 70, textwrap.fill(license, 70), '*' * 70,)

    usage = """ """

    print(title)
    print(license)
    print(usage)


def main():
    args = parse_args()

    # Import the test file.
    try:
        test_file = args.test_file
        test_file = test_file[:-3].replace('/', '.')
        mod = __import__(test_file, fromlist=['TestRun'])
        TestRun = getattr(mod, 'TestRun')
    except:
        report('Unable to load TestRun() from file: %s' % args.test_file, 'critical', no_date=True)
        print(traceback.print_exc())
        sys.exit(1)

    # Import the config file.
    try:
        config_file = args.config
        config_file = config_file[:-3].replace('/', '.')
        mod = __import__(config_file, fromlist=['CONFIG_DICT'])
        config_dict = getattr(mod, 'CONFIG_DICT')
    except:
        report("Unable to import the config file: %s" % args.config, 'critical', no_date=True)
        print(traceback.print_exc())
        sys.exit(1)

    do_welcome()

    class GeppettoExecutableTest(TestRun):
        def __init__(self):
            Geppetto.__init__(self)
            TestRun.set_init_params(self, config_dict, args, test_file)

        @capture_exception_and_abort
        def run(self):
            TestRun.run(self)

    g = GeppettoExecutableTest()
    g.run()


if __name__ == '__main__':
    main()
