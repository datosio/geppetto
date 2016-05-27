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


class DatabaseCluster(object):
    """
    Class prototype for db classes.
    """
    def __init__(self):
        self.name = '_name_'
        self.ips = ['',]

    def query(self, sql):
        assert False, 'Implement database.query'

    def remove(self, ks, table=None):
        assert False, 'Implement database.remove'

    def status(self):
        assert False, 'Implement database.status'

    def db_stop(self, ip):
        assert False, 'Implement database.db_stop'

    def db_start(self, ip):
        assert False, 'Implement database.db_start'

    def node_reboot(self, ip):
        assert False, 'Override database.node_reboot'

    def node_restore(self, ip):
        assert False, 'Override database.node_restore'

    def shutdown(self):
        assert False, 'Override database.shutdown'

    def install(self, version, as_service=True):
        assert False, 'Override database.install'

    def __str__(self):
        return self.name
