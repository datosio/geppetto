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


from common import report, get_hostname_ip, shell, rpc


class NetworkTrafficControl(object):
    def __init__(self, ips=None, username=None, password=None, key=None):
        self.ips = list(ips)
        self.username = username
        self.password = password
        self.key = key
        self.host = get_hostname_ip()

        if not self.ips:
            self.cli = shell
        elif len(self.ips) == 1 and (self.host in self.ips or '127.0.0.1' in self.ips):
            self.cli = shell
        else:
            self.cli = self._rpc_mask

    def _rpc_mask(self, command, ip=None, suppress_output=False, no_tty=False, timeout=60 * 20):
        if ip:
            rpc(ip, command, self.username, self.password, self.key, timeout, no_tty=no_tty, suppress_output=suppress_output)
        else:
            for ip in self.ips:
                rpc(ip, command, self.username, self.password, self.key, timeout, no_tty=no_tty, suppress_output=suppress_output)

    def slow(self, delay=200, ip=None):
        self.cli('tc qdisc add dev eth0 root netem delay %sms' % delay, ip=ip)

    def status(self, ip=None):
        self.cli('tc -s qdisc ls dev eth0', ip=ip)

    def reset(self, ip=None):
        self.cli('sudo tc qdisc del dev eth0 root', ip=ip)
        self.cli('tc -s qdisc ls dev eth0', ip=ip)
