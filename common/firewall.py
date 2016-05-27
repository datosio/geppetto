"""
Copyright (c) Datos IO, Inc. 2015. All rights reserved. All information contained herein is, and remains the property
of Datos IO, Inc. The intellectual and technical concepts contained herein are proprietary to Datos IO, Inc. and may
be covered by U.S. and Foreign Patents, patents in process, and are protected by trade secret or copyright law.
Dissemination of this information or reproduction of this material is strictly forbidden unless prior written
permission is obtained from Datos IO, Inc.
"""


from common import get_hostname_ip, shell, rpc


class Firewall(object):
    def __init__(self, ips=None, username=None, password=None, key=None):
        self.ips = ips
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

    def _rpc_mask(self, command, suppress_output=False, no_tty=False, timeout=60 * 20):
        for ip in self.ips:
            rpc(ip, command, self.username, self.password, self.key, timeout, no_tty=no_tty, suppress_output=suppress_output)

    def activate(self):
        self.cli('sudo service iptables start')

    def deactivate(self):
        self.cli('sudo service iptables stop')

    def port_allow(self, port, connection_type='tcp'):
        assert connection_type in ['tcp', 'udp']
        self.cli('iptables -A INPUT -p %s --dport %s -j ACCEPT' % (connection_type, port))

    def port_block(self, port, connection_type='tcp'):
        assert connection_type in ['tcp', 'udp']
        self.cli('iptables -A INPUT -p %s --dport %s -j DROP' % (connection_type, port))

    def clear(self):
        self.cli('sudo iptables --flush')

    def save(self):
        self.cli('sudo service iptables save')

    def status(self):
        out, _ = self.cli('sudo service iptables status')
        return out
