#!/usr/bin/env bash

#    The MIT License (MIT)
#    Copyright (c) Datos IO, Inc. 2015.
#
#    Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
#    documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
#    rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
#    persons to whom the Software is furnished to do so, subject to the following conditions:
#
#    The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
#    Software.
#
#    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
#    WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
#    COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
#    OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


#
# Geppetto bootstrap script for CentOS
#

sudo yum install -y sshpass
sudo pip install pymongo==3.2
sudo pip install boto==2.4
sudo pip install paramiko
sudo pip install cassandra-driver
sudo pip install awscli
sudo pip install IPy
sudo pip install wget
sudo pip install scp

# For future graphing
# sudo yum install -y python-numpy python-matplotlib
# sudo pip install seaborn