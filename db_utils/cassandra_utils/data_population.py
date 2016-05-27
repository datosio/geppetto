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
import time
import uuid
import random
import argparse
import datetime
import binascii
import itertools
import subprocess


import cassandra.util as CU
import cassandra.cluster as CC
from cassandra.query import BatchType
from cassandra.query import tuple_factory
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider


import common.common
from common.common import report


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('ip_list', action='append', help='Cassandra cluster ip list')
    parser.add_argument('schema_file', action='store', help='Schema file')

    # for driver authentication
    parser.add_argument('--db_user', default='', help='Authentication username')
    parser.add_argument('--db_pass', default='', help='Authentication password')

    subparsers = parser.add_subparsers(dest='command', help='commands')

    # for insert command
    insert_parser = subparsers.add_parser('insert', help='Insert records')
    insert_parser.add_argument('-r', action='store', dest='record_size', default='1000', help='Record size')
    insert_parser.add_argument('-s', action='store', dest='start_record', default='1', help='Starting record number')
    insert_parser.add_argument('-n', action='store', dest='record_count', default='10000', help='Record count')
    insert_parser.add_argument('-t', action='store', dest='target_db', default='ks1.table1', help='keyspace.table format')
    insert_parser.add_argument('-u', action='store', dest='uuid4', default=None, help='uuid4 to use if given.')
    insert_parser.add_argument('--replication', default=3, type=int, help='Keyspace replication factor.')

    # for update command
    update_parser = subparsers.add_parser('update', help='Update records')
    update_parser.add_argument('-r', action='store', dest='record_size', default='1000', help='Record size')
    update_parser.add_argument('-s', action='store', dest='start_record', default='1', help='Starting record number')
    update_parser.add_argument('-t', action='store', dest='target_db', default='ks1.table1', help='keyspace.table format')
    update_parser.add_argument('-i', action='store', dest='insert_percentage', default='70', help='Insert percentage')
    update_parser.add_argument('-b', action='store', dest='batch_size', default='1', help='Batch size')
    update_parser.add_argument('-d', action='store', dest='delay', default='1000', help='Delay time in ms')
    update_parser.add_argument('-c', action='store', dest='batch_count', default='-1', help='Number of batches to execute')
    update_parser.add_argument('--replication', dest='replication', default=3, type=int, help='Keyspace replication factor.')

    # for compare command
    compare_parser = subparsers.add_parser('compare', help='Update records')
    compare_parser.add_argument('first', action='store', default='ks1.table1',
                                help='First table to compare in keyspace.table format')
    compare_parser.add_argument('second', action='store', default='ks1.table101',
                                help='Second table to compare in keyspace.table format')

    compare_parser = subparsers.add_parser('describe', help='get schema information')
    compare_parser.add_argument('dbname', action='store', default='k1.t1',
                                help='Keyspace.Table to retrieve schema')

    return parser.parse_args()


class CassandraTestingCluster():
    def __init__(self, ip_list, db_user='', db_pass=''):
        self.ip_list = ip_list
        self.session = None
        self.datacenter = None
        self.db_user = db_user
        self.db_pass = db_pass
        self.cluster_auth = PlainTextAuthProvider(username=self.db_user, password=self.db_pass)

    def connect(self, ip_list = None):
        if ip_list is not None:
            self.ip_list.append(ip_list)

        try:
            if self.db_user:
                cluster = CC.Cluster(self.ip_list, auth_provider=self.cluster_auth)
            else:
                cluster = CC.Cluster(self.ip_list)
            cluster.protocol_version = 2

            self.session = cluster.connect()
            self.updateIPList()
            return True

        except Exception:
            self.session = None
            return False

    def disconnect(self):
        if self.session is not None:
            self.session.cluster.shutdown()
            self.session.shutdown()
            self.session = None

    def runQuery(self, query, fetch_size = None):
        if self.session is None:
            return None, False

        try:
            if fetch_size is not None:
                statement = SimpleStatement(query, fetch_size=fetch_size)
            else:
                statement = SimpleStatement(query)

            result = self.session.execute(statement)

        except Exception as e:
            return str(e), False

        if result:
            r = []
            for res in result:
                r.append(res)
            result = r

        return result, True

    def updateIPList(self):
        if self.session is None:
            return False

        host_list = self.session.cluster.metadata.all_hosts()
        if len(host_list) == 0:
            return False

        self.ip_list = []
        for host in host_list:
            host_ip = str(host.address)
            self.ip_list.append(host_ip)
            self.datacenter = host.datacenter

        return True


def getRandomValue4CompositeType(dataType, arg1=0, arg2=0):
    dataTypes = dataType.split('/')
    superType = dataTypes[0]

    if superType == 'ListType' or superType == 'SetType' or superType == 'TupleType':
        entryType = dataTypes[1]
        if superType == 'ListType':
            value_string = '['
        elif superType == 'TupleType':
            value_string = '('
        else:
            value_string = '{'
        value_len = 0
        for i in range(arg2):
            v, l = getRandomValue(entryType, arg1)
            if value_len > 0:
                value_string += ','
            value_string += v
            value_len += l
        if superType == 'ListType':
            value_string += ']'
        elif superType == 'TupleType':
            value_string = ')'
        else:
            value_string += '}'

        return value_string, value_len

    if superType == 'MapType':
        keyType = dataTypes[1]
        entryType = dataTypes[2]
        value_string = '{'
        value_len = 0
        for i in range(arg2):
            k, lk = getRandomValue(keyType, arg1)
            v, lv = getRandomValue(entryType, arg1)
            if value_len > 0:
                value_string += ','
            value_string += "%s:%s" % (k, v)
            value_len += lv + lk
        value_string += '}'

        return value_string, value_len


def addSingleQuote(str):
    return '\'%s\'' % str


def getRandomValue(dataType, arg1=0, arg2=0, uuid4=None):
    if isinstance(dataType, UDT):
        return dataType.getRandomValue(arg1, arg2)

    if '/' in dataType:
        return getRandomValue4CompositeType(dataType, arg1, arg2)

    if dataType == 'BooleanType':
        if random.randint(0, 100) > 50:
            b = 'True'
        else:
            b = 'False'
        return b, 1

    if dataType == 'Int32Type' or dataType =='DecimalType':
        return str(random.randint(100000, 999999)), 4

    if dataType == 'LongType':
        return str(random.randint(1000000000, 9999999999)), 8

    if dataType == 'UTF8Type' or dataType == 'AsciiType':
        rstr = ''
        while len(rstr) < arg1:
            rstr += str(uuid.uuid4())
        return addSingleQuote(rstr[:arg1]), arg1

    if dataType == 'UUIDType':
        if uuid4:
            return uuid4, 16
        return str(uuid.uuid4()), 16

    if dataType == 'TimestampType' or dataType == 'DateType':
        ts = datetime.datetime.now()
        ts_str = str(ts).split('.')[0]
        return addSingleQuote(ts_str), 16

    if dataType == 'DoubleType' or dataType == 'FloatType':
        return str(random.random()), 8

    if dataType == 'InetAddressType':
        i1 = random.randint(0, 255)
        i2 = random.randint(0, 255)
        i3 = random.randint(0, 255)
        i4 = random.randint(0, 255)
        return addSingleQuote('%d.%d.%d.%d' % (i1, i2, i3, i4)), 4

    if dataType == 'TimeUUIDType':
        #t = time.time()
        return str(uuid.uuid1()), 16

    if dataType == 'CounterColumnType':
        return str(random.randint(1000000000, 9999999999)), 8

    if dataType == 'BytesType':
        size = random.randint(1, arg1 * 8)
        bits = '%x' % random.getrandbits(size)
        rstr = 'textAsBlob(%s)' % addSingleQuote(bits)
        return rstr, size

    if dataType == 'IntegerType':
        size = random.randint(1, arg1 * 8)
        rstr = '%d' % random.getrandbits(size)
        return rstr, size

    print('ERROR: cannot handle %s' % dataType)
    raise Exception()


def getValueFromRowKey(dataType, rowkey):
    if dataType == 'Int32Type' or dataType =='DecimalType':
        return str(rowkey), 4

    if dataType == 'LongType':
        return str(rowkey), 8

    if dataType == 'UTF8Type' or dataType == 'AsciiType':
        rstr = str(rowkey)
        return addSingleQuote(rstr), len(rstr)

    if dataType == 'TimeUUIDType':
        return str(uuid.uuid1()), 16

    if dataType == 'UUIDType':
        rstr = '%016d' % rowkey
        ruuid = '%s-%s-%s-%s-%s' % (rstr[:8], rstr[8:12], rstr[12:], rstr[:4], rstr[4:])

        return ruuid, 16

    if dataType == 'TimestampType' or dataType == 'DateType':
        ts_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(116233200 + rowkey))
        return addSingleQuote(ts_str), 16

    if dataType == 'DoubleType' or dataType == 'FloatType':
        return str(rowkey), 8

    if dataType == 'CounterColumnType':
        return str(rowkey), 8

    if dataType == 'BooleanType':
        if rowkey % 2 == 1:
            rstr = 'True'
        else:
            rstr = 'False'
        return rstr, 1

    if dataType == 'BytesType':
        rstr = '%#x' % rowkey
        return rstr, len(rstr) - 2

    if dataType == 'IntegerType':
        rstr = str(rowkey)
        return rstr, len(rstr)

    if dataType == 'InetAddressType':
        i = rowkey % 256
        return '%d.%d.%d.%d' % (i, i, i, i), 4

    raise Exception()


def splitParenthesis(s):
    left = s.find('(')
    right = s.rfind(')')
    if left >= right:
        return s, None

    return s[0:left], s[left + 1: right]


class UDT():
    def __init__(self, validator):
        w = validator.split(',')

        self.ks_name = w[0]
        self.name = binascii.unhexlify(w[1])
        self.members = []

        for member in w[2:]:
            member_name, member_type = member.split(':')
            member_name = binascii.unhexlify(member_name)
            member_type = getType(member_type)
            self.members.append((member_name, member_type))

    def __repr__(self):
        e = None
        for member_name, member_type in self.members:
            if e is None:
                e = "{0}({1}:{2}".format(self.name, member_name, member_type)
            else:
                e += ",{0}:{1}".format(member_name, member_type)

        e += ')'
        return e

    def getRandomValue(self, arg1, arg2):
        # {firstname: 'Bill', lastname: 'Gates'}
        total_length = 0
        ret_str = None
        for member_name, member_type in self.members:
            v, l = getRandomValue(member_type, arg1, arg2)
            if ret_str is None:
                ret_str = '{ %s: %s' % (member_name, v)
            else:
                ret_str += ', %s: %s' % (member_name, v)
            total_length += l
        ret_str += '}'

        return ret_str, total_length


def getType(validator):
    parentType = validator
    columntype = None

    while parentType is not None:
        parentType, childType = splitParenthesis(parentType)

        if childType is None and ',' in parentType:
            subtypes = parentType.split(',')
            parentType = None
            for t in subtypes:
                t = t.split('.')[-1]
                if parentType is None:
                    parentType = t
                else:
                    parentType += '/%s' % t
        else:
            parentType = parentType.split('.')[-1]

        if parentType == 'UserType':
            return UDT(childType)

        if parentType != 'ReversedType':
            if columntype is None:
                columntype = parentType
            else:
                columntype += '/%s'% parentType

        parentType = childType

    return columntype


class TestSchema():
    def __init__(self, cluster, keyspace_name, table_name):
        self.cluster = cluster
        self.keyspace_name = keyspace_name
        self.table_name = table_name

        self.columns = []
        self.spaceFiller = None
        self.nr_regular_columns = 0
        self.counter_table = False

    def getSchema(self):

        query = "select column_name, validator, type from system.schema_columns where keyspace_name = '%s' and columnfamily_name = '%s'" % \
                (self.keyspace_name, self.table_name)

        result, success = self.cluster.runQuery(query)

        if not success:
            return success

        self.nr_regular_columns = 0
        for row in result:
            if row.type == 'regular':
                self.nr_regular_columns += 1

            columntype = getType(row.validator)

            if isinstance(columntype, UDT):
                self.columns.append((row.column_name, columntype, row.type))
                continue

            if columntype == 'CounterColumnType':
                self.counter_table = True

            if self.spaceFiller is None and columntype == 'UTF8Type' and row.type == 'regular':
                self.spaceFiller = row.column_name
            else:
                self.columns.append((row.column_name, columntype, row.type))

        if self.spaceFiller is not None:
            self.columns.append((self.spaceFiller, 'UTF8Type', 'regular'))

        return success

    def getRandomTargetColumn(self):
        target_column = random.randrange(1, self.nr_regular_columns + 1)
        cur_column = 0
        for column_name, data_type, column_type in self.columns:
            if column_type == 'regular':
                cur_column += 1
                if cur_column == target_column:
                    return column_name, data_type, column_type
        return (None, None, None)

    def getInsertQuerywithRandomData4CounterTbl(self, rownum):
        column_name, data_type, column_type = self.getRandomTargetColumn()
        query = "%s = %s + 1" % (column_name, column_name)
        whereQuery = self.getWherePart(rownum)
        return "UPDATE %s.%s SET %s WHERE %s;" % (self.keyspace_name, self.table_name, query, whereQuery)

    def getInsertQuerywithRandomData(self, rownum, record_size, uuid4=None):
        if self.counter_table:
            return self.getInsertQuerywithRandomData4CounterTbl(rownum)
        query_1 = "INSERT INTO %s.%s (" % (self.keyspace_name, self.table_name)
        query_2 = ") VALUES ("
        needCommaFlag = False
        for column_name, data_type, column_type in self.columns:
            item_length = 16
            if column_name == self.spaceFiller and record_size > item_length:
                item_length = record_size
            if needCommaFlag:
                query_1 += ','
                query_2 += ','
            query_1 += column_name

            if column_type == 'regular':
                if uuid4:
                    column_value, l = getRandomValue(data_type, item_length, 4, uuid4)
                else:
                    column_value, l = getRandomValue(data_type, item_length, 4)
            else:
                column_value, l = getValueFromRowKey(data_type, rownum)

            query_2 += column_value
            record_size -= l
            needCommaFlag = True

        return query_1 + query_2 + ");"

    def getWherePart(self, rownum):
        query = ''
        needAndFlag = False
        for column_name, data_type, column_type in self.columns:
            if column_type != 'regular':
                column_value, l = getValueFromRowKey(data_type, rownum)
                if needAndFlag:
                    query += ' and '

                query += '%s = %s' % (column_name, column_value)
                needAndFlag = True

        return query

    def getDeleteQuery(self, rownum):
        whereQuery = self.getWherePart(rownum)
        return "DELETE FROM %s.%s WHERE %s;" % (self.keyspace_name, self.table_name, whereQuery)

    def getUpdateQuery(self, rownum):
        column_name, data_type, column_type = self.getRandomTargetColumn()
        column_value, l = getRandomValue(data_type, 16, 16)
        query = "%s = %s" % (column_name, column_value)
        whereQuery = self.getWherePart(rownum)

        return "UPDATE %s.%s SET %s WHERE %s;" % (self.keyspace_name, self.table_name, query, whereQuery)


def createUDT(cluster, keyspace_name, schema):
    for line in schema.split(','):
        s = line.find('frozen')
        if s < 0:
            s = line.find('FROZEN')

        if s < 0:
            continue

        line = line[s:]
        i = line.find('<')
        j = line.find('>')
        if i < 0 or j < 0 or i >= j:
            continue

        udtname = line[i + 1 : j]
        udtfile = '{0}.udt'.format(udtname)
        #print udtfile

        if not os.path.exists(udtfile):
            continue

        with  open(udtfile) as fp:
            udtschema = fp.read()

        cql = "CREATE TYPE IF NOT EXISTS {0}.{1} {2};".format(keyspace_name, udtname, udtschema)

        result, success = cluster.runQuery(cql)
        if success:
            #print 'TYPE {0} has been successfully created'.format(udtname)
            pass


def createKeyspace(cluster, keyspace_name, replication_class='NetworkTopologyStrategy', replication_factor=3):
    cql = 'SELECT * FROM system.schema_keyspaces;'  # Grab keyspaces.
    result, success = cluster.runQuery(cql)
    
    if keyspace_name in str(result):
        #print('Keyspace %s already exists.' % keyspace_name)
        pass
        
    else:
        if replication_class == 'NetworkTopologyStrategy':
            cql = "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '%s' : %s };" % \
                  (keyspace_name, cluster.datacenter, replication_factor)
        else:
            cql = "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : %s};" % \
                  (keyspace_name, replication_factor)
        result, success = cluster.runQuery(cql)
        if success:
            #print 'Keyspace %s has been successfully created.' % keyspace_name
            pass
        else:
            #print 'Error creating keyspace %s.' % keyspace_name
            pass


def createTable(cluster, keyspace_name, table_name, schema_file):
    cql = "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = '%s' ;" % (keyspace_name)
    result, success = cluster.runQuery(cql)

    if table_name in str(result):
        #print('%s.%s already exists.' % (keyspace_name, table_name))
        pass
    else:  
        with open(schema_file) as fp:
            schema = fp.read()

        createUDT(cluster, keyspace_name, schema)

        cql = "CREATE TABLE IF NOT EXISTS %s.%s %s;" % (keyspace_name, table_name, schema.strip())
        result, success = cluster.runQuery(cql)
        if success:
            #print('%s.%s has been successfully created.' % (keyspace_name, table_name))
            pass
        else:
            #print('Error creating %s.%s.' % (keyspace_name, table_name))
            pass


def getKSCFNames(db_name):
    w = db_name.split('.')
    if len(w) != 2:
        #print "ERROR: wrong target database (%s)" % db_name
        return None, None
    return (w[0], w[1])


def do_insert(cluster, target_db, schema_file, record_size, start_record, record_count, uuid4=None, replication_factor=3, suppress_output=False):
    record_size = int(record_size)
    record_num = int(start_record)
    record_count = int(record_count)
    end_record = record_num + record_count
    inserted_record = 0

    #random.seed(0)

    ks_name, cf_name = getKSCFNames(target_db)
    if ks_name == None or cf_name == None:
        return

    createKeyspace(cluster, ks_name, replication_factor=replication_factor)
    createTable(cluster, ks_name, cf_name, schema_file)

    ts = TestSchema(cluster, ks_name, cf_name)
    ts.getSchema()

    if ts.counter_table:
        batch = BatchStatement(batch_type = BatchType.COUNTER)
    else:
        batch = BatchStatement()

    i = 0
    while record_num < end_record:
        if uuid4:
            query = ts.getInsertQuerywithRandomData(record_num, record_size, uuid4)
        else:
            query = ts.getInsertQuerywithRandomData(record_num, record_size)

        if i == 0 and not suppress_output:
            report(query)

        batch.add(query)

        record_num += 1
        inserted_record += 1

        if (inserted_record % 100) == 0 or record_num == end_record:
            msg = '\rInserting %s %8d / %8d (%3d %%)' % (target_db, inserted_record, record_count,
                                                      inserted_record * 100 / record_count)
            #sys.stdout.write(msg + '\n') ; sys.stdout.flush()
            if not suppress_output:
                report(msg)
            try:
                cluster.session.execute(batch)
            except Exception as e:
                print("\n**** Detected Exception ****")
                print(e)
                print('\n')
                p = subprocess.Popen('nodetool status', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell= True)
                out, err = p.communicate()

                if "Failed to connect" in err:  # This node's cassandra is down
                    print("**** Restarting Cassandra ****")
                    os.system('sudo service cassandra start')
                    print('**** This Node crashed, sleeping for 3 minutes to reduce load. ****\n')
                    time.sleep(60*3)
                    print("")
                elif 'DN' in out:
                    print('**** Another Node crashed, sleeping for 3 minutes to reduce load. ****\n')
                    time.sleep(60*3)
                else:
                    print("**** Sleeping for 3 minutes to reduce load. ****\n")
                    time.sleep(60*3)

            if ts.counter_table:
                batch = BatchStatement(batch_type = BatchType.COUNTER)
            else:
                batch = BatchStatement()

        i += 1


def do_update(cluster, target_db, schema_file, record_size, start_record, batch_size, insert_percentage, delay, batch_count, replication_factor=3, suppress_output=False):
    record_size = int(record_size)
    start_record = int(start_record)
    batch_size = int(batch_size)
    insert_percentage = int(insert_percentage)
    delay = float(delay) / 1000
    batch_count = int(batch_count)
    nr_batch = 0

    random.seed(1)

    ks_name, cf_name = getKSCFNames(target_db)
    if ks_name == None or cf_name == None:
        return

    createKeyspace(cluster, ks_name, replication_factor=replication_factor)
    createTable(cluster, ks_name, cf_name, schema_file)

    ts = TestSchema(cluster, ks_name, cf_name)
    ts.getSchema()

    while True:
        if ts.counter_table:
            batch = BatchStatement(batch_type = BatchType.COUNTER)
        else:
            batch = BatchStatement()
        stat_str = ''
        for i in range(batch_size):
            if start_record <= 0 or random.randrange(100) <= insert_percentage:
                # insert case
                record_num = start_record
                query = ts.getInsertQuerywithRandomData(record_num, record_size)
                stat_str += 'I(%d) ' % record_num

            else:
                record_num = random.randrange(0, start_record)
                if random.randrange(100) <= 70:  # 70% update
                    if not ts.counter_table:
                        query = ts.getUpdateQuery(record_num)
                    else:
                        query = ts.getInsertQuerywithRandomData(record_num, 0)

                    stat_str += 'U(%d) ' % record_num
                else:                           # 30% deletion
                    query = ts.getDeleteQuery(record_num)
                    stat_str += 'D(%d) ' % record_num
            if not suppress_output:
                report(stat_str)
            batch.add(query)
            start_record += 1

        #print stat_str
        cluster.session.execute(batch)
        nr_batch += 1
        if nr_batch == batch_count:
            if batch_count >= 0:
                break
        time.sleep(delay)


def get_udt_list(schema, ks_name, cf_name):
    udt_list = []
    header = "CREATE TABLE {0}.{1}".format(ks_name, cf_name)
    start_pos = schema.find(header)
    if start_pos < 0:
        return udt_list
    end_pos = schema[start_pos:].find(';')
    if end_pos < 0:
        return udt_list
    cf_schema = schema[start_pos : start_pos + end_pos + 1]

    for line in cf_schema.splitlines():
        words = line.split()
        if words[1][:6] == 'frozen':
            end_pos = words[1].find('>')
            if end_pos < 0:
                continue
            udt_list.append(words[1][7:end_pos])
    return udt_list


def filter_schema(schema, cf_name, udt_list):
    mode = 0
    filtered_schema = ''
    for line in schema.splitlines():
        words = line.split()
        if len(words) == 0:
            continue

        # Mode 0: wait CREATE command
        if mode == 0 and words[0] == 'CREATE':
            # e.g., CREATE KEYSPACE ks1 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}  AND durable_writes = true;
            if words[1] == 'KEYSPACE':
                mode = 1
            # e.g., CREATE TYPE ks1.fullname (
            elif words[1] == 'TYPE':
                udtname = words[2].split('.')[-1]
                mode = 1 if udtname in udt_list else 2
            # e.g., CREATE TABLE ks1.xy_db (
            elif words[1] == 'TABLE':
                cfname = words[2].split('.')[-1]
                mode = 1 if cfname == cf_name else 2
            # e.g., CREATE INDEX xy_db_evt_payld_idx ON ks1.xy_db (evt_payld);
            elif words[1] == 'INDEX':
                cfname = words[4].split('.')[-1]
                mode = 1 if cfname == cf_name else 2

        # Mode 1: include until ';' is found
        if mode == 1:
            filtered_schema += line + '\n'

        if line.rfind(';') > 0:
            if mode != 2:
                filtered_schema += '\n'
            mode = 0

    return filtered_schema


def main():
    args = parse_args()

    if not os.path.exists(args.schema_file):
        sys.exit(-1)

    if args.db_user:
        cluster = CassandraTestingCluster(args.ip_list, db_user=args.db_user, db_pass=args.db_pass)
    else:
        cluster = CassandraTestingCluster(args.ip_list)

    if not cluster.connect():
        report('Cannot connect to cassandra cluster.', 'error')
        sys.exit(-1)

    try:
        if args.command == 'insert':
            do_insert(cluster, args.target_db, args.schema_file, args.record_size, args.start_record, args.record_count, args.uuid4, replication_factor=args.replication)
        elif args.command == 'update':
            do_update(cluster, args.target_db, args.schema_file, args.record_size, args.start_record, args.batch_size, args.insert_percentage, args.delay, args.batch_count, replication_factor=args.replication)
        else:
            report('Unrecognized command.\n')

    except Exception as e:
        report('%s\n' % e)
        sys.exit()

    cluster.disconnect()


if __name__ == '__main__':
    main()
