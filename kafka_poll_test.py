from influxdb import InfluxDBClient
import json
from queue import Queue, Full, Empty
from kafka import KafkaConsumer, TopicPartition
import datetime
from threading import Thread
import logging
import time
import copy
from functools import wraps
from time import monotonic as now

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

queue = Queue(maxsize=30000)
bootstrap_servers = ['kafka-service:9092']
client = InfluxDBClient('influxdb-service', 8086)


def took(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = now()
        r = func(*args, **kwargs)
        ms = (now() - start) * 1000
        print('%s took %.2f ms' % (func.__name__, ms))
        return r

    return wrapper


def recv(bootstrap_servers, topic):
    while True:
        consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id='kafka-test1',
                                 auto_offset_reset='earliest', enable_auto_commit=False)
        tp = TopicPartition(topic=topic, partition=0)
        #            consumer.assign([TopicPartition(topic=topic, partition=0)])
        #            consumer.seek(TopicPartition(topic=topic, partition=0), 0)
        print("start listening")
        while True:
            try:
                count = 0
                start = int(time.time() * 1000)
                for i in range(20):
                    p = consumer.poll(timeout_ms=10000, update_offsets=False)
                    msgs = p.get(tp)
                    if not msgs:
                        continue
                    lst = []
                    for msg in msgs:
                        lst.append(msg.value)
                        offset = msg.offset
                    b_msgs = bolt_twin_dmc(lst)
                    sink_twin_dmc(b_msgs)
                    sink(b_msgs)
                    consumer.commit()
                    count += len(lst)
                t = int(time.time() * 1000) - start
                print("插入{}数据用了{}ms, 插入速度为{}, 此时的offset={}".format(count, t, count / t * 1000, offset))
            except Exception as e:
                logging.error(e)
                continue


def parse_time(t):
    try:
        return datetime.datetime.fromtimestamp(t / 1000)
    except Exception:
        return datetime.datetime.now()


def bolt_twin_dmc(msgs):
    if not msgs:
        return []
    lst = []
    for msg in msgs:
        try:
            if isinstance(msg, (bytes, str)):
                msg = json.loads(msg)
            if msg.get('action') == 'DEVICE_DATA':
                dt = parse_time(msg.get('time'))
                action = msg.get('action')
                msgId = msg.get('msgId')
                payload = msg.get('data')
                if isinstance(payload, (str, bytes)):
                    payload = json.loads(payload)
                operate = payload.get('operate')
                operateId = payload.get('operateId')
                data = payload.get('data')
                if isinstance(data, (str, bytes)):
                    a = json.loads(data)
                if isinstance(data, dict):
                    a = [data]
                elif isinstance(data, list):
                    a = data
                else:
                    logging.error('invalid data type {}'.format(data))
                    return
                for item in a:
                    if item.get('time'):
                        dt = parse_time(item.get('time'))
                    if operate == 'EVENT_UP':
                        v = {'tags': {'action': action, 'operate': operate, 'pk': item.get('pk'),
                                      'devId': item.get('devId'),
                                      'identifier': item.get('identifier')}}
                        if item.get('params'):
                            params = json.dumps(item.get('params'))
                        else:
                            params = None
                        v['fields'] = {'value': params, 'operateId': operateId, 'msgId': msgId}
                        v['time'] = dt
                        lst.append(v)
                    if operate == 'DEV_STAT':
                        v = {}
                        params = item.get('params')
                        if not params:
                            continue
                        if isinstance(params, (str, bytes)):
                            params = json.loads(params)
                        online = params.get('online')
                        active = params.get('active')
                        ip = params.get('ip')
                        v['tags'] = {'action': action, 'operate': operate, 'pk': item.get('pk'),
                                     'devId': item.get('devId'), 'online': online, 'active': active}
                        v['fields'] = {'ip': ip, 'operateId': operateId, 'msgId': msgId}
                        v['time'] = dt
                        lst.append(v)
                    if payload.get('operate') == 'ATTR_UP':
                        tags = {'action': action, 'operate': operate, 'pk': item.get('pk'),
                                'devId': item.get('devId'),
                                'identifier': item.get('identifier')}
                        if not item.get('params'):
                            continue
                        for attr, value in item.get('params').items():
                            try:
                                value = float(value)
                            except Exception:
                                value = str(value)
                            v = {}
                            tags_copy = copy.deepcopy(tags)
                            tags_copy.update({'attributeIdentifier': attr})
                            v['tags'] = tags_copy
                            v['fields'] = {'value': value, 'operateId': operateId, 'msgId': msgId}
                            v['time'] = dt
                            lst.append(v)
            if msg.get('action') == 'DEV_CONTROL_RES':
                dt = parse_time(msg.get('time'))
                action = msg.get('action')
                msgId = msg.get('msgId')
                payload = msg.get('data')
                hour = int(dt.strftime('%Y%m%d%H'))
                if not isinstance(payload, (str, bytes)):
                    payload = str(payload)
                v = {'tags': {'action': action, 'hour': hour}, 'fields': {'value': payload, 'msgId': msgId}, 'time': dt}
                lst.append(v)
        except Exception as e:
            logging.warning(e)
            continue
    #    print('bolt {} messages'.format(len(lst)))
    return lst


def sink_twin_dmc(message):
    dbs = client.get_list_database()
    if 'device_test1' not in [x['name'] for x in dbs]:
        print('create database')
        client.create_database('device_test1')
    if not message:
        return []
    if not isinstance(message, list):
        logging.error('invalid message type')
        return
    #    print('sink {} messages'.format(len(message)))
    points = []
    for row in message:
        error = check_row_format(row)
        if error:
            logging.error('invalid row format {}: {}'.format(error, row))
            continue
        measurement = check_measurement(row.get('tags').get('operate'), row.get('fields').get('value'))
        if measurement:
            row['measurement'] = measurement
        points.append(row)
    if points:
        #        print('write {} points'.format(len(points)))
        client.write_points(points, database='device_test1')


def sink(message):
    dbs = client.get_list_database()
    if 'device1' not in [x['name'] for x in dbs]:
        print('create database')
        client.create_database('device1')
    if not message:
        return []
    if not isinstance(message, list):
        logging.error('invalid message type')
        return
    #    print('sink {} messages'.format(len(message)))
    points = []
    for row in message:
        try:
            error = check_row_format(row)
            if error:
                print('invalid row format {}: {}'.format(error, row))
                continue
            measurement = 'device_value'
            if measurement:
                row['measurement'] = measurement
            tags = row.get('tags')
            if tags.get('operate') == 'ATTR_UP':
                hour = int(row.get('time').strftime("%Y%m%d%H"))
                row['tags'] = {'productKey': tags.get('pk'),
                               'deviceIdentifier': tags.get('devId'),
                               'attributeIdentifier': tags.get('attributeIdentifier'), 'projectIdentifier': 1,
                               'hour': hour}
                row['fields'] = {'value': row.get('fields').get('value')}
                row['time'] = row.get('time').replace(microsecond=0)
                points.append(row)
        except Exception as e:
            logging.error(e)
            continue
    if points:
        #        print('write {} points'.format(len(points)))
        client.write_points(points, database='device1')


def check_measurement(operate, value):
    if not operate:
        return 'dev_control_res'
    if operate == 'DEV_STAT':
        return 'dev_stat'
    elif operate == 'EVENT_UP':
        return 'event_up'
    elif operate == 'SERVICE_DOWN_RES' or operate == 'ATTR_WRITE_RES':
        return 'service_attr_res'
    elif operate == 'DEV_STAT_RES' or operate == 'EVENT_UP_RES' or operate == 'ATTR_UP_RES':
        return 'event_attr_dev_res'
    elif operate == 'SERVICE_DOWN' or operate == 'ATTR_WRITE':
        return 'service_attr'
    elif operate == 'ATTR_UP':
        if isinstance(value, (int, float)):
            return 'attr_up_num'
        elif isinstance(value, str):
            return 'attr_up_str'
        else:
            return
    else:
        return


def check_row_format(row):
    try:
        assert 'time' in row
        assert 'fields' in row
    except Exception as e:
        return str(e)


recv(bootstrap_servers, 'twin_dmc')

