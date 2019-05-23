import time
import redis
import pandas as pd
from timestamp import Timestamp
from argparse import ArgumentParser



def insert_bulk(file_path, redis_host, redis_port, redis_db_index, task_index):
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db_index)

    if task_index == 0:
        with open(file_path) as f:
            for line in f.readlines():
                data = line.strip().split(' ')
                if len(data) != 3:
                    print('Datum error:', data)
                client.rpush(data[0], ' '.join([data[1], data[2]]))

    elif task_index == 1:
        with open(file_path) as f:
            for line in f.readlines():
                data = line.strip().split(' ')
                if len(data) != 3:
                    print('Datum error:', data)
                client.rpush(data[2], ' '.join([data[0], data[1]]))

    elif task_index == 2:
        with open(file_path) as f:
            for line in f.readlines():
                data = line.strip().split(' ')
                if len(data) != 3:
                    print('Datum error:', data)
                client.rpush(data[1], data[0])

    elif task_index == 3:
        with open(file_path) as f:
            for line in f.readlines():
                data = line.strip().split(' ')
                if len(data) != 3:
                    print('Datum error:', data)
                client.zincrby(data[2], 1, data[0])


def search_0(redis_host, redis_port, redis_db_index):
    global timestamp
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db_index, decode_responses=True)

    timestamp.stamp('search')

    si_list = ['Shamsuzzaman_Khan', 'Jake_Gyllenhaal', 'Rubn_Arocha', 'frBgnet', 'Cody_Kessler']

    for si in si_list:
        res = client.lrange(si, 0, -1)
        pair_list = [set(r.split(' ')) for r in res]
        print('Search: {} | Number of results: {} | {}'.format(si, len(res), pair_list))

    timestamp.stamp('search')


def search_1(redis_host, redis_port, redis_db_index):
    global timestamp
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db_index, decode_responses=True)

    timestamp.stamp('search')

    oi_list = ['France', 'Belgium', 'Canada', 'Scotland', 'Spain']

    for oi in oi_list:
        res = client.lrange(oi, 0, -1)
        pair_list = [set(r.split(' ')) for r in res]
        print('Search: {} | Number of results: {}'.format(oi, len(res)))

    timestamp.stamp('search')


def search_2(redis_host, redis_port, redis_db_index):
    global timestamp
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db_index, decode_responses=True)

    timestamp.stamp('search')

    p_pair = ('isLeaderOf', 'isCitizenOf')

    res_0 = client.lrange(p_pair[0], 0, -1)
    res_1 = client.lrange(p_pair[1], 0, -1)

    res_cross = set(res_0) & set(res_1)

    print('Search: {} | Number of results: {} \n| Partial Samples: {}'.format(p_pair, len(res_cross), list(res_cross)[:5]))

    timestamp.stamp('search')


def search_3(redis_host, redis_port, redis_db_index):
    global timestamp
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db_index, decode_responses=True)

    timestamp.stamp('search')

    oi_list = ['DurhamSud', 'Norway', 'Germany', 'France', 'China']
    
    for oi in oi_list:
        res = client.zrevrange(oi, 0, 0)

        print('Search: {} | Result: {}'.format(oi, res))

    timestamp.stamp('search')



if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-m', action='store', type=str, required=True, help='mode [insert, task]')
    parser.add_argument('-i', action='store', type=int, required=True, help='index of task')
    parser = parser.parse_args()

    FILE_PATH = './yagoThreeSimplifiedShort.txt'
    REDIS_HOST, REDIS_PORT, REDIS_DB_INDEX = '127.0.0.1', 6379, parser.i
    
    timestamp = Timestamp()


    if parser.m == 'insert':
        timestamp.stamp('all')
        
        insert_bulk(FILE_PATH, REDIS_HOST, REDIS_PORT, REDIS_DB_INDEX, parser.i)
        
        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
    

    elif parser.m == 'search':
        timestamp.stamp('all')
        
        if parser.i == 0:
            search_0(REDIS_HOST, REDIS_PORT, REDIS_DB_INDEX)

        elif parser.i == 1:
            search_1(REDIS_HOST, REDIS_PORT, REDIS_DB_INDEX)

        elif parser.i == 2:
            search_2(REDIS_HOST, REDIS_PORT, REDIS_DB_INDEX)

        elif parser.i == 3:
            search_3(REDIS_HOST, REDIS_PORT, REDIS_DB_INDEX)
        
        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Search time: {}'.format(timestamp.get_diff('search')))