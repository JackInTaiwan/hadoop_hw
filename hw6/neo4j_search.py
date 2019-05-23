from neo4j import GraphDatabase, Node
from argparse import ArgumentParser
from collections import Counter
from timestamp import Timestamp



def insert_bulk(file_path, uri, acc, pwd, task_index):
    driver = GraphDatabase.driver(uri, auth=(acc, pwd))

    timestamp.stamp('insert')

    # if True:
    #     statement = 'UNWIND {} AS s_ UNWIND {} AS p_ UNWIND {} AS o_ CREATE (:test{{s: s_, p: p_, o: o_}})'
    #     s_list, p_list, o_list = [], [], []
    #     with open(file_path) as f:
    #         for i, line in enumerate(f.readlines()):
    #             doc = line.strip().split(' ')
    #             s_list.append(doc[0])
    #             p_list.append(doc[1])
    #             o_list.append(doc[2])
    #             if i > 1000: break
                
    #     with driver.session() as session:
    #         print('Start creating...')
    #         script = statement.format(s_list, p_list, o_list)
    #         session.run(script)

    if task_index == 0:
        statement = 'CREATE (:set1{s: {s}, p: {p}, o: {o}})'
        with driver.session() as session:
            with open(file_path) as f:
                for i, line in enumerate(f.readlines()):
                    if i % 100 == 0: print('{} rounds'.format(i))
                    doc = line.strip().split(' ')
                    session.run(statement, s=doc[0], p=doc[1], o=doc[2])


def search_0(file_path, uri, acc, pwd, task_index):
    driver = GraphDatabase.driver(uri, auth=(acc, pwd))

    global timestamp
    timestamp.stamp('search')
    
    si_list = ['Shamsuzzaman_Khan', 'Spencer_Chandra_Herbert', 'Minnesota_Public_Radio', 'OJ_da_Juiceman', 'Colt_Lyerla']
    statement = 'MATCH (set:set1{s: {s}}) RETURN set'

    for si in si_list:
        with driver.session() as session:
            res = session.run(statement, s=si)
            data = list(res.records())
            print('Search task returns {} matched items.'.format(len(data)))

    timestamp.stamp('search')


def search_1(file_path, uri, acc, pwd, task_index):
    driver = GraphDatabase.driver(uri, auth=(acc, pwd))

    global timestamp
    timestamp.stamp('search')
    
    oi_list = oi_list = ['France', 'Belgium', 'Canada', 'Scotland', 'Spain']
    statement = 'MATCH (set:set1{o: {o}}) RETURN set'

    for oi in oi_list:
        with driver.session() as session:
            res = session.run(statement, o=oi)
            data = list(res.records())
            print('Search task returns {} matched items.'.format(len(data)))

    timestamp.stamp('search')
    

def search_2(file_path, uri, acc, pwd, task_index):
    driver = GraphDatabase.driver(uri, auth=(acc, pwd))

    global timestamp
    timestamp.stamp('search')
    
    p_pair = ('isLeaderOf', 'isCitizenOf')
    statement = 'MATCH (set:set1{p: {p}}) RETURN set'

    with driver.session() as session:
        res_0 = session.run(statement, p=p_pair[0])
        res_1 = session.run(statement, p=p_pair[1])
        data_0 = set([item['set']['s'] for item in list(res_0.records())])
        data_1 = set([item['set']['s'] for item in list(res_1.records())])
        print('Search task returns {} matched items.'.format(len(data_0 & data_1)))

    timestamp.stamp('search')


def search_3(file_path, uri, acc, pwd, task_index):
    driver = GraphDatabase.driver(uri, auth=(acc, pwd))

    global timestamp
    timestamp.stamp('search')
    
    oi_list = ['DurhamSud', 'Norway', 'Germany', 'France', 'China']
    statement = 'MATCH (set:set1{o: {o}}) RETURN set'

    for oi in oi_list:
        with driver.session() as session:
            res = session.run(statement, o=oi)
            data = [item['set']['s'] for item in list(res.records())]
            counter = Counter(data)
            max_ele = sorted(counter.items(), key=lambda x: x[1], reverse=True)[0]
            print('Search: {} | Result: {}'.format(oi, max_ele))

    timestamp.stamp('search')


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-m', action='store', type=str, required=True, help='mode [insert, task]')
    parser.add_argument('-i', action='store', type=int, required=True, help='index of task')
    parser = parser.parse_args()

    FILE_PATH = './yagoThreeSimplifiedShort.txt'
    NEO4J_URI, NEO4J_ACC, NEO4J_PWD = 'bolt://localhost:7687', 'neo4j', 'test'
    
    timestamp = Timestamp()


    if parser.m == 'insert':
        timestamp.stamp('all')
        
        insert_bulk(FILE_PATH, NEO4J_URI, NEO4J_ACC, NEO4J_PWD, parser.i)
        
        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
    

    elif parser.m == 'search':
        timestamp.stamp('all')
        
        if parser.i == 0:
            search_0(FILE_PATH, NEO4J_URI, NEO4J_ACC, NEO4J_PWD, parser.i)

        if parser.i == 1:
            search_1(FILE_PATH, NEO4J_URI, NEO4J_ACC, NEO4J_PWD, parser.i)
        
        if parser.i == 2:
            search_2(FILE_PATH, NEO4J_URI, NEO4J_ACC, NEO4J_PWD, parser.i)

        if parser.i == 3:
            search_3(FILE_PATH, NEO4J_URI, NEO4J_ACC, NEO4J_PWD, parser.i)

        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Search time: {}'.format(timestamp.get_diff('search')))