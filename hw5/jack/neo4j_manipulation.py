import pandas as pd
from argparse import ArgumentParser
from neo4j import GraphDatabase
from neo4j import Node
from timestamp import Timestamp



def insert_bulk(file_path, uri, acc, pwd):
    driver = GraphDatabase.driver(uri, auth=(acc, pwd))

    data = pd.read_csv(file_path, delimiter=';')
    print('Load data {} with size of {}.'.format(file_path, data.shape[0]))
    
    global timestamp
    timestamp.stamp('insert')

    statement = 'CREATE (:student{{{}}})'.format(','.join(['{v}: {{{v}}}'.format(v=v) for v in data.columns.values]))

    with driver.session() as session:
        for i, row in data.iterrows():
            session.run(statement, dict(row))
            
    timestamp.stamp('insert')


def update_bulk(file_path, uri, acc, pwd):
    driver = GraphDatabase.driver(uri, auth=(acc, pwd))

    global timestamp
    timestamp.stamp('update')

    new_job, new_sex = 'teacher', 'F'
    statement = 'MATCH (s:student{}) SET s.Mjob = {Mjob} SET s.sex = {sex} RETURN s'

    with driver.session() as session:
        res = session.run(statement, Mjob=new_job, sex=new_sex)
        data = list(res.records())

    timestamp.stamp('update')


def search_bulk(file_path, uri, acc, pwd):
    driver = GraphDatabase.driver(uri, auth=(acc, pwd))

    global timestamp
    timestamp.stamp('search')
    
    sex_filter, paid_filter, internet_filter = 'F', 'no', 'yes'
    statement = 'MATCH (s:student{sex: {sex}, paid: {paid}, internet: {internet}}) RETURN s'

    with driver.session() as session:
        res = session.run(statement, sex=sex_filter, paid=paid_filter, internet=internet_filter)
        data = list(res.records())
        
        print('Search task returns {} matched items.'.format(len(data)))

    timestamp.stamp('search')



if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-m', action='store', type=str, required=True, help='mode of manipulation')
    parser = parser.parse_args()

    CSV_FILE_PATH = './student.csv'
    NEO4J_URI, NEO4J_ACC, NEO4J_PWD = 'bolt://localhost:7687', 'neo4j', 'test'
    
    timestamp = Timestamp()


    if parser.m == 'insert':
        timestamp.stamp('all')
        
        insert_bulk(CSV_FILE_PATH, NEO4J_URI, NEO4J_ACC, NEO4J_PWD)
        
        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Insertion time: {}'.format(timestamp.get_diff('insert')))

    elif parser.m == 'update':
        timestamp.stamp('all')
        
        update_bulk(CSV_FILE_PATH, NEO4J_URI, NEO4J_ACC, NEO4J_PWD)
        
        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Update time: {}'.format(timestamp.get_diff('update')))

        if parser.m == 'update':
            timestamp.stamp('all')
        
        update_bulk(CSV_FILE_PATH, NEO4J_URI, NEO4J_ACC, NEO4J_PWD)
        
        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Update time: {}'.format(timestamp.get_diff('update')))z