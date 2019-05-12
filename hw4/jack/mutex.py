from kazoo.client import KazooClient
import time
import random
import os



PARENT_PATH = '/mutex'
NODE_PATH = 'task-'



class Task:
    def __init__(self, zk, parent_path, node_path):
        self.zk = zk
        self.parent_path = parent_path
        self.node_path = node_path
        self.name = None
        self.done = False
    

    def ensure(self):
        self.zk.ensure_path(self.parent_path)

    
    def task(self, event=None):
        print('task {} starts'.format(self.name))
        t_s = time.time()
        time.sleep(random.random() * 5)
        print('task {} ends'.format(self.name))
        t_e = time.time()
        
        self.done = True
        print('task {} consumes {:.2f} s'.format(self.name, t_e - t_s))


    def run(self):
        self.ensure()
        new_node_path = self.zk.create(os.path.join(self.parent_path, self.node_path), sequence=True)
        name = new_node_path.split('/')[-1]
        self.name = name
        nodes = sorted(self.zk.get_children(self.parent_path))
        min_node = nodes[0]
        if min_node != name:
            print('task {} is awaiting...'.format(name))
            self.t_0 = time.time()
            precedent_node_path = os.path.join(self.parent_path, nodes[nodes.index(name) - 1])
            self.zk.get(precedent_node_path, watch=self.task)
        else:
            self.task()
        while not self.done:
            pass

        self.zk.delete(new_node_path)
    
        

if __name__ == '__main__':
    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()
    
    for i in range(5):
        task = Task(zk, PARENT_PATH, NODE_PATH)
        task.run()

    zk.stop()