# Dig Data Management Assignment 4
<div align = right>信科四 郑元嘉 1800920541</div>



## Implement Mutex based on Zookeeper and Python
In this task, we're about to implement a Mutex (Mutual Exclusive) based on Zookeeper APIs on Python.
Here shows the steps.



### 1. Installation
Install Zookeeper by following the tutorial provided by TA.
 
 
 
<br><br>
### 2. Start the zookeeper
```console
# /usr/local/zookeeper/zookeeper-3.4.14
$ ./bin/zkServer.sh [start-foreground/start]
```



<br><br>
### 3. Python Zookeeper API library - `kazoo`
We use library `kazoo` on Python to manipulate zookeeper.
Some essentail segments would be expanded on below. (Complete source code is append at *5. Complete Source Code*)
* Connect to zookeeper service and end it in the end.
  ```python
  zk = KazooClient(hosts='127.0.0.1:2181')
  zk.start()
  zk.stop()
  ```
    Note that the port of zookeeper service is defined in `(/usr/local/zookeeper/zookeeper-3.4.14/)conf/zoo.cfg` file with a default of 2181.
* Ensure the node path before creating a node
  ```python
  def ensure(self):
    self.zk.ensure_path(self.parent_path)
  ```
  It's easy to ignore this step and we'll find we're not allowed to create a node with its parent path not existing.
* Create a sequence node to indicate a key
  ```python
  new_node_path = self.zk.create(os.path.join(self.parent_path, self.node_path), sequence=True)
  name = new_node_path.split('/')[-1]
  self.name = name
  ```
* Ask of the minimal sequence number
  ```python
  nodes = sorted(self.zk.get_children(self.parent_path))
  min_node = nodes[0]
  if min_node != name:
      print('task {} is awaiting...'.format(name))
      self.t_0 = time.time()
      min_node_path = os.path.join(self.parent_path, min_node)
      self.zk.get(min_node_path, watch=self.task)
  else:
      self.task()
  while not self.done:
      pass
  ```
  If it's the one with minimal sequence number, then start its task right away. If not, set a watch to the precedent node, then set a watch to the precedent node and the task will be kicked off when the watch sends the event.
  Here's a trick, because the watch doesn't hold the program, we must apply while loop (or any similiar other) to wait for that the watch triggers the task and task is well done.
* Delete the node to delete the mutext
  ```python
  self.zk.delete(new_node_path)
  ```
  Then, the metux of the task is totally gone through.



<br><br>
### 4. Experiments and Results
To simulate the multiple programs share the metux to run their own tasks, we run two processes at a time in the experiment. Plus we use `time.sleep()` and `random.random()` to mock the running time of tasks.
Here are the results:
*Program 1*
![c214de23.png](:storage/d77d9c28-7304-4fa5-80c9-0697aff5b8dd/c214de23.png)
*Program 2*
![e4787a9a.png](:storage/d77d9c28-7304-4fa5-80c9-0697aff5b8dd/e4787a9a.png)
As seen, the two programs actually await when the shared mutex is occupied.



<br><br>
### 5. Complete Source Code
```python
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
```

