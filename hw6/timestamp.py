import time



class Timestamp:
    def __init__(self):
        self.__table = dict()

    
    def stamp(self, name):
        self.__table.setdefault(name, [])
        self.__table[name].append(time.time())
        
        if len(self.__table[name]) > 2:
            raise Warning('There is one stamp name `{}` with unexpected stamp number > 2 in Timestamp.')
        

    def get_diff(self, name):
        if len(self.__table[name]) < 2:
            return None
        else:
            return self.__table[name][-1] - self.__table[name][0]