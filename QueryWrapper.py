class QueryWrapper():
    def __init__(self, path='', name=''):
        self.path = path
        self.name = name
     
    def read_query(self):
        file = open(self.path+'/'+self.name,'r') 
        my_query =''''''
        for row in file:
            my_query+=row
        return my_query