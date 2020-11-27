import hashlib
import bisect


def my_hash(key):
  '''my_hash(key) returns a hash in the range [0,1).'''
  return (int(hashlib.md5(key).hexdigest(),16) % 1000000)/1000000.0

class ConsistentHashing:
    def __init__(self,servers=None):
        self.num_servers=len(servers)
        self.servers=servers
        num_servers=len(self.servers)
        hash_tuples = [(j,k,my_hash((self.servers[j]+"_"+str(k)).encode('utf-8'))) \
                        for j in range(num_servers) \
                        for k in range(self.splits)]
        
        print(hash_tuples)
        self.sorted_hash_tuples=sorted(hash_tuples, key = lambda x: x[2])
        self.hash_values = list(map(lambda x: x[2],self.sorted_hash_tuples))
    

    def select_node(self,key):
        print(key)
        h=my_hash(key.encode('utf-8'))
        
        index=bisect.bisect_left(self.hash_values,h)
        index=index%len(self.hash_values)
        return self.sorted_hash_tuples[index][0]