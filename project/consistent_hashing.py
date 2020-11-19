import hashlib
import bisect


def my_hash(key):
  '''my_hash(key) returns a hash in the range [0,1).'''
  return (int(hashlib.md5(key).hexdigest(),16) % 1000000)/1000000.0

class ConsistentHashing:
    def __init__(self,servers=None):
        self.num_servers=len(servers)
        print(self.num_servers)
            
        hash_tuples = [(j,k,my_hash((servers[j]+"_"+str(k)).encode('utf-8'))) \
                        for j in range(self.num_servers) \
                        for k in range(1)]
        
        print(hash_tuples)
        map={}
        hashed_id=[]
        for t in hash_tuples:
            map[t[2]]=t[0]
            hashed_id.append(t[2])

        hashed_id.sort()
        
        self.hash_tuples=hash_tuples
        self.hashed_id=hashed_id
        self.map=map
    

    def select_node(self,key):
        print(key)
        h=my_hash(key.encode('utf-8'))
        
        hash_values=list(map(lambda x: x[2],self.hash_tuples))
        index=bisect.bisect(self.hashed_id,h)
        index=index%self.num_servers
        print("index")
        
        print(index)
        index=self.map[self.hashed_id[index]]
        return self.hash_tuples[index][0]