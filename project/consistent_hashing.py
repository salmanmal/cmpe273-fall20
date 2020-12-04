import hashlib
import bisect


def my_hash(key):
  '''my_hash(key) returns a hash in the range [0,1).'''
  return (int(hashlib.md5(key).hexdigest(),16) % 1000000)/1000000.0

class ConsistentHashing:
    def __init__(self,servers=None):
        print("Consistent Hashing Initialised")
        self.splits=40
        self.servers=servers
        self.distribution=[]
        self.create_ring()
        
        
    def create_ring(self,):
        num_servers=len(self.servers)
        hash_tuples = [(j,k,my_hash((self.servers[j]+"_"+str(k)).encode('utf-8'))) \
                        for j in range(num_servers) \
                        for k in range(self.splits)]
        
        self.sorted_hash_tuples=sorted(hash_tuples, key = lambda x: x[2])
        self.hash_values = list(map(lambda x: x[2], self.sorted_hash_tuples))
        if len(self.distribution)==0 :
            self.distribution=dict.fromkeys(self.servers,0)
        else:
            new_distribution={}
            for server in self.servers:
                if server in self.distribution:
                    new_distribution[server]=self.distribution[server]
                else:
                    new_distribution[server]=0
                    
            self.distribution=new_distribution

    def remove_server_by_name(self, server_to_remove):
        self.servers.remove(server_to_remove)
        self.create_ring()

    def add_server(self, server_to_add):
        # adds the server to the ring and returns the servers list to reiterate
        self.servers.append(server_to_add)
        self.create_ring()
        index=len(self.servers)-1

        # Server data to be redistributed
        lst=[]
        for i in range(len(self.sorted_hash_tuples)):
            if self.sorted_hash_tuples[i][0]==index:
                next_server=0
                if i < len(self.sorted_hash_tuples)-1:
                    next_server=self.sorted_hash_tuples[i+1][0]
                else:
                    next_server=self.sorted_hash_tuples[0][0]
                
                if next_server!=index:
                    lst.append(self.servers[next_server])
                    self.distribution[self.servers[next_server]]=0
        return set(lst)
        


    def update_servers(self, servers):
        self.servers=servers
        self.create_ring()

    def getDistribution(self):
        return self.distribution

    def select_node_for_put(self,key):
        node_index=self.select_node(key)
        self.distribution[self.servers[node_index]]+=1
        return node_index
        
    def select_node(self,key):
        h=my_hash(key.encode('utf-8'))
        index=bisect.bisect_left(self.hash_values,h)
        index=index%len(self.hash_values)
        return self.sorted_hash_tuples[index][0]