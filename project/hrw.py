import hashlib

def generate_hash(key,server_i):
    m=hashlib.md5()
    m.update(key)
    m.update(server_i)
    return int(m.hexdigest(),16)


class HRW:
    def __init__(self,servers=None):
        print("HRW Initialised")
        self.servers=servers
        self.distribution=[]
        self.reset_distribution()

    def reset_distribution(self,):
        num_servers=len(self.servers)
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
        self.reset_distribution()

    def add_server(self, server_to_add):
        # adds the server to the ring and returns the servers list to reiterate
        self.servers.append(server_to_add)
        self.reset_distribution()
        for server in self.servers:
            self.distribution[server]=0

        # Server data to be redistributed
        return set(self.servers)
        
    def update_servers(self, servers):
        self.servers=servers
        self.reset_distribution()

    def getDistribution(self):
        return self.distribution

    def select_node_for_put(self,key):
        node_index=self.select_node(key)
        self.distribution[self.servers[node_index]]+=1
        return node_index
        
    def select_node(self,key):
        key=key.encode('utf-8')
        max_weight=0
        selected_node = None
        for i in range(len(self.servers)):
            weight=generate_hash(key,self.servers[i].encode('utf-8'))
            if weight > max_weight:
                max_weight = weight
                selected_node = i
        return selected_node