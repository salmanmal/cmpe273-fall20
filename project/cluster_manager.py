import consul
import zmq
from consistent_hashing import ConsistentHashing
from hrw import HRW
import random

class ClusterManger:
    def __init__(self):
        self.consul_obj=consul.Consul()
        self.message_channel="tcp://127.0.0.1:4000"
        self.servers=self.get_server_list_from_services()
        context = zmq.Context()
        self.consumer_response = context.socket(zmq.PULL)
        self.consumer_response.bind(self.message_channel)
        self.create_clients()

        # For Consistent Hashing
        self.ch=ConsistentHashing(self.servers)

        # For HRW
        # self.ch=HRW(self.servers)


    def get_server_list_from_services(self):
        services=self.consul_obj.agent.services()
        servers=[]
        for key,data in services.items():
            address=data["Address"]
            port=data["Port"]
            servers.append(f"tcp://{address}:{port}")
        return servers

    def remove_from_consul_registry(self,server):
        split_server=server.split(":")
        port=split_server[-1]
        service_name=f"datanode#{port}"
        self.consul_obj.agent.service.deregister(service_name)

    def create_clients(self):
        producers={}
        context = zmq.Context()
        for server in self.servers:
            print(f"Creating a server connection to {server}...")
            producer_conn = context.socket(zmq.PUSH)
            producer_conn.connect(server)
            producers[server] = producer_conn
        self.producers=producers
    
    def create_single_client(self,server):
        print(f"Creating a server connection to {server}...")
        context=zmq.Context()
        producer_conn = context.socket(zmq.PUSH)
        producer_conn.connect(server)
        self.producers[server] = producer_conn
    
    def put(self,curr_req):
        curr_req["op"]="PUT"
        node = self.ch.select_node_for_put(curr_req["key"])
        self.producers[self.servers[node]].send_json(curr_req)  
        print(f"Consistent hashing :: sent data:{curr_req} to server {self.servers[node]}")

    def get(self,key):
        curr_req={"key":key,"op":"GET_ONE"}
        node = self.ch.select_node(curr_req["key"])
        self.producers[self.servers[node]].send_json(curr_req)  
        print(f"=> Consistent hashing :: Fetching key data:{curr_req} from server {self.servers[node]}")
        response= self.consumer_response.recv_json()
        print(f"=> Consistent hashing :: Fetched key data:{response} from server {self.servers[node]}")
        return response

    def unbind(self):
        self.consumer_response.unbind(self.message_channel)

    def remove_random_node(self):
        # randomly removes node
        random_server_index=random.randint(0,len(self.servers)-1)
        self.remove_node(self.servers[random_server_index])

    def remove_node(self,server):
        # remove node passed in function argument
        print("=>distribution before removing the node:")
        print(self.ch.getDistribution())
        

        print(f"=>removed Node {server}")
        # Fetch all the data to redistribute before removig node
        self.producers[server].send_json({"op":"GET_ALL_AND_CLEAR"})
        response=self.consumer_response.recv_json()

        # Remove server from list and recreate hashring
        self.servers.remove(server)
        self.remove_from_consul_registry(server)
        self.ch.update_servers(self.servers)
        collection=response["collection"]
        # Redistribute data on new hashring
        for i in range(len(collection)):
            node =self.ch.select_node_for_put(collection[i]["key"])
            collection[i]["op"]="PUT"
            self.producers[self.servers[node]].send_json(collection[i])  
        
        print("=>distribution after the rebalancing:")
        print(self.ch.getDistribution())

    def add_node(self):
        print("=> distribution before adding the node:")
        print(self.ch.getDistribution())
        
        current_set=set(self.servers)
        
        membership_list=self.get_server_list_from_services()
        
        new_servers=[]
        for ser in membership_list:
            if not ser in current_set:
                print(f"=> New node {ser} found!")
                new_servers.append(ser)
                
        
        servers_to_reiterate_data = set()
        for new_server in new_servers:
            servers_to_reiterate_data=servers_to_reiterate_data.union(self.ch.add_server(new_server))
            self.servers.append(ser)
            self.create_single_client(new_server)
            
        all_result=[]
        for server in servers_to_reiterate_data:
            self.producers[server].send_json({"op":"GET_ALL_AND_CLEAR"})
            response= self.consumer_response.recv_json()
            all_result.extend(response["collection"])
        
        print(f"=> rebalancing the data!")
        for i in range(len(all_result)):
            node = self.ch.select_node_for_put(all_result[i]["key"])
            all_result[i]["op"]="PUT"
            self.producers[self.servers[node]].send_json(all_result[i])  

        print("=> distribution after the rebalancing:")
        print(self.ch.getDistribution())
    
    def get_all(self):
        all_result=[]
        curr_req={"op":"GET_ALL"}
        for server in self.servers:
            self.producers[server].send_json(curr_req)
            response= self.consumer_response.recv_json()
            all_result.extend(response["collection"])

        print(f"Consistent hashing :: All data from all server:{all_result}")
        return all_result

    def getDistribution(self):
        return self.ch.getDistribution()