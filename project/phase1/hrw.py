import hashlib

def generate_hash(key,server_i):
    m=hashlib.md5()
    m.update(key)
    m.update(server_i)
    return int(m.hexdigest(),16)

def select_node(key,servers):
    key=key.encode('utf-8')
    max_weight=0
    selected_node = None
    for i in range(len(servers)):
        weight=generate_hash(key,servers[i].encode('utf-8'))
        if weight > max_weight:
            max_weight = weight
            selected_node = i
    return selected_node
