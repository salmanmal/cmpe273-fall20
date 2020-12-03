import consul

c=consul.Consul()

# Prints the list of active members in the cluster
print(c.agent.services())

# Consul Agent command
# consul agent -dev -enable-script-checks