import zmq
import sys
import time

def send_to_voting_workers():
    context = zmq.Context()
    sock = context.socket(zmq.PUSH)
    sock.bind("tcp://127.0.0.1:4000")
    
    time.sleep(1)
    
    # send worker to count East states' votes
    sock.send_json({ 'region': 'east' })
    
    # send worker to count East states' votes
    sock.send_json({ 'region': 'west' })
    
    time.sleep(1)
    
 
def receive_result():
    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    receiver.bind("tcp://127.0.0.1:3000")
    
    result_1 = receiver.recv_json()
    result_2 = receiver.recv_json()
    print("in receive")
    # FIXME 
    # Calculate total votes from result 1 and 2.
    result={"x_votes":0, "y_votes":0}
    result["x_votes"]+=result_1["x"]
    result["y_votes"]+=result_1["y"]

    result["x_votes"]+=result_2["x"]
    result["y_votes"]+=result_2["y"]

    return result
     

def main():
    send_to_voting_workers()
    time.sleep(5)
    receive_result()
    print(receive_result)

if __name__ == "__main__":
    main()
    