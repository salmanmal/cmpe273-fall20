import schedule
import time
import yaml
import sys
import requests


# request bin to test it out
my_dict={}


def send_http_request(method,url):
    response = requests.request(method,url)
    return response

def read_input(input_file):
    global my_dict
    with open(input_file) as f:
        my_dict = yaml.safe_load(f)

def search_step(id):
    if "Steps" in my_dict:
        for i in range(len(my_dict["Steps"])):
            if id in my_dict["Steps"][i]:
                return my_dict["Steps"][i][id]
        return 
    else:
        return 
    
def checkForInputData(key_val,input_data):
    if type(key_val) == str and key_val.startswith("::input:") and input_data:
        return input_data
    else :
        return key_val
    
def checkForResponseData(data,response):
    if type(data) == str and data.startswith("http.response."):
        data=data.replace("http.response.","")
        keys=data.split(".")
        data=response
        if len(keys)>0:
            if keys[0]=="code":
                data=data.status_code
            elif keys[0]=="headers":
                data=data.headers
            elif keys[0]=="body":
                data=data.text

        for i in range(len(keys)):
            if i>0 and keys[i] in data:
                data=data[keys[i]]
    return data

def execute_step(id, input_data):
    step_data=search_step(id)
    if step_data:
        if "type" in step_data:
            types=checkForInputData(step_data["type"],input_data)
            
            if types=="HTTP_CLIENT":
                
                if "method" in step_data and step_data["method"] and "outbound_url" in step_data and step_data["outbound_url"]:
                    method=checkForInputData(step_data["method"],input_data)
                    outbound_url=checkForInputData(step_data["outbound_url"],input_data)

                    response=send_http_request(method,outbound_url)
                    if "condition" in step_data:
                        condition=checkForInputData(step_data["condition"],input_data)
                        if "if" in condition:
                            _if=checkForInputData(condition["if"],input_data)
                            if "equal" in _if:
                                equal=checkForInputData(_if["equal"],input_data)
                                if "left" in equal and "right" in equal:
                                    left=checkForInputData(equal["left"],input_data)
                                    left=checkForResponseData(left,response)
                                    right=checkForInputData(equal["right"],input_data)
                                    right=checkForResponseData(left,response)
                                    
                                    if left==right:
                                       # Do then part
                                        if "then" in condition:
                                            then=checkForInputData(condition["then"],input_data)
                                            data=""
                                            if "data" in then:
                                                data=checkForInputData(then["data"],input_data)
                                                data=checkForResponseData(data,response)

                                            if "action" in then:
                                                action=checkForInputData(then["action"],input_data)
                                                if action.startswith("::print"):
                                                    print(data)
                                                elif action.startswith("::invoke:step:"):
                                                    action=action.replace("::invoke:step:","")
                                                    execute_step(int(action),data)
                                                else :
                                                    print("Error : Invalid action {}".format(action))                                               
                                        else :
                                            print("Error : No Action to handle")                                       
                                    elif "else" in step_data["condition"]:
                                        print('we are in else part')
                                        # Do Else Part
                                        
                                    else:
                                        print("Error : No Action to executed")
                                else:
                                    print("Error : Missing values to compare.")
                            else:
                                print("Error : Invalid operator <Expected 'equal'>")
                        else:
                            print("Error : No If block present")
                    else:
                        print("Error : No conditional action present")
                else:
                    print("Error : Not enough info to make http request")
            else:
                print("Error : Invalid type {}".format(types))
        else:
            print("Error : type is missing")
    else:
        print("Error : Invalid Step id {}".format(id))

def job():
    for i in my_dict["Scheduler"]["step_id_to_execute"]:
        execute_step(i,"")

def start():
    if "Scheduler" in my_dict:
        if "when" in my_dict["Scheduler"] and  "step_id_to_execute" in my_dict["Scheduler"] and len(my_dict["Scheduler"]["step_id_to_execute"])>0:

            cron_schedule=my_dict["Scheduler"]["when"].split(" ")

            if cron_schedule[1]==cron_schedule[2]=="*":
                if cron_schedule[0]=="*":
                    schedule.every().minute.do(job)
                else:
                    schedule.every(int(cron_schedule[0])).minutes.do(job)
            else :
                weekday=cron_schedule[2]
                minute=cron_schedule[0]

                if minute=="*":
                    minute="00"
                elif int(minute)<10:
                    minute="0"+minute
                
                hour=cron_schedule[1]
                if hour=="*":
                    hour="00"
                elif int(minute)<10:
                    hour="0"+hour
                
                schedule_time=hour+":"+minute

                print(schedule_time)
                if weekday=="*":
                    schedule.every().day.at(schedule_time).do(job)
                elif weekday=="0" or weekday=="7":
                    print("came")
                    schedule.every().sunday.at(schedule_time).do(job)
                elif weekday=="1":
                    schedule.every().monday.at(schedule_time).do(job)
                elif weekday=="2":
                    schedule.every().tuesday.at(schedule_time).do(job)
                elif weekday=="3":
                    schedule.every().wednesday.at(schedule_time).do(job)
                elif weekday=="4":
                    schedule.every().thursday.at(schedule_time).do(job)
                elif weekday=="5":
                    schedule.every().friday.at(schedule_time).do(job)
                elif weekday=="6":
                    schedule.every().saturday.at(schedule_time).do(job)

            while True:
                schedule.run_pending()
                time.sleep(1)
        elif "step_id_to_execute" in my_dict["Scheduler"] and len(my_dict["Scheduler"]["step_id_to_execute"])>0:
            job()

def main(argv):
    input_file=argv[0]
    read_input(input_file)
    start()

if __name__ == "__main__":
    main(sys.argv[1:])
