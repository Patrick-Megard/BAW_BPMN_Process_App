import pandas as pd
import os
import requests, urllib3
requests.packages.urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.auth import HTTPBasicAuth
from jsonpath_ng import jsonpath, parse
import json



baw_fields = {
    "process_mining_mapping": {
        "process_ID": "piid",
        "task_name": "name",
        "start_date": "startTime",
        "end_date": "completionTime",
        "owner": "owner",
        "team": "teamDisplayName"
    },
    "included_task_data": [
        "activationTime",
        "atRiskTime",
        "completionTime",
        "description",
        "isAtRisk",
        "originator",
        "priority",
        "startTime",
        "state",
        "piid",
        "priorityName",
        "teamDisplayName",
        "managerTeamDisplayName",
        "tkiid",
        "name",
        "status",
        "owner",
        "assignedToDisplayName",
        "assignedToType",
        "dueTime",
        "closeByUser"
    ],
    "excluded_task_data": [
        "description",
        "clientTypes",
        "containmentContextID",
        "kind",
        "externalActivitySnapshotID",
        "serviceID",
        "serviceSnapshotID",
        "serviceType",
        "flowObjectID",
        "nextTaskId",
        "actions",
        "teamName",
        "teamID",
        "managerTeamName",
        "managerTeamID",
        "displayName",
        "processInstanceName",
        "assignedTo",
        "assignedToID",
        "collaboration",
        "activationTime",
        "lastModificationTime",
        "assignedToDisplayName",
        "closeByUserFullName"
    ]
}



PROCESS_SEARCH_URL = "rest/bpm/wle/v1/processes/search?"
PROCESS_SEARCH_BPD_FILTER = "searchFilter="
PROCESS_SEARCH_PROJECT_FILTER = "&projectFilter="
TASK_SUMMARY_URL = "rest/bpm/wle/v1/process/"
TASK_SUMMARY_URL_SUFFIX = "/taskSummary/"
TASK_DETAIL_URL = "rest/bpm/wle/v1/task/"
TASK_DETAIL_URL_SUFFIX = "?parts=data"


def build_instance_search_url(config):
    url = config['root_url'] + PROCESS_SEARCH_URL

    # from_date and from_date_criteria
    from_date_str = config['from_date_criteria']+"="+config['from_date']

    # to_date and to_date_criteria
    to_date_str = config['to_date_criteria']+"="+config['to_date']

    url = url + from_date_str + "&" + to_date_str

    # Add the process name and project to the URL
    url = url + "&" + config['process_name'] + PROCESS_SEARCH_PROJECT_FILTER + config['project']

  
    if config['instance_limit'] > 0 :
        url = url + f"&limit={str(config['instance_limit'])}"

    if config['offset'] > 0 :
        url = url + f"&offset={str(config['offset'])}"

    if config['status_filter'] != "":
        url = url + "&statusFilter="+config['status_filter']

    return url

def get_instance_list(instance_list, config):
    try:
        url = build_instance_search_url(config)
        message = f"Search URL : {url}"
        response = requests.get(url, auth=config['auth_data'], verify=False)
        status = response.status_code

        if status == 200:
            instance_data_json = response.json()

            for bpd_instance in instance_data_json['data']['processes']:
                instance_list.append({'piid' : bpd_instance['piid']})
        else :
            error = json.loads(response.text)
            message = f"BAW REST API response code: {response.status_code}, reason: {response.reason}, {error['Data']['errorMessage']}"
            print(message)
    except Exception as e:
        message = f"Unexpected error processing BPD : {config['process_name']}"
        print(message)

    

# Function to fetch task details info for a specific instance
def create_event(task_id, event_data, config):
    try:
        url = config['root_url'] + TASK_DETAIL_URL + task_id + TASK_DETAIL_URL_SUFFIX
        task_detail_response = requests.get(url, auth=config['auth_data'], verify=False)
        if task_detail_response.status_code == 200:
            task_detail_data = task_detail_response.json()
            task_data = task_detail_data['data']
            task_data_keys = task_data.keys()

            # Create the process mining event
            event = {}
             # find and rename the keys that are mapped into process mining keys
            ipm_mapping = config['BAW_fields']['process_mining_mapping']
            ipm_fields = ipm_mapping.keys()
            for field in ipm_fields:
                if (ipm_mapping[field] in task_data_keys):
                    event[field] = task_data.pop(ipm_mapping[field])
                else:
                    print("Error: task data: %s mapped to: %s not found" % (ipm_mapping[field], field))

            # include the keys that in config['BAW_fields']['included_task_data']
            keepkeys = config['BAW_fields']['included_task_data']
            for key in keepkeys:
                if (key in task_data_keys):
                    event[key] = task_data.pop(key)

            # Take care of the process data if any, that's an array!
            # only if ['BAW']['export_exposed_variables'] == True
            if (config['export_exposed_variables'] == True) and ("processData" in task_data_keys):
                processdata = task_data["processData"]
                businessdata = processdata['businessData']
                for trackeddata in businessdata:
                    event["trkd."+trackeddata['name']] = trackeddata['value']

            # Append the data.variables that are listed
            data_variables = config['task_data_variables']

            # Search from 'data.variables' keep 'data.' before each variable name
            #
            for searched_var in data_variables:
                # Default value to use if no match is found in the task data
                variable_value = ""
                jsonpath_expression = parse("variables"+"."+searched_var)
                for match in jsonpath_expression.find(task_data['data']):
                    # Update the default variable
                    variable_value = match.value
                    break
                # Add the value to the event dictionary (could be "")
                event["tsk."+searched_var] = variable_value

            # append event to the event_data array
            event_data.append(event)
    except Exception as e:
        message = f"Unexpected error while creating event for task : {task_id}"
        print(message)


# Function to fetch task summary info for a specific instance
def get_tasks(instance_list, config):
    
    for instance in instance_list:
        try:
            url = config['root_url'] + TASK_SUMMARY_URL + instance['piid'] + TASK_SUMMARY_URL_SUFFIX

            response = requests.get(url, auth=config['auth_data'], verify=False)
            if response.status_code == 200:
                task_summary_data = response.json()
                task_list = []
                for task_summary in task_summary_data['data']['tasks']:
                    task_id = task_summary['tkiid']
                    task_list.append(task_id)
            instance['task_list'] = task_list
        
        except Exception as e:
            print("--- There was an error in the execution: "+str(e))


def extract_baw_data(instance_list, event_data, config): 
    try:
        # if instance_list size is 0, fetch the processes from BAW
        if len(instance_list) == 0:
            loop_instance_list = []
            get_instance_list(loop_instance_list, config)
            if (len(loop_instance_list) == 0):
                print("No instances match the search")
                return instance_list
            else:
                print(f"Found : {len(loop_instance_list)} instances of BPD {config['process_name']} in project {config['project']}")
        else:
            # there are still instances in the list, we are in another paging loop
            # loop_instance_list is used to fetch the task summaries and details during this loop
            loop_instance_list = instance_list

        # If loop and paging size: we fetch all the instance lists, we extract the task details for at max paging size instance at each loop.
        # split the instance list to fetch at max config['paging_size']. The rest will be processed at next loops
        if (config['paging_size'] > 0 and len(loop_instance_list) > config['paging_size']):
            instance_list = loop_instance_list[config['paging_size']:]
            loop_instance_list = loop_instance_list[:config['paging_size']]
        else:
            # all the instances are fetched, that's the last loop
            instance_list = []


        # bpd_instance_dict looks like this: {instance_1 : [task_1, task_2], instance_2 : [task_3, task_4], ...}
        # Let's use instance_list that is now: [{'instance': instance_1, 'tasks': [task1, task2]}, {'instance': instance_2, 'tasks': [task3, task4]}]

        instance_count = len(loop_instance_list)
        print(f"Processing {instance_count} instances. Fetching task summaries .....")

        get_tasks(loop_instance_list, config)

        # Calculate how many tasks exist in the dictionary
        task_count = 0
        for instance in loop_instance_list:
            task_count += len(instance['task_list'])
        print(f"Processing {task_count} tasks. Fetching task details .....")

        # Create the event row for each task of each instance
        for instance in loop_instance_list:
            for task in instance['task_list']:
                create_event(task, event_data, config)

    except Exception as e:
        print("--- There was an error in the execution: "+str(e))
            
    print("Still %s instances to process" % len(instance_list))
    return instance_list


# This is the entry function for the logic file.

default_config = {
        "root_url": "https://9.172.229.85:9443/",
        "user": "admin",
        "password": "admin",
        "password_env_var": "",
        "project": "HSS",
        "process_name": "Standard HR Open New Position",
        "from_date": "2022-10-08T23:44:44Z",
        "from_date_criteria": "createdAfter",
        "to_date": "2022-11-23T22:33:33Z",
        "to_date_criteria": "modifiedBefore",
        "paging_size": 0,
        "status_filter": "",
        "loop_rate": 0,
        "instance_limit": 0,
        "offset": 0,
        "task_data_variables": [
            "requisition.gmApproval",
            "requisition.requester"
        ],
        "export_exposed_variables": False
    }

def execute(context):
    '''   if config['root_url'] != default_config['root_url']:
        config['root_url'] = default_config['root_url']
    if config['user'] != default_config['user']:
        config['user'] = default_config['user']
    if config['password'] != default_config['password']:
        config['password'] = default_config['password']
    if config['project'] != default_config['project']:
        config['project'] = default_config['project']
    if config['process_name'] != default_config['process_name']:
        config['process_name'] = default_config['process_name']
    if config['from_date'] != default_config['from_date']:
        config['from_date'] = default_config['from_date']
    if config['from_date_criteria'] != default_config['from_date_criteria']:
        config['from_date_criteria'] = default_config['from_date_criteria']
    if config['to_date'] != default_config['to_date']:
        config['to_date'] = default_config['to_date']
    if config['to_date_criteria'] != default_config['to_date_criteria']:
        config['to_date_criteria'] = default_config['to_date_criteria']'''

    # Complement the config fields
    config = context['config']
    # task_data_variables is entered as a string like this: "requisition.gmApproval,requisition.requester"
    # we have to split it and put each string in an array
    # remove any blank character
    config['task_data_variables'] = config['task_data_variables'].replace(' ','')
    config['task_data_variables'] = config['task_data_variables'].split(',');

    config['instance_limit'] = int(config['instance_limit'])
    config['BAW_fields'] = baw_fields
    config['paging_size'] = 0
    config['status_filter'] = ''
    config['loop_rate'] = 0
    config['offset'] = 0
    config['export_exposed_variables'] = False
        
    event_list = []
    instance_list = []
    df_final = pd.DataFrame()
    while(1):
        config['auth_data'] = HTTPBasicAuth(config['user'], config['password'])
        instance_list = extract_baw_data(instance_list, event_list, config)

        if event_list !=  []: # there are events to send
            df_loop = pd.DataFrame(event_list)
        if instance_list == []: # Nothing more, exit
            print("Done, bye!")
            break;    
    df_final = pd.concat([df_final, df_loop])
    return df_final


if __name__ == "__main__":
    context = {'config': default_config}
    df = execute(context) 
    print (df)