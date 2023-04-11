import sys,json

import requests, urllib3
requests.packages.urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from requests.auth import HTTPBasicAuth
import csv
import logging
from tqdm import tqdm
import time
import sys
import aiohttp
import asyncio
from time import sleep
from jsonpath_ng import jsonpath, parse
from yaml.loader import SafeLoader
from datetime import datetime
import zipfile
import os 



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

def get_instance_list(instance_list, config, logger):
    try:
        url = build_instance_search_url(config)
        message = f"Search URL : {url}"
        logger.info(url)
        response = requests.get(url, auth=config['auth_data'], verify=False)
        status = response.status_code

        if status == 200:
            instance_data_json = response.json()
            logger.debug("Retrieved instance list: %s" % instance_data_json)

            for bpd_instance in instance_data_json['data']['processes']:
                instance_list.append({'piid' : bpd_instance['piid']})
        else :
            error = json.loads(response.text)
            message = f"BAW REST API response code: {response.status_code}, reason: {response.reason}, {error['Data']['errorMessage']}"
            logger.error(message)
            print(message)
    except Exception as e:
        message = f"Unexpected error processing BPD : {config['process_name']}"
        logger.error(message)
        logger.error(e)
    
    return instance_list

# Function to fetch task details info for a specific instance
async def create_events(session, instance, event_data, pbar, config, logger):

    auth = get_aiohttp_BAW_auth(config, logger)
    if auth == 0:
        logger.error('ERROR getting Auth')
        return
    try:
        for task_id in instance['task_list']:
            url = config['root_url'] + TASK_DETAIL_URL + task_id + TASK_DETAIL_URL_SUFFIX
            logger.debug(f"Creating event for task : {task_id}")
            async with session.get(url, auth=auth, ssl=False) as task_detail_response:
                task_detail_status = task_detail_response.status
                if task_detail_status == 200:
                    task_detail_data = await task_detail_response.json()
                    # print(task_detail_data)

                    task_data=task_detail_data['data']
                    task_data_keys = task_data.keys()

                    # Create the process mining event 
                    event={}

                    # find and rename the keys that are mapped into process mining keys
                    ipm_mapping = config['BAW_fields']['process_mining_mapping']
                    ipm_fields = ipm_mapping.keys()
                    for field in ipm_fields:
                        if (ipm_mapping[field] in task_data_keys):
                            event[field]=task_data.pop(ipm_mapping[field])
                        else:
                            logger.error("Error: task data: %s mapped to: %s not found" % (ipm_mapping[field], field))

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
                            event["trkd."+trackeddata['name']]=trackeddata['value']
                        
                    # Append the data.variables that are listed
                    data_variables = config['task_data_variables']
                    # Search from 'data.variables' keep 'data.' before each variable name
                    for searched_var in data_variables:
                        # Default value to use if no match is found in the task data
                        variable_value = ""
                        jsonpath_expression = parse("variables"+"."+searched_var)
                        for match in jsonpath_expression.find(task_data['data']):
                            # Update the default variable
                            variable_value = match.value
                            break
                        # Add the value to the event dictionary (could be "")
                        event["tsk."+searched_var]=variable_value
                    
                    # append event to the event_data array
                    event_data.append(event)
                    pbar.update(1)
    except Exception as e:
        message = f"Unexpected error while creating event from : {task_id}"
        print(message)

# get BAW auth from environment variable or from config file
def get_aiohttp_BAW_auth(config, logger):
    # if config['password_env_var'] != "" we search the BAW admin password in the environment variable
    #print("get_BAW_auth")

    if (config['password_env_var'] != ""):
        #print("environment vars: %s" % os.environ)
        pwd = os.getenv(config['password_env_var'])
    
        if pwd is None: # no env variable set
            logger.error(f"Error environment variable: {config['password_env_var']} for BAW password not found")
            if (config['password']!= ""):
                # try with the password
                pwd = config['password']
            else :# no pwd set in the config file
                print("BAW extraction error: missing password")
                logger.error("BAW extraction error: missing password")
                return 0    
    else : # use the password
        pwd = config['password']

    return(aiohttp.BasicAuth(login=config['user'], password=pwd, encoding='utf-8'))

# Function to fetch task summary info for a specific instance
async def get_tasks(session, instance, pbar, config, logger):

    logger.debug('Fetching tasks for bpd instance : ' + instance['piid'])
    url = config['root_url'] + TASK_SUMMARY_URL + instance['piid'] + TASK_SUMMARY_URL_SUFFIX
    #print(f"Task summaries URL: {url}")
    
    auth = get_aiohttp_BAW_auth(config, logger)
    if auth == 0:
        logger.error('ERROR getting Auth')
        return

    async with session.get(url, auth=auth, ssl=False) as task_summary_response:
        task_summary_status = task_summary_response.status
        if task_summary_status == 200:
            task_summary_data = await task_summary_response.json()

            task_list = []
            for task_summary in task_summary_data['data']['tasks']:
                task_id = task_summary['tkiid']
                logger.debug(f"Instance {instance} found Task : {task_id}")
                task_list.append(task_id)

            # We have the instance + a list of its task id's
            # Update the bpd_instance_dict that was passed from the calling function
            instance['task_list'] = task_list
            pbar.update(1)


async def get_instance_data(instance_list, event_data, config, logger):
    # Dictionary to hold bpd instances and related tasks
    bpd_instance_dict = {}
    ## Replace bpd_instance_dict with the array of instances, where each instance is an object

    instance_count = len(instance_list)
    print(f"Processing {instance_count} instances. Getting tasks for each instance ...")
    logger.info(f"Processing {instance_count} instances. Getting tasks for each instance ...")

    # Initialise the connector
    connector = aiohttp.TCPConnector(limit=config['thread_count'])

    # create a ClientTimeout to allow for long running jobs
    infinite_timeout = aiohttp.ClientTimeout(total=None , connect=None,
                          sock_connect=None, sock_read=None)

    # Get the task list for each instance
    async with aiohttp.ClientSession(connector=connector, timeout=infinite_timeout) as session:
        async_tasks = []
        pbar = tqdm(total=instance_count)
        for instance in instance_list:
            async_task = asyncio.ensure_future(get_tasks(session, instance, pbar, config, logger))
            async_tasks.append(async_task)

        await asyncio.gather(*async_tasks)
        pbar.close()

    # Re-initialise the connector
    connector = aiohttp.TCPConnector(limit=config['thread_count'])

    # Calculate how many tasks exist in the dictionary

    task_count = 0

    for instance in instance_list:
        task_count += len(instance['task_list'])
    print(f"Processing {task_count} tasks. Creating an event from each task ...")
    logger.info(f"Processing {task_count} tasks. Creating an event from each task ...")

    async with aiohttp.ClientSession(connector=connector, timeout=infinite_timeout) as session:
        async_tasks = []

        pbar = tqdm(total=task_count)
        for instance in instance_list:
            async_task = asyncio.ensure_future(create_events(session, instance, event_data, pbar, config, logger))
            async_tasks.append(async_task)
        await asyncio.gather(*async_tasks)
        pbar.close()

def setup_logger(config, level):
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
    file_handler = logging.FileHandler(config['logfile'])
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(level)
    
    return logger

def extract_baw_data(instance_list, event_data, config, logger): 

    try:
        logger.info('Extraction from BAW starting')
        # if instance_list size is 0, fetch the processes
        if len(instance_list) == 0:
            run_instance_list = get_instance_list([], config, logger)
            if (len(run_instance_list) == 0):
                print("No instances match the search")
                logger.info("No instances match the search")
                return instance_list
            else:
                print(f"Found : {len(run_instance_list)} instances of BPD {config['process_name']} in project {config['project']}")
                logger.info(f"Found : {len(run_instance_list)} instances of BPD {config['process_name']} in project {config['project']}")
        else:
            # we use another loop to fetch data from the instance list
            run_instance_list = instance_list

        # If loop and paging size: we fetch all the instance lists, we extract the task details for at max paging size instance at each loop.
        # In this case we don't update last_before and last_after because we have not searched new instances in BAW 
        # split the instance list to fetch at max config['paging_size']. The rest will be processed at next loops
        if (config['loop_rate']>0 and config['paging_size']>0 and len(run_instance_list)>config['paging_size']):
            instance_list = run_instance_list[config['paging_size']:]
            run_instance_list = run_instance_list[:config['paging_size']]

        else:
            # all the instances are fetched
            instance_list = []

        # get_instance_data() calls get_task_summaries() then get_task_details()
        asyncio.run(get_instance_data(run_instance_list, event_data, config, logger))

    except Exception as e:
        logger.info('There was an error in the execution'+str(e))
        print("--- There was an error in the execution: "+str(e))
            
    print("Still %s instances to process" % len(instance_list))
    logger.info("Still %s instances to process" % len(instance_list))

    return instance_list



def file_compress(file_to_write, out_zip_file):
    # Select the compression mode ZIP_DEFLATED for compression
    # or zipfile.ZIP_STORED to just store the file
    compression = zipfile.ZIP_DEFLATED
    # create the zip file first parameter path/name, second mode
    # print(f' *** out_zip_file is - {out_zip_file}')
    zf = zipfile.ZipFile(out_zip_file, mode="w")
    try:
        # Add file to the zip file
        # first parameter file to zip, second filename in zip
        # print(f' *** Processing file {file_to_write}')
        zf.write(file_to_write, file_to_write, compress_type=compression)

    except FileNotFoundError as e:
        print(f' *** Exception occurred during zip process - {e}')
    finally:
        # Don't forget to close the file!
        zf.close()

def generate_csv_file(event_data, config):
    # event_data contains an array of events as json objects
    if (len(event_data) == 0):
        print("No events extracted")
        return

    cwd = os.getcwd()
    os.chdir(config['csvpath'])    
    filename = config['csvfilename']+".csv"
    zipfilename = config['csvfilename']+".zip"

    data_file = open(filename, 'w')
    csv_writer = csv.writer(data_file)
    header=event_data[0].keys()
    csv_writer.writerow(header)
    for event in event_data:
        csv_writer.writerow(event.values())
    data_file.close()
    
    file_compress(filename, zipfilename)
    os.chdir(cwd)
    return config['csvpath']+zipfilename


# This is the entry function for the logic file.
import pandas as pd
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
        "paging_size": 3,
        "status_filter": "",
        "loop_rate": 1,
        "thread_count": 10,
        "instance_limit": 0,
        "offset": 0,
        "logfile": "logs.log",
        "task_data_variables": [
            "requisition.gmApproval",
            "requisition.requester"
        ],
        "export_exposed_variables": False
    }

def execute(context):

    config = default_config
    config['BAW_fields'] = baw_fields
    logger = setup_logger(config, logging.DEBUG)
    event_list = []
    instance_list = []
    df_final = pd.DataFrame()
    while(1):
        config['auth_data'] = HTTPBasicAuth(config['user'], config['password'])
        instance_list = extract_baw_data(instance_list, event_list, config, logger)

        if event_list !=  []: # there are events to send
            df_loop = pd.DataFrame(event_list)
        if instance_list == []: # Nothing more, exit
            print("Done, bye!")
            break;    
    df_final = pd.concat([df_final, df_loop])
    return df_final


if __name__ == "__main__":
    df = execute(0)
    print (df)