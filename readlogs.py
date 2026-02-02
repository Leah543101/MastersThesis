# Read data from external file
#external file contains json data
#json data is a chain of dictionaries
#get all keys 

import json
import os
import pandas as pd
from pandas import json_normalize
import re
import ast
from collections import defaultdict

path = os.path.join("./smalllogs/spark-b6cbe34c521e44439afd3f5e7e2e5977")

records =[]
def read_logs(datapath):
    global records
    
    with open(datapath, 'r') as json_data:

        for i, line in enumerate(json_data):
            #if i == max_records:
            #    break

            line = line.strip()

            if not line:
                continue
            #print(f"{line} \n")
            try:
                #get json data
                jsondata = json.loads(line.strip())
                #convert to pandas for insights
                records.append(jsondata)
            #throw exception if it is not in the desired json format   
            except json.decoder.JSONDecodeError as e:
                print(e)

    #print(records)
    return records

def storeLogscsv():
    logs = read_logs(path)
    recordsdf = pd.DataFrame(logs)
    flat_data = json_normalize(recordsdf.to_dict(orient="records"))
    columns_list = flat_data.columns.to_list()


    unstruct_data = pd.json_normalize(read_logs(path),sep=".")


    def stringify_complex(v):
        return json.dumps(v, ensure_ascii=False) if isinstance(v,(list,dict)) else v

    for col in unstruct_data.columns:
        unstruct_data[col] = unstruct_data[col].map(stringify_complex)

    df = unstruct_data.reindex(columns=columns_list)
    output_path = "spark-b6cb.csv"
    write_header = not os.path.exists(output_path)

    df.to_csv(output_path,index=False,mode='a',header=write_header,encoding="utf-8")
    

def recordevents():
    records  = read_logs(path)
    record_events =[]
    for value in records:
        if isinstance(value,str):
            match = re.search(r'\{.*\}', value)
            if match:
                dict_string = match.group()
                dictionary = ast.literal_eval(dict_string)
                record_events.append(dictionary['Event'])
        else:
            record_events.append(value["Event"])
             

    return record_events
    

def check_all_events_captured():
    events_from_data = recordevents()
    events = [
                "SparkListenerLogStart",
                "SparkListenerResourceProfileAdded",
                "SparkListenerBlockManagerAdded",
                "SparkListenerEnvironmentUpdate"
                "SparkListenerApplicationStart"
                "SparkListenerExecutorAdded"
                "SparkListenerBlockManagerAdded"
                "SparkListenerExecutorAdded"
                "SparkListenerBlockManagerAdded"
                "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
                "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
                "SparkListenerJobStart",
                "SparkListenerStageSubmitted",
                "SparkListenerTaskStart",
                "SparkListenerTaskEnd",
                "SparkListenerStageCompleted",
                "SparkListenerJobEnd",
                "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
                "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
                "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
                "SparkListenerJobStart",
                "SparkListenerStageSubmitted",
                "SparkListenerTaskStart",
                "SparkListenerTaskEnd",
                "SparkListenerStageCompleted",
                "SparkListenerJobEnd",
                "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
                "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
                "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate",
                "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
                "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
                "SparkListenerJobStart",
                "SparkListenerStageSubmitted",
                "SparkListenerTaskStart",
                "SparkListenerTaskEnd",
                "SparkListenerStageCompleted",
                "SparkListenerJobEnd",
                "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate",
                "SparkListenerJobStart",
                "SparkListenerStageSubmitted",
                "SparkListenerTaskStart",
                "SparkListenerTaskEnd",
                "SparkListenerStageCompleted",
                "SparkListenerJobEnd",
                "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
                "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
                "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate",
                "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
                "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
                "SparkListenerJobStart",
                "SparkListenerStageSubmitted",
                "SparkListenerTaskStart",
                "SparkListenerTaskEnd",
                "SparkListenerStageCompleted",
                "SparkListenerJobEnd",
                "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate",
                "SparkListenerJobStart",
                "SparkListenerStageSubmitted",
                "SparkListenerTaskStart",
                "SparkListenerTaskEnd",
                "SparkListenerStageCompleted",
                "SparkListenerJobEnd",
                "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
                "SparkListenerApplicationEnd"
            ]
    for entry in events:
        if entry in events_from_data:
            capture_entry =  entry
            return capture_entry
        else:
            return f"entry {entry} not found"


def recordentriesjson():
    events_keys = read_logs(path) 
    groups = defaultdict(list)

    for val in events_keys:
        if not isinstance(val, dict):
            match = re.search(r'\{.*\}', val)
            if match:
                dict_string = match.group()
                dictionary = ast.literal_eval(dict_string)
                events_keys.append(dictionary['Event'])
        else:
            #print("Processing records:")
            common_entries = next(iter(val.keys()))
            groups[common_entries].append(val)
    #print(f"{groups} \n")

    for key, items in groups.items():
        filename = f"{key}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(items,f, ensure_ascii=False,indent=2)
            #print(f"Created {filename} for")
    return groups

def eventssubfiles():
    data = recordentriesjson()
    for value in data["Event"]:
        value_key = value["Event"]
        filename = os.path.join(f"./smalllogs/spark-b6cbe/{value_key}.json")

        if os.path.exists(filename):
            with open(filename, 'r', encoding="utf-8") as f:
                try:
                    existing = json.load(f)
                except json.JSONDecodeError:
                    existing = []
        else:
            existing = []

        
        if isinstance(existing, dict):
            existing = [existing]
        elif not isinstance(existing, list):
            existing = []

        existing.append(value)
 
        with open(filename, "w", encoding="utf-8") as f:     
                json.dump(existing,f, ensure_ascii=False,indent=2)
        # from the event, create a new file
    return data
eventssubfiles()
    
# check if event matched the file name
# if it matches add the data to that file 