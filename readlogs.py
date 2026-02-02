# Read data from external file
#external file contains json data
#json data is a chain of dictionaries
#get all keys 

import json
import os
import pandas as pd
from pandas import json_normalize
import csv



path = os.path.join("/root/ThesisProject/testsparklog/smalllogs/spark-b6cbe34c521e44439afd3f5e7e2e5977")
records =[]
max_records = 20000
with open(path, 'r') as json_data:

    for i, line in enumerate(json_data):
        if i == max_records:
            break

        line = line.strip()

        if not line:
            continue
        try:
            #get json data
            jsondata = json.loads(line.strip())
            #convert to pandas for insights
            records.append(jsondata)
            
        except json.decoder.JSONDecodeError as e:
            print(e)

print(records)

recordsdf = pd.DataFrame(records)
flat_data = json_normalize(recordsdf.to_dict(orient="records"))
columns_list = flat_data.columns.to_list()


unstruct_data = pd.json_normalize(records,sep=".")


def stringify_complex(v):
    return json.dumps(v, ensure_ascii=False) if isinstance(v,(list,dict)) else v

for col in unstruct_data.columns:
    unstruct_data[col] = unstruct_data[col].map(stringify_complex)

df = unstruct_data.reindex(columns=columns_list)
output_path = "spark-b6cb.csv"
write_header = not os.path.exists(output_path)

df.to_csv(output_path,index=False,mode='a',header=write_header,encoding="utf-8")


'''
df = pd.DataFrame(records)
flat_data = json_normalize(df.to_dict(orient="records"))
new_flat_data = flat_data.columns.to_list()

with open("small-logs/spark-b6cb.csv", 'w', newline='') as sparkheader:
    wr = csv.writer(sparkheader,quoting=csv.QUOTE_ALL)
    wr.writerow(new_flat_data)
'''


#new_flat_data.to_csv("smalllogs/spark-b6cb.csv", index=False)