from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
from pyspark.sql import SQLContext
from sklearn.metrics import accuracy_score
import numpy as np
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

import os
import sys

#setting env vars- using python3
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext("local[2]","test")

process_id_cnt = 1
ssc = StreamingContext(sc,1)
spark = SparkSession(sc)
sql_context = SQLContext(sc)
input_stream_lines = ssc.socketTextStream('localhost',6100)
input_batches = input_stream_lines.flatMap(lambda line: line.split("\n"))


#the function used to process each input batch streamed
def processBatch(input_rdd,process_id_cnt):
        if not input_rdd.isEmpty():
            json_str = input_rdd.collect()
            for rows in json_str:
                json_obj_temp = json.loads(rows,strict = False)
            rdd_arr = []

            #computing the number of attributes- for our dataset it is 42
            feat_len=len(json_obj_temp['0'])

            #reading from the log file to check which file is being streamed -train.txt or test.txt
            f = open("log.txt", "r")
            file_currently_streaming=f.read()

            #dictionary to map the subclass attack types to the main attack types
            #the target column will only contain the main attack types after mapping used as a target
            #class in the classification later on
            target_dict={'back':"DoS",'land':"DoS",'neptune':"DoS",'pod':"DoS",'smurf':"DoS",
      'teardrop':"DoS",'apache2':"DoS",'udpstorm':"DoS",
      'processtable':"DoS",'worm':"DoS",
     'satan':"Probe",'ipsweep':"Probe",'nmap':"Probe",
      'portsweep':"Probe",'mscan':"Probe",'saint':"Probe",
     'guess_passwd':"R2L",'ftp_write':"R2L",'mailbomb':"R2L",
      'imap':"R2L",
      'phf':"R2L",'multihop':"R2L",'warezmaster':"R2L",
      'warezclient':"R2L",'spy':"R2L",'xlock':"R2L",
      'xsnoop':"R2L"
      ,'snmpguess':"R2L",'snmpgetattack':"R2L",
      'httptunnel':"R2L",'sendmail':"R2L",'named':"R2L",
     'buffer_overflow':"U2R",'loadmodule':"U2R",'rootkit':"U2R",
      'perl':"U2R",'sqlattack':"U2R",'xterm':"U2R",'ps':"U2R",'normal':"Normal"}

            #dictionary to map the protocol type to int values.
            protocol_dict={'tcp':2,'udp':1,'icmp':0}

            for i in json_obj_temp.keys(): 
                temp_row = []
                for j in range(feat_len):
                    #attr_val will contain the value of the cell at row i and column j
                	attr_val=str(json_obj_temp[i]['feature'+str(j)]).strip(' ')
                	if(j==41):
                        #if the column is the target column("attack") then using the dictionary target_dict
                        #the main attack type is appended to the temp_row array
                		temp_row.append(target_dict[attr_val])
                	elif(j==1):
                        #if the column is the column("protocol_type") then using the dictionary protocol_dict
                        #the protocol type is mapped to an int val
                		temp_row.append(protocol_dict[attr_val])
                	elif(j==2 or j==3):
                        #enter a dummy value later to be dropped
                        #unnecessary columns
                		temp_row.append("tbr")
                	else:
                        #all other attr values are casted as float values
                		temp_row.append(float(attr_val))
                		
                rdd_arr.append(tuple(temp_row))
            
            print("Recieved batch of data of length :",len(json_obj_temp.keys()))

            #converting the array of rows to rdd
            final_rdd = sc.parallelize(rdd_arr) 
            #converting the rdd to a DataFrame
            df = final_rdd.toDF()
            print(df.head())

            #extracting the target column
            Y=np.array(df.select('_42').collect())
            #dropping the target column
            df=df.drop('_42')

            #dropping unnecessary columns
            df=df.drop('_4')
            df=df.drop('_3')
            df=df.drop('_20')
            print(df.head())

            X=np.array(df.collect())

            print(file_currently_streaming)
            
            labels = [] 
            if(file_currently_streaming == 'train'):
            	print(X.shape,Y.shape)           	
            else:
            	print("its time to predict")           	
                
            print("batch completed\n\n")

input_batches.foreachRDD(lambda rdd : processBatch(rdd,process_id_cnt))

process_id_cnt+=1

ssc.start()
ssc.awaitTermination()

sys.exit("done")
