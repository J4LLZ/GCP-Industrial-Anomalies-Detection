import argparse
import pandas as pd 
import datetime
from random import randint
from time import sleep
import numpy as np
import uuid
from google.cloud import pubsub_v1


def run():
    print('The process has been started')
    parser = argparse.ArgumentParser(description='data generator')
    parser.add_argument('--project',required=True, help='Google Cloud project')
    parser.add_argument('--topic',required=True, help='Pub/Sub topic')
    parser.add_argument('--source',required=True, help='Data sample GCS source path ')
    parser.add_argument('--iterations_num', type = int, default = 1, help='number of iterations')
    parser.add_argument('--iterations_num', type = int, default = 1, help='number of iterations')
    parser.add_argument('--sources_num', type = int,default = 1, help='number of sources')
    parser.add_argument('--min_data_latency', type = int,default = 5, help='latency min')
    parser.add_argument('--max_data_latency',type = int ,default = 10, help='latency max')
    opts = parser.parse_args()
    
    source_path = opts.source
    project_id = opts.project
    topic_id = opts.topic
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    
    df = pd.read_json(source_path, lines = True)
    
    iter_num = opts.iterations_num
    sources_num = opts.sources_num
    
    
    
    def sum_funct(x,y):
        if x is None or y is None:
            return None
        else:
            return x + y

    for i in range(iter_num):
        for j in range(sources_num):
            print(f'iteration : {i + 1}. source: {j}')
            tdf = df.sample()
            tdf['datetime'] = [[str(x) for x in [pd.to_datetime(item) + pd.to_timedelta(randint(1, 365 * 2), unit='D') + pd.to_timedelta(randint(1, 24), unit='H') + pd.to_timedelta(randint(1, 60), unit='Min') for item in tdf['datetime']][0].values]]
            tdf['actual_velocity'] = tdf['actual_velocity'].apply(lambda x: [sum_funct(a,b) for a, b in zip(x, np.random.normal(0,0.1,len(x)))])
            msg = str({j : tdf.explode(['datetime','actual_velocity', 'setpoint_velocity', 'chain_position'])\
                          .drop(columns = ['batch_id', 'source_id'])\
                          .rename(columns = {'datetime' : 'ts'}).to_dict('records')}).encode("utf-8")
                           
            future = publisher.publish(topic_path, msg)
            
            sleep(randint(opts.min_data_latency, opts.max_data_latency))
    
    print('End of the process')
    
    
if __name__ == '__main__':
    run()
    
    
    
    
    
    