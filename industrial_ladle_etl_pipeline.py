import argparse
import time
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import DataflowRunner, DirectRunner

# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--tempLocation', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.stagingLocation
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('etl-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner
    options.view_as(StandardOptions).streaming = True
    
    # Static output BigQuery table
    output = '{0}:industrial_ladle_demo.ladle_hist'.format(opts.project)

    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {
                "name": "ts",
                "type": "TIMESTAMP"
            },
            {
                "name": "processing_dttm",
                "type": "TIMESTAMP"
            },            
            {
                "name": "batch_id",
                "type": "STRING"
            },
            {
                "name": "source_id",
                "type": "INTEGER"
            },
            {
                "name": "setpoint_velocity",
                "type": "FLOAT"
            },
            {
                "name": "actual_velocity",
                "type": "FLOAT"
            },
            {
                "name": "chain_position",
                "type": "FLOAT"
            }
        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)    
                
    def normalize_funct(data):
        import datetime
        import uuid
        batch_id  = uuid.uuid1().hex
        processing_dttm = datetime.datetime.now()
        return [dict(item, **{'source_id': list(data.keys())[0],
                              'processing_dttm' : processing_dttm,
                              'batch_id' : batch_id}) for item in list(data.values())[0]]
    
    (p 
        | "Read message from Pubsub" >> beam.io.ReadFromPubSub(
            topic= 'projects/iot-poc-354821/topics/industrial_ladle_data_generator',
            timestamp_attribute='ts'
            )
        | 'JSONParse' >> beam.Map(lambda x: eval(x.decode("utf-8")))
        | "Normalize" >> beam.ParDo(normalize_funct)
        | 'Write to BQ' >> beam.io.WriteToBigQuery(
            output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        ) 
            

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
  run()
