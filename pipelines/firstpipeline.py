# Links
# https://beam.apache.org/documentation/programming-guide/

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import json
import colouredlogs, logging

logger = logging.getLogger(__name__)

colouredlogs.install(level="INFO", logger=logger) # Logger used to ignore other libraries

# Pipeline - A Pipeline encapsulates your entire data processing task, from start to finish.
# This includes reading input data, transforming that data, and writing output data.
# All Beam driver programs must create a Pipeline.
# When you create the Pipeline, you must also specify the execution options that tell the Pipeline where and how to run.
# https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.pipeline.html
with beam.Pipeline(runner=None, options=PipelineOptions(), argv=None) as p:

    # Runners
    # https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.runners.html
    # dataflow, direct, interactive, internal, job
    logger.debug(p.runner)

    # Options
    # https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.options.pipeline_options.html
    # PipelineOptions, 
    # StandardOptions, 
    # TypeOptions, 
    # DirectOptions, 
    # GoogleCloudOptions, 
    # HadoopFileSystemOptions, 
    # WorkerOptions, 
    # DebugOptions, 
    # ProfilingOptions, 
    # SetupOptions, 
    # TestOptions
    logger.debug(json.dumps(p._options.get_all_options(), indent=4))

    # Or pass None and a custom argv key value pair object
    # Custom options
    # https://beam.apache.org/documentation/programming-guide/#configuring-pipeline-options

    # Sources and Sinks
    # https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.io.html
    # aws, [s3 (boto3, fake, messages), s3filesystem, s3io]
    # azure, [blobstoragefilesystem, blobstorageio]
    # external, [gcp pubsub, generate_sequence, kafka, snowflake]
    # flink, [flink_streaming_impulse_source]
    # gcp [
    #       datastore, 
    #       experimental (spannerio),
    #       bigquery,
    #       bigtable,
    #       gcs,
    #       pubsub
    #     ]
    # avro, file, filesystem, hadoopfilesystem, jdbc, kafka, kinesis, localfilesystem, mongodb, parquet, snowflake, text, TF

    # PCollections
    # https://beam.apache.org/documentation/programming-guide/#pcollections

    season1_lines = (
        p
        | beam.io.ReadFromText(file_pattern="inputs/rickandmorty/season1*.txt") # https://beam.apache.org/releases/pydoc/2.25.0/_modules/apache_beam/io/textio.html#ReadFromText
    )

    logger.info(f"Season 1 Lines Object:{season1_lines}")

