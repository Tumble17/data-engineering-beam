# Links
# https://beam.apache.org/documentation/programming-guide/

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import json
import colouredlogs, logging

logger = logging.getLogger(__name__)

colouredlogs.install(
    level="INFO", logger=logger
)  # Logger used to ignore other libraries

# Pipeline - A Pipeline encapsulates your entire data processing task, from start to finish.
# This includes reading input data, transforming that data, and writing output data.
# All Beam driver programs must create a Pipeline.
# When you create the Pipeline, you must also specify the execution options that tell the Pipeline where and how to run.
# https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.pipeline.html
with beam.Pipeline(runner=None, options=PipelineOptions(), argv=None) as p:

    # ====================================================================
    # Runners
    # https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.runners.html
    # ====================================================================
    # dataflow, direct, interactive, internal, job
    logger.debug(p.runner)

    # ====================================================================
    # Options
    # https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.options.pipeline_options.html
    # ====================================================================
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

    # ====================================================================
    # Sources and Sinks
    # https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.io.html
    # ====================================================================
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

    # ====================================================================
    # PCollections
    # https://beam.apache.org/documentation/programming-guide/#pcollections
    # https://beam.apache.org/documentation/programming-guide/#pcollection-characteristics
    # ====================================================================
    # type - May be of any type but must all be of the same type. Encoded to byte string.
    # schema - Structure that can be introspected
    # immutability
    # random access - Not supported
    # size and boundedness - No upper limit. Bounded or unbounded. Windowing for dividing unbounded into logical windows or finite size. Often uses timestamp.
    # timestamps - Intrinsic
    # https://beam.apache.org/releases/pydoc/2.25.0/_modules/apache_beam/pvalue.html#PCollection

    season1_lines = p | beam.io.ReadFromText(
        file_pattern="inputs/rickandmorty/season1*.txt"
    )  # https://beam.apache.org/releases/pydoc/2.25.0/_modules/apache_beam/io/textio.html#ReadFromText

    logger.info(f"Season 1 Lines Object: {season1_lines}")

    # ====================================================================
    # Transforms
    # https://beam.apache.org/documentation/programming-guide/#transforms
    # ====================================================================

    # ParDo
    # (Map) - Considers each element in in the input PCollection, performs user code and emits zero, one or multiple elements to an output PCollection
    # User code in the form of a DoFn object.

    # --------------------------------------------------------------------
    # Creating a custom DoFn
    class ComputeLengthFn(beam.DoFn):
        def process(self, element):
            return [len(element)]

    line_lengths = season1_lines | beam.ParDo(ComputeLengthFn())
    # --------------------------------------------------------------------
    # Passing a function
    def compute_length(element):
        return [len(element)]

    line_lengths = season1_lines | beam.Map(lambda x: compute_length(x))
    # --------------------------------------------------------------------
    # Lightweight DoFns
    line_lengths = season1_lines | beam.Map(lambda x: [len(x)])
    # --------------------------------------------------------------------
    # ParDo for one-to-one mapping
    line_lengths = season1_lines | beam.Map(len)
    # --------------------------------------------------------------------
    # Map is one-to-one, FlatMap can produce zero or more outputs, ParDo is generalised to include multiple outputs and side-inputs
    # --------------------------------------------------------------------
    logger.info(f"Completed basic functions")

    # ====================================================================
    # Filter
    # https://beam.apache.org/documentation/transforms/python/elementwise/filter/
    # ====================================================================
    # Filtering with a function
    def has_rick_or_morty(element):
        patterns = ["rick", "morty"]
        return any([True for pattern in patterns if pattern in element.lower()])

    rick_morty_lines = season1_lines | "Filter with Function" >> beam.Filter(
        has_rick_or_morty
    )
    # --------------------------------------------------------------------
    # Filtering with a lambda
    rick_morty_lines = season1_lines | "Filter with Lambda" >> beam.Filter(
        lambda line: any(
            [True for pattern in ["rick", "morty"] if pattern in line.lower()]
        )
    )
    # --------------------------------------------------------------------
    # Filtering with multiple arguments
    def has_pattern(element, patterns):
        return any([True for pattern in patterns if pattern in element.lower()])

    rick_morty_lines = season1_lines | "Filter with Multi Args" >> beam.Filter(
        has_pattern, patterns=["rick", "morty"]
    )
    # --------------------------------------------------------------------
    # Filtering with side inputs as singletons (single value PCollection)
    rick_pattern_singleton = p | "Rick Singleton" >> beam.Create(["rick"])
    rick_lines = season1_lines | "Filter with Singleton Side Input" >> beam.Filter(
        lambda line, pattern: True if pattern in line.lower() else False,
        pattern=beam.pvalue.AsSingleton(rick_pattern_singleton),
    )
    # --------------------------------------------------------------------
    # Filtering with side inputs as iterators (multiple values)
    rick_and_morty_iterator = p | "Rick and Morty Iterator" >> beam.Create(
        ["rick", "morty"]
    )
    rick_morty_lines = season1_lines | "Filter with Iterator Side Input" >> beam.Filter(
        lambda line, patterns: any(
            [True for pattern in patterns if pattern in line.lower()]
        ),
        patterns=beam.pvalue.AsIter(rick_and_morty_iterator),
    )
    # --------------------------------------------------------------------
    # Filtering with side inputs as dictionaries (in-memory only)
    # See demonstration example as real-world example counter-intuitive for this use case
    logger.info(f"Completed filters")

    # --------------------------------------------------------------------
    # ====================================================================
    # Format
    # ====================================================================
    # --------------------------------------------------------------------
    # ====================================================================
    # Extract
    # ====================================================================
    # --------------------------------------------------------------------
    # ====================================================================
    # Computation
    # ====================================================================
    # --------------------------------------------------------------------
