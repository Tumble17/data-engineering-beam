# ====================================================================
# Links
# https://beam.apache.org/documentation/programming-guide/
# ====================================================================
# Notes
# - Combiners are inconsistently documented at
#   https://beam.apache.org/releases/pydoc/2.25.0/index.html and
#   https://beam.apache.org/documentation/programming-guide/#core-beam-transforms
#   such as beam.combiners in code examples but not in docs

# - No examples for many transforms available at
#   https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.transforms.html
# ====================================================================

# ====================================================================
# Packages
# ====================================================================
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils.timestamp import Timestamp

import json
import colouredlogs, logging

# ====================================================================
# Config
# ====================================================================
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
    # https://beam.apache.org/documentation/transforms/python/overview/
    # https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.transforms.html
    # ====================================================================
    # Purpose is to filter, format, extract and compute
    # General patterns of application:
    #   - Passing a function
    #       - Using multiple arguments
    #   - Using a lambda
    #   - Using side inputs
    #       - Singleton
    #       - Iterator
    #       - Dictionary
    # ===============================
    # Element-wise
    # ===============================
    # ==============
    # Filter
    # Examples: https://beam.apache.org/documentation/transforms/python/elementwise/filter/
    # API: https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.transforms.core.html#apache_beam.transforms.core.Filter
    # Given a predicate, filter out all elements that don’t satisfy that predicate.
    # May also be used to filter based on an inequality with a given value based on the comparison ordering of the element.
    # ==============
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

    logger.info(f"Created lines")
    # --------------------------------------------------------------------
    # Filtering with side inputs as dictionaries (in-memory only)
    # See demonstration example as real-world example counter-intuitive for this use case
    logger.info(f"Completed Filter")

    # Create word lists from lines for use in functions
    def map_to_word_lists(line):
        return list(line.split(" "))

    rick_morty_word_lists = rick_morty_lines | "Map to word lists" >> beam.Map(
        map_to_word_lists
    )

    # Create tuples for use in functions
    def map_to_tuples(line):
        for word in line.split(" "):
            return (word, 1)

    rick_morty_tuples = rick_morty_lines | "Map to tuples" >> beam.Map(map_to_tuples)

    logger.info(f"Created tuples")

    # ==============
    # FlatMap
    # Applies a simple 1-to-many mapping function over each element in the collection.
    # The many elements are flattened into the resulting collection.
    # Examples: https://beam.apache.org/documentation/transforms/python/elementwise/flatmap/
    # API: https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.transforms.core.html#apache_beam.transforms.core.FlatMap
    # ==============

    # --------------------------------------------------------------------
    # FlatMap with a predefined function
    rick_morty_words = (
        rick_morty_lines
        | "FlatMap with a predefined function" >> beam.FlatMap(str.split)
    )

    # --------------------------------------------------------------------
    # FlatMap with a function (PAS)
    # --------------------------------------------------------------------
    # FlatMap with a lambda (PAS)
    # --------------------------------------------------------------------
    # FlatMap with a generator
    def generate_words(word_list):
        for word in word_list:
            yield word

    rick_morty_words = (
        rick_morty_word_lists
        | "FlatMap with a generator" >> beam.FlatMap(generate_words)
    )

    # --------------------------------------------------------------------
    # FlatMapTuple for key-value pairs
    def index_words(key, value):
        cleaned_key = key.replace(",", "")
        yield "Key: {}, Value: {}".format(cleaned_key, value)

    rick_morty_word_tuples = (
        rick_morty_tuples
        | "FlatMapTuple for key-value pairs" >> beam.FlatMapTuple(index_words)
    )
    # --------------------------------------------------------------------
    # FlatMap with multiple arguments (PAS)
    # --------------------------------------------------------------------
    # FlatMap with side inputs as singletons (PAS)
    # --------------------------------------------------------------------
    # FlatMap with side inputs as iterators (PAS)
    # --------------------------------------------------------------------
    # FlatMap with side inputs as dictionaries (PAS)
    logger.info(f"Completed FlatMap")
    # ==============
    # Keys
    # Takes a collection of key-value pairs and returns the key of each element.
    # Examples: https://beam.apache.org/documentation/transforms/python/elementwise/keys/
    # API: https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.transforms.util.html#apache_beam.transforms.util.Keys
    # ==============
    rick_morty_keys = rick_morty_tuples | "Extracting Keys" >> beam.Keys()

    logger.info(f"Completed Keys")
    # ==============
    # KvSwap
    # Takes a collection of key-value pairs and returns a collection of key-value pairs which has each key and value swapped.
    # Examples: https://beam.apache.org/documentation/transforms/python/elementwise/kvswap/
    # API: https://beam.apache.org/releases/pydoc/2.25.0/_modules/apache_beam/transforms/util.html#KvSwap
    # ==============
    rick_morty_swapped_tuples = rick_morty_tuples | "Key-Value swap" >> beam.KvSwap()
    # ==============
    # Map
    # Applies a simple 1-to-1 mapping function over each element in the collection.
    # Examples: https://beam.apache.org/documentation/transforms/python/elementwise/map/
    # API: https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.transforms.core.html#apache_beam.transforms.core.Map
    # ==============
    # Map with a predefined function
    rick_morty_lower_case_words = (
        rick_morty_words | "Map with a predefined function" >> beam.Map(str.lower)
    )
    # --------------------------------------------------------------------
    # Map with a function (PAS)
    # --------------------------------------------------------------------
    # Map with a lambda (PAS)
    # --------------------------------------------------------------------
    # Map with multiple arguments (PAS)
    # --------------------------------------------------------------------
    # MapTuple for key-value pairs

    rick_morty_word_tuples = (
        rick_morty_tuples
        | "MapTuple for key-value pairs"
        >> beam.MapTuple(lambda key, value: "Key: {}, Value: {}".format(key, value))
    )

    # --------------------------------------------------------------------
    # Map with side inputs as singletons (PAS)
    # --------------------------------------------------------------------
    # Map with side inputs as iterators (PAS)
    # --------------------------------------------------------------------
    # Map with side inputs as dictionaries (PAS)

    # ==============
    # ParDo
    # A transform for generic parallel processing.
    # A ParDo transform considers each element in the input PCollection,
    # performs some processing function (your user code) on that element,
    # and emits zero or more elements to an output PCollection.
    # Examples: https://beam.apache.org/documentation/transforms/python/elementwise/pardo/
    # API: https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.transforms.core.html#apache_beam.transforms.core.ParDo
    # ==============
    # --------------------------------------------------------------------
    # ParDo with a simple DoFn
    # ParDo takes DoFn class inputs
    class SplitWordsDoFn(beam.DoFn):
        def __init__(self, delimiter=" "):
            self.delimiter = delimiter

        # Process is called once per element and it can yield zero or more outputs
        def process(self, line):
            for word in line.split(self.delimiter):
                yield word

    rick_morty_words = rick_morty_lines | "ParDo with a simple DoFn" >> beam.ParDo(
        SplitWordsDoFn(" ")
    )
    # --------------------------------------------------------------------
    # ParDo with timestamp and window information
    # DoFn API: https://beam.apache.org/releases/pydoc/2.25.0/_modules/apache_beam/transforms/core.html#DoFn
    # Timestamp API: https://beam.apache.org/releases/pydoc/2.18.0/_modules/apache_beam/utils/timestamp.html
    # Window API: https://beam.apache.org/releases/pydoc/2.25.0/_modules/apache_beam/transforms/window.html
    class AnalyzeElement(beam.DoFn):
        def process(
            self, elem, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam
        ):
            yield "\n".join(
                [
                    "# ===========",
                    "# ELEM",
                    "# ===========",
                    elem,
                    "# -----------",
                    "# TIMESTAMP",
                    "# -----------",
                    "type(timestamp) -> " + repr(type(timestamp)),
                    "timestamp.micros -> " + repr(timestamp.micros),
                    "timestamp.to_rfc3339() -> " + repr(timestamp.to_rfc3339()),
                    "timestamp.to_utc_datetime() -> "
                    + repr(timestamp.to_utc_datetime()),
                    "",
                    "# -----------",
                    "# WINDOW",
                    "# -----------",
                    "type(window) -> " + repr(type(window)),
                    "window.start -> {} ({})".format(
                        window.start, window.start.to_utc_datetime()
                    ),
                    "window.end -> {} ({})".format(
                        window.end, window.end.to_utc_datetime()
                    ),
                    "window.max_timestamp() -> {} ({})".format(
                        window.max_timestamp(), window.max_timestamp().to_utc_datetime()
                    ),
                    "# -----------",
                    "# ===========",
                    "\n"
                ]
            )

    rick_morty_analysis = (
        rick_morty_lines
        | "Add timestamps"
        >> beam.Map(lambda elem: beam.window.TimestampedValue(elem, Timestamp.now()))
        | "Place into 30 second windows"
        >> beam.WindowInto(beam.window.FixedWindows(30))
        | "Analyze element" >> beam.ParDo(AnalyzeElement())
        | beam.Map(print)
    )

    # ==============
    # Partition
    # ==============
    # ==============
    # Regex
    # ==============
    # ==============
    # Reify
    # ==============
    # ==============
    # ToString
    # ==============
    # ==============
    # WithTimestamps
    # ==============
    # ==============
    # Values
    # ==============
    # ===============================
    # Aggregation
    # ===============================
    # ==============
    # CoGroupByKey
    # ==============
    # ==============
    # CombineGlobally
    # ==============
    # ==============
    # CombinePerKey
    # ==============
    # ==============
    # CombineValues
    # ==============
    # ==============
    # Count
    # ==============
    # ==============
    # Distinct
    # ==============
    # ==============
    # GroupByKey
    # ==============
    # ==============
    # GroupIntoBatches
    # ==============
    # ==============
    # Latest
    # ==============
    # ==============
    # Max
    # ==============
    # ==============
    # Mean
    # ==============
    # ==============
    # Min
    # ==============
    # ==============
    # Sample
    # ==============
    # ==============
    # Sum
    # ==============
    # ==============
    # Top
    # ==============
    # ===============================
    # Other
    # ===============================
    # ==============
    # Create
    # ==============
    # ==============
    # Flatten
    # ==============
    # ==============
    # Reshuffle
    # ==============
    # ==============
    # WindowInto
    # ==============
    # --------------------------------------------------------------------

    # ===============================
    # Combine
    # ===============================
