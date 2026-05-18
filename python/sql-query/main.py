import pandas as pd
from pandasql import sqldf
import re
import time
import sys
import os
from google.protobuf.struct_pb2 import Struct, Value, ListValue
import json
import argparse
from dynamos.ms_init import NewConfiguration
from dynamos.signal_flow import signal_continuation, signal_wait
from dynamos.logger import InitLogger
from dynamos.tracer import InitTracer, pretty_print_span_context

from google.protobuf.empty_pb2 import Empty
import microserviceCommunication_pb2 as msCommTypes
import rabbitMQ_pb2 as rabbitTypes
import threading
import time
import sys
from opentelemetry.context.context import Context
from opentelemetry import trace


# --- DYNAMOS Interface code At the TOP ----------------------------------------------------
if os.getenv('ENV') == 'PROD':
    import config_prod as config
else:
    import config_local as config

logger = InitLogger()
tracer = InitTracer(config.service_name, config.tracing_host)
# Debugging for traces:
logger.debug(f"tracer host: {config.tracing_host}")
logger.debug(f"tracer: {tracer}")

# Events to start the shutdown of this Microservice, can be used to call 'signal_shutdown'
stop_event = threading.Event()
stop_microservice_condition = threading.Condition()

# Events to make sure all services have started before starting to process a message
# Might be overkill, but good practice
wait_for_setup_event = threading.Event()
wait_for_setup_condition = threading.Condition()

ms_config = None

STREAM_PARTIAL_METADATA_KEY = "stream_partial"
STREAM_FINAL_METADATA_KEY = "stream_final"
STREAM_SEQUENCE_METADATA_KEY = "stream_sequence"
STREAM_ROWS_PROCESSED_METADATA_KEY = "stream_rows_processed"
STREAM_ROWS_TOTAL_METADATA_KEY = "stream_rows_total"
STREAM_PROVIDER_METADATA_KEY = "stream_provider"
STREAM_BATCH_ID_METADATA_KEY = "stream_batch_id"
DEFAULT_STREAM_BATCH_ROWS = 5000
TABLE_NAME_PATTERN = re.compile(r'\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)', re.IGNORECASE)
LIMIT_PATTERN = re.compile(r'\bLIMIT\s+(\d+)\b', re.IGNORECASE)
UNSUPPORTED_AVERAGE_STREAM_PATTERN = re.compile(r'\b(?:GROUP\s+BY|ORDER\s+BY|HAVING|UNION|DISTINCT)\b', re.IGNORECASE)
UNIQUE_NUMBER_COLUMN = "Unieknr"
GENDER_COLUMN = "Geslacht"
SALARY_SCALE_COLUMN = "Salschal"

# --- END DYNAMOS Interface code At the TOP ----------------------------------------------------

#---- LOCAL TEST SETUP OPTIONAL!

# Go into local test code with flag '-t'
parser = argparse.ArgumentParser()
parser.add_argument("-t", "--test", action='store_true')
args = parser.parse_args()
test = args.test

#--------------------------------

# Start span using the span context created in the request handler
@tracer.start_as_current_span("load_and_query_csv")
def load_and_query_csv(file_path_prefix, query):
    # Extract table names from the query
    table_names = re.findall(r'FROM (\w+)', query) + re.findall(r'JOIN (\w+)', query)
    # Create a dictionary to hold DataFrames, keyed by table name
    dfs = {}
    DATA_STEWARD_NAME = os.getenv("DATA_STEWARD_NAME")
    if DATA_STEWARD_NAME == "":
        logger.error(f"DATA_STEWARD_NAME not set.")


    for table_name in table_names:
        try:
            file_name = f"{file_path_prefix}{table_name}_{DATA_STEWARD_NAME}.csv"
            logger.debug(f"Loading file {file_name}")
            dfs[table_name] = pd.read_csv(file_name, delimiter=';')
            logger.debug(f"after read csv")
        except FileNotFoundError:
            logger.error(f"CSV file for table {table_name}_{DATA_STEWARD_NAME} not found.")
            return None

    try:
        # Use pandasql's sqldf function to execute the SQL query
        result_df = sqldf(query, dfs)
    except Exception as e:
        logger.error(f"An error occurred while executing the query: {str(e)}")

    logger.debug(f"after result_df")

    return result_df

# Start span using the span context created in the request handler
@tracer.start_as_current_span("dataframe_to_protobuf")
def dataframe_to_protobuf(df):
    # Convert the DataFrame to a dictionary of lists (one for each column)
    data_dict = df.to_dict(orient='list')

    # Convert the dictionary to a Struct
    data_struct = Struct()

    # Iterate over the dictionary and add each value to the Struct
    for key, values in data_dict.items():
        # Pack each item of the list into a Value object
        value_list = [Value(string_value=str(item)) for item in values]
        # Pack these Value objects into a ListValue
        list_value = ListValue(values=value_list)
        # Add the ListValue to the Struct
        data_struct.fields[key].CopyFrom(Value(list_value=list_value))

    # Create the metadata
    # Infer the data types of each column
    data_types = df.dtypes.apply(lambda x: x.name).to_dict()
    # Convert the data types to string values
    metadata = {k: str(v) for k, v in data_types.items()}

    return data_struct, metadata

def get_stream_batch_rows():
    batch_rows = os.getenv("SQL_STREAM_BATCH_ROWS", str(DEFAULT_STREAM_BATCH_ROWS))
    try:
        parsed_batch_rows = int(batch_rows)
    except ValueError:
        logger.warning(f"Invalid SQL_STREAM_BATCH_ROWS value: {batch_rows}, falling back to {DEFAULT_STREAM_BATCH_ROWS}")
        return DEFAULT_STREAM_BATCH_ROWS

    if parsed_batch_rows <= 0:
        logger.warning(f"SQL_STREAM_BATCH_ROWS must be positive, got: {parsed_batch_rows}, falling back to {DEFAULT_STREAM_BATCH_ROWS}")
        return DEFAULT_STREAM_BATCH_ROWS

    return parsed_batch_rows

def stream_provider_name():
    data_steward_name = os.getenv("DATA_STEWARD_NAME", "").strip()
    if data_steward_name == "":
        return "unknown"
    return data_steward_name

def with_stream_metadata(metadata, sequence, rows_processed, rows_total, final):
    stream_metadata = dict(metadata)
    provider = stream_provider_name()
    stream_metadata[STREAM_PARTIAL_METADATA_KEY] = "false" if final else "true"
    stream_metadata[STREAM_FINAL_METADATA_KEY] = "true" if final else "false"
    stream_metadata[STREAM_SEQUENCE_METADATA_KEY] = str(sequence)
    stream_metadata[STREAM_ROWS_PROCESSED_METADATA_KEY] = str(rows_processed)
    stream_metadata[STREAM_ROWS_TOTAL_METADATA_KEY] = str(rows_total)
    stream_metadata[STREAM_PROVIDER_METADATA_KEY] = provider
    stream_metadata[STREAM_BATCH_ID_METADATA_KEY] = f"{provider}:{sequence}"
    return stream_metadata

def get_dataset_path(file_path_prefix, table_name):
    data_steward_name = os.getenv("DATA_STEWARD_NAME")
    if data_steward_name == "":
        logger.error("DATA_STEWARD_NAME not set.")
    return f"{file_path_prefix}{table_name}_{data_steward_name}.csv"

def build_average_stream_plan(query):
    if query is None or UNSUPPORTED_AVERAGE_STREAM_PATTERN.search(query):
        return None

    table_names = TABLE_NAME_PATTERN.findall(query)
    if len(table_names) < 2:
        return None

    personen_table = next((name for name in table_names if name.lower().startswith("personen")), None)
    appointments_table = next((name for name in table_names if name.lower().startswith("aanstellingen")), None)
    if personen_table is None or appointments_table is None:
        return None

    limit_match = LIMIT_PATTERN.search(query)
    if limit_match is None:
        return None

    try:
        limit = int(limit_match.group(1))
    except ValueError:
        return None

    if limit <= 0:
        return None

    return {
        "personen_table": personen_table,
        "appointments_table": appointments_table,
        "limit": limit,
    }

def empty_average_frame():
    return pd.DataFrame({
        GENDER_COLUMN: pd.Series(dtype="string"),
        SALARY_SCALE_COLUMN: pd.Series(dtype="string"),
    })

def build_person_lookup(file_path_prefix, personen_table):
    personen_path = get_dataset_path(file_path_prefix, personen_table)
    people_df = pd.read_csv(
        personen_path,
        delimiter=';',
        usecols=[UNIQUE_NUMBER_COLUMN, GENDER_COLUMN],
        dtype=str,
    )
    people_df = people_df.dropna(subset=[UNIQUE_NUMBER_COLUMN, GENDER_COLUMN])
    return dict(zip(people_df[UNIQUE_NUMBER_COLUMN], people_df[GENDER_COLUMN]))

def stream_average_batches(file_path_prefix, query):
    plan = build_average_stream_plan(query)
    if plan is None:
        return None

    people_by_id = build_person_lookup(file_path_prefix, plan["personen_table"])
    appointments_path = get_dataset_path(file_path_prefix, plan["appointments_table"])
    total_rows = plan["limit"]
    batch_rows = get_stream_batch_rows()
    rows_emitted = 0
    sequence = 0

    chunk_reader = pd.read_csv(
        appointments_path,
        delimiter=';',
        usecols=[UNIQUE_NUMBER_COLUMN, SALARY_SCALE_COLUMN],
        dtype=str,
        chunksize=batch_rows,
    )

    for chunk in chunk_reader:
        if rows_emitted >= total_rows:
            break

        chunk[UNIQUE_NUMBER_COLUMN] = chunk[UNIQUE_NUMBER_COLUMN].fillna("")
        chunk[SALARY_SCALE_COLUMN] = chunk[SALARY_SCALE_COLUMN].fillna("")
        chunk[GENDER_COLUMN] = chunk[UNIQUE_NUMBER_COLUMN].map(people_by_id)

        filtered = chunk.loc[
            chunk[GENDER_COLUMN].notna() & (chunk[SALARY_SCALE_COLUMN] != ""),
            [GENDER_COLUMN, SALARY_SCALE_COLUMN],
        ].copy()
        if filtered.empty:
            continue

        remaining_rows = total_rows - rows_emitted
        if len(filtered.index) > remaining_rows:
            filtered = filtered.iloc[:remaining_rows]

        rows_emitted += len(filtered.index)
        sequence += 1
        data, metadata = dataframe_to_protobuf(filtered)
        yield (
            data,
            with_stream_metadata(metadata, sequence, rows_emitted, total_rows, rows_emitted >= total_rows),
        )

        if rows_emitted >= total_rows:
            return

    if rows_emitted == 0:
        data, metadata = dataframe_to_protobuf(empty_average_frame())
        yield (data, with_stream_metadata(metadata, 1, 0, 0, True))
        return

    data, metadata = dataframe_to_protobuf(empty_average_frame())
    yield (data, with_stream_metadata(metadata, sequence + 1, rows_emitted, rows_emitted, True))

def process_sql_data_request(sqlDataRequest, ctx):
    global config
    logger.debug("Start process_sql_data_request")

    try:
        if sqlDataRequest.algorithm == "average":
            streamed_batches = stream_average_batches(config.dataset_filepath, sqlDataRequest.query)
            if streamed_batches is not None:
                yield from streamed_batches
                return

        result = load_and_query_csv(config.dataset_filepath, sqlDataRequest.query)
        logger.debug("after load and query csv")
        if result is None:
            return

        row_count = len(result.index)
        if sqlDataRequest.algorithm != "average":
            data, metadata = dataframe_to_protobuf(result)
            yield (data, metadata)
            return

        if row_count == 0:
            data, metadata = dataframe_to_protobuf(result)
            yield (data, with_stream_metadata(metadata, 1, 0, 0, True))
            return

        batch_rows = get_stream_batch_rows()
        for sequence, start in enumerate(range(0, row_count, batch_rows), start=1):
            end = min(start + batch_rows, row_count)
            batch_df = result.iloc[start:end]
            data, metadata = dataframe_to_protobuf(batch_df)
            yield (
                data,
                with_stream_metadata(metadata, sequence, end, row_count, end >= row_count),
            )
        return
    except FileNotFoundError:
        logger.error(f"File not found at path {config.dataset_filepath}")
        return
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return


# ---  DYNAMOS Interface code At the Bottom -----------------------------------------------------

def request_handler(msComm : msCommTypes.MicroserviceCommunication, ctx: Context):
    global ms_config
    logger.info(f"Received original request type: {msComm.request_type}")
    
    # Ensure all connections have finished setting up before processing data
    signal_wait(wait_for_setup_event, wait_for_setup_condition)

    try:
        if msComm.request_type == "sqlDataRequest":
            sqlDataRequest = rabbitTypes.SqlDataRequest()
            msComm.original_request.Unpack(sqlDataRequest)
            
            # Create a new span, using the context (ctx) passed to this function. In the background, the context 
            # (metadata that helps combine data into a single trace) is set in the dynamos-python-lib/dynamos/ms_init.py file
            # in the request_handler function (similar to the StartRemoteParentSpan() in tracing.go), which sets the 
            # context (as ctx) to use for the spans (subsequent spans will also use this context automatically)
            with tracer.start_as_current_span("process_sql_data_request", context=ctx) as parent_span:
                batch_count = 0
                for index, (data, metadata) in enumerate(process_sql_data_request(sqlDataRequest, ctx), start=1):
                    batch_count = index
                    logger.debug(f"Forwarding batch {index}, metadata: {metadata}")
                    ms_config.next_client.ms_comm.send_data(msComm, data, metadata)
                parent_span.set_attribute("sql_query.batch_count", batch_count)

            signal_continuation(stop_event, stop_microservice_condition)

        else:
            logger.error(f"An unknown request_type: {msComm.request_type}")

        return Empty()
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return Empty()


def main():
    global config
    global ms_config

    if test:
        logger.info("Running in test mode")
        return

    ms_config = NewConfiguration(config.service_name, config.grpc_addr, request_handler)

    # Signal the message handler that all connections have been created
    signal_continuation(wait_for_setup_event, wait_for_setup_condition)

    # Wait for the end of processing to shutdown this Microservice
    try:
        signal_wait(stop_event, stop_microservice_condition)

    except KeyboardInterrupt:
        print("KeyboardInterrupt received, stopping server...")
        signal_continuation(stop_event, stop_microservice_condition)


    ms_config.stop(2)
    logger.debug(f"Exiting {config.service_name}")
    sys.exit(0)

# ---  END DYNAMOS Interface code At the Bottom -------------------------------------------------



if __name__ == "__main__":
    main()
