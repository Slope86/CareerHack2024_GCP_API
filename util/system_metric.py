import asyncio
import os
from datetime import datetime

import pandas as pd
import pytz
from dotenv import load_dotenv
from google.cloud.monitoring_v3 import MetricServiceClient
from google.cloud.monitoring_v3.query import Query


def get_metric(metric: str, days: int = 0, hours: int = 0, minutes: int = 0) -> pd.DataFrame:
    """
    Retrieves specified metrics for a Google Cloud Run service over a specified time range.

    This function queries Google Cloud Monitoring for metrics related to a Cloud Run service. It supports various
    metrics like request counts, latencies, instance counts, CPU and memory utilization, etc. The time range for
    the query can be specified in days, hours, and minutes.

    Args:
        metric (str): The specific metric to retrieve. Valid options include 'request_count', 'request_latencies',
                      'instance_count', 'CPU_utilization', 'memory_utilization', 'startup_latency'.
        days (int): The number of days to go back in time for the metric data.
        hours (int): The number of hours to go back in time for the metric data.
        minutes (int): The number of minutes to go back in time for the metric data.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the queried metric data.

    Note:
        The function requires the 'PROJECT_ID' environment variable to be set, which specifies the GCP project ID.
        If not set, it defaults to 'tsmccareerhack2024-icsd-grp1'.
    """
    # if all values are 0, return empty dataframe
    if days == 0 and hours == 0 and minutes == 0:
        return pd.DataFrame()
    load_dotenv(override=True)
    PROJECT_ID: str = os.getenv("PROJECT_ID", default="tsmccareerhack2024-icsd-grp1")
    SERVER_NAME: str = os.getenv("SERVER_NAME", default="dvwa")
    metrics_info: dict[str, dict[str, str]] = {
        "request_count": {
            "type": "run.googleapis.com/request_count",
            "label": "response_code",
        },
        "request_latencies": {
            "type": "run.googleapis.com/request_latencies",
            "label": "response_code",
        },
        "instance_count": {
            "type": "run.googleapis.com/container/instance_count",
            "label": "state",
        },
        "CPU_utilization": {
            "type": "run.googleapis.com/container/cpu/utilizations",
            "label": "service_name",
        },
        "memory_utilization": {
            "type": "run.googleapis.com/container/memory/utilizations",
            "label": "service_name",
        },
        "startup_latency": {
            "type": "run.googleapis.com/container/startup_latencies",
            "label": "service_name",
        },
    }

    client: MetricServiceClient = MetricServiceClient()
    query: Query = Query(
        client=client,
        project=PROJECT_ID,
        metric_type=metrics_info[metric]["type"],
        end_time=datetime.utcnow().replace(tzinfo=pytz.timezone("UTC")),
        days=days,
        hours=hours,
        minutes=minutes,
    ).select_resources(service_name=SERVER_NAME)

    result: pd.DataFrame = query.as_dataframe(label=metrics_info[metric]["label"])

    return _normalize_dataframe(result, metric)


def _normalize_dataframe(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """
    Normalizes and aggregates a DataFrame with potentially duplicated column names.

    This function first fills any NaN values with 0.0. It then standardizes the data by averaging values across
    multiple time points if necessary. Finally, it handles duplicated column names by transposing the DataFrame,
    grouping by column names, summing the values, and transposing back. This method is chosen to avoid the
    deprecation warning associated with using DataFrame.groupby with axis=1.

    Args:
        df (pd.DataFrame): The DataFrame to be normalized and aggregated.

    Returns:
        pd.DataFrame: The normalized and aggregated DataFrame.
    """
    # Fill NaN values with 0.0
    df = df.fillna(0.0)

    # remove seconds and microseconds from the index
    df.index = df.index.map(lambda x: x.replace(second=0, microsecond=0))

    # Standardize the data (e.g., averaging values across multiple time points)
    for index, row in df.iterrows():
        for column_name, value in row.items():
            if hasattr(value, "mean"):
                df.loc[index, column_name] = value.mean  # type: ignore

    # Transpose the DataFrame, group by the column names and sum the values, and then transpose back
    # This approach is used to handle the deprecation warning for DataFrame.groupby with axis=1
    match metric:
        case "request_count" | "request_latencies":
            df = df.T.groupby(level=0).sum().T
        case "CPU_utilization" | "memory_utilization":
            df = df.T.groupby(level=0).max().T
            # convert to percentage
            df = df * 100
        case "startup_latency" | "instance_count":
            df = df.T.groupby(level=0).max().T
        case _:
            df = df.T.groupby(level=0).sum().T
    return df


async def get_metric_async(metric: str, days: int = 0, hours: int = 0, minutes: int = 0) -> pd.DataFrame:
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, get_metric, metric, days, hours, minutes)
    if isinstance(result, Exception):
        raise result
    if isinstance(result, pd.DataFrame):
        return result
    raise Exception(f"Unexpected result type: {type(result)}")


async def get_all_metrics(days: int = 0, hours: int = 0, minutes: int = 0) -> dict[str, str]:
    metric_list = [
        "request_count",
        "request_latencies",
        "instance_count",
        "CPU_utilization",
        "memory_utilization",
        "startup_latency",
    ]

    results = await asyncio.gather(
        *(get_metric_async(metric, days, hours, minutes) for metric in metric_list),
        return_exceptions=True,
    )

    response = {}
    for metric, result in zip(metric_list, results):
        if isinstance(result, Exception):
            response[metric] = f"An error occurred while getting the metric '{metric}':\n{result}"
        elif isinstance(result, pd.DataFrame):
            response[metric] = result.to_json()
        else:
            response[metric] = f"Unexpected result type: {type(result)}"

    return response
