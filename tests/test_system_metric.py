import asyncio
from io import StringIO

import pandas as pd
import pytest

from util.system_metric import _normalize_dataframe, get_metric_async


@pytest.mark.asyncio
async def test_get_metric_async():
    metric_list = [
        "request_count",
        "request_latencies",
        "instance_count",
        "CPU_utilization",
        "memory_utilization",
        "startup_latency",
    ]

    results = await asyncio.gather(
        *(get_metric_async(metric, 1, 0, 0) for metric in metric_list),
        return_exceptions=True,
    )

    for metric, result in zip(metric_list, results):
        # Check if an error occurred
        if isinstance(result, Exception):
            assert False, f"An error occurred while getting the metric '{metric}':\n{result}"

        assert isinstance(result, pd.DataFrame), f"Expected a DataFrame for metric '{metric}', got another type."

        # Test converting DataFrame to JSON
        try:
            json_result = result.to_json()
        except Exception as e:
            assert False, f"Failed to convert DataFrame to JSON for metric '{metric}':\n{e}"

        # Test converting JSON back to DataFrame
        try:
            pd.read_json(StringIO(json_result))
        except Exception as e:
            assert False, f"Failed to convert JSON back to DataFrame for metric '{metric}':\n{e}"


def test_normalize_dataframe_fillna_and_aggregate():
    # Create a test DataFrame with NaN values and duplicated column names
    test_data = {
        "200": [None, 0, None, None, 1],
        "302": [0, None, 3, 0, 0],
        "503": [None, 0, 0, 2, None],
    }
    index = pd.to_datetime(
        [
            "2024-01-19 15:18:00",
            "2024-01-19 15:19:00",
            "2024-01-19 15:20:00",
            "2024-01-19 15:21:00",
            "2024-01-19 15:22:00",
        ]
    )
    df = pd.DataFrame(data=test_data, index=index)

    # Expected DataFrame after normalization
    expected_data = {"200": [.0, 0, .0, .0, 1], "302": [0, .0, 3, 0, 0], "503": [.0, 0, 0, 2, .0]}
    expected_df = pd.DataFrame(data=expected_data, index=index)

    # Normalize the DataFrame
    normalized_df = _normalize_dataframe(df)

    # Check if the normalized DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(normalized_df, expected_df)


def test_normalize_dataframe_with_mean_values():
    # Create a test DataFrame with mean values
    test_data = {"dvwa": [0.0, 0.381171, 0.035862, 0.034418, 0.034892]}
    index = pd.to_datetime(
        [
            "2024-01-19 15:14:00",
            "2024-01-19 15:15:00",
            "2024-01-19 15:16:00",
            "2024-01-19 15:17:00",
            "2024-01-19 15:18:00",
        ]
    )
    df = pd.DataFrame(data=test_data, index=index)

    # Expected DataFrame (in this case, it should be the same as input as no aggregation or NaN filling is needed)
    expected_df = df.copy()

    # Normalize the DataFrame
    normalized_df = _normalize_dataframe(df)

    # Check if the normalized DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(normalized_df, expected_df)


def test_normalize_dataframe_remove_seconds_microseconds():
    # Create a test DataFrame with precise time stamps
    test_data = {"data": [1, 2, 3, 4, 5]}
    index = pd.to_datetime(
        [
            "2024-01-19 15:14:23.123456",
            "2024-01-19 15:15:45.654321",
            "2024-01-19 15:16:56.789101",
            "2024-01-19 15:17:12.345678",
            "2024-01-19 15:18:34.987654",
        ]
    )
    df = pd.DataFrame(data=test_data, index=index)

    # Expected DataFrame after removing seconds and microseconds
    expected_index = pd.to_datetime(
        [
            "2024-01-19 15:14:00",
            "2024-01-19 15:15:00",
            "2024-01-19 15:16:00",
            "2024-01-19 15:17:00",
            "2024-01-19 15:18:00",
        ]
    )
    expected_df = pd.DataFrame(data=test_data, index=expected_index)

    # Normalize the DataFrame
    normalized_df = _normalize_dataframe(df)

    # Check if the DataFrame index matches the expected index (with seconds and microseconds removed)
    pd.testing.assert_index_equal(normalized_df.index, expected_df.index)
