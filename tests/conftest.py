import json
import os
import random
import re
from pathlib import Path
from typing import Dict
from typing import List
from typing import Tuple

import numpy as np
import pandas as pd
import pytest
from mimesis.schema import Field
from mimesis.schema import Schema
from pandas import Timestamp
from pyspark.sql import SparkSession
from pytest_regressions.data_regression import RegressionYamlDumper

from dummy_data_generation.helpers import CustomRandom
from dummy_data_generation.helpers_weight import Distribution
from dummy_data_generation.schemas import get_example_participant_data_data_description
from dummy_data_generation.schemas import get_example_survey_response_data_v1_data_description
from dummy_data_generation.schemas import get_example_survey_response_data_v2_data_description
from dummy_data_generation.schemas import get_example_swab_sample_results_data_description
from survey_pipeline_template.hdfs_utils import copy_local_to_hdfs
from survey_pipeline_template.pipeline.input_file_stages import example_participant_data_parameters
from survey_pipeline_template.pipeline.input_file_stages import example_survey_response_data_v1_parameters
from survey_pipeline_template.pipeline.input_file_stages import example_survey_response_data_v2_parameters
from survey_pipeline_template.pipeline.input_file_stages import example_swab_sample_results_parameters
from survey_pipeline_template.pipeline.input_file_stages import generate_input_processing_function
from survey_pipeline_template.pyspark_utils import running_in_dev_test


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: mark integration tests (likely to run slowly)")
    config.addinivalue_line("markers", "regression: mark tests that include regression testing of output")


def timestamp_representer(dumper, timestamp):
    """Custom represented for using Timestamp type in data_regression testing"""
    return dumper.represent_data(timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"))


RegressionYamlDumper.add_custom_yaml_representer(Timestamp, timestamp_representer)


@pytest.fixture(scope="session")
def spark_session():
    """Session-wide SparkSession to optimise testing PySpark functions."""
    spark_session = (
        SparkSession.builder.master("local[*]")
        .config("spark.executor.memory", "6g")
        .config("spark.executor.cores", 3)
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", 3)
        .config("spark.sql.shuffle.partitions", 18)
        .config("spark.sql.crossJoin.enabled", "true")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def generate_barcodes(count=60, format="ONS########"):
    """
    Generate a set of random formatted barcodes

    `count` should equal the number of unique barcodes required across all survey versions e.g. 2 x 30 = 60
    """
    random.seed(1234)

    def repl(match):
        return str(random.randint(0, 9))

    barcodes = [re.sub("#", repl, format) for i in range(count)]
    random.seed(None)
    return barcodes


@pytest.fixture(scope="session")
def swab_barcodes():
    """
    Generate a set of random formatted barcodes

    `count` should equal the number of unique barcodes required across all survey versions e.g. 2 x 30 = 60
    """
    return generate_barcodes(60, format="SWT########")


@pytest.fixture(scope="session")
def example_participant_data_description(pandas_df_to_temporary_csv, swab_barcodes):
    """
    Generate dummy data for the example participant data.
    """
    schema = Schema(schema=get_example_participant_data_data_description(create_mimesis_field(), swab_barcodes))
    pandas_df = pd.DataFrame(schema.create(iterations=30))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep="|")
    processing_function = generate_input_processing_function(
        **example_participant_data_parameters, include_hadoop_read_write=False
    )
    processed_df = processing_function(resource_path=csv_file_path)
    return processed_df


@pytest.fixture(scope="session")
def example_survey_response_data_v1_data_description(pandas_df_to_temporary_csv, swab_barcodes):
    """
    Generate dummy data for the example survey responses data v1.
    """
    schema = Schema(schema=get_example_survey_response_data_v1_data_description(create_mimesis_field(), swab_barcodes))
    pandas_df = pd.DataFrame(schema.create(iterations=30))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep="|")
    processing_function = generate_input_processing_function(
        **example_survey_response_data_v1_parameters, include_hadoop_read_write=False
    )
    processed_df = processing_function(resource_path=csv_file_path)
    return processed_df


@pytest.fixture(scope="session")
def example_survey_response_data_v2_data_description(pandas_df_to_temporary_csv, swab_barcodes):
    """
    Generate dummy data for the example survey responses data v2.
    """
    schema = Schema(schema=get_example_survey_response_data_v2_data_description(create_mimesis_field(), swab_barcodes))
    pandas_df = pd.DataFrame(schema.create(iterations=30))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep="|")
    processing_function = generate_input_processing_function(
        **example_survey_response_data_v2_parameters, include_hadoop_read_write=False
    )
    processed_df = processing_function(resource_path=csv_file_path)
    return processed_df


@pytest.fixture(scope="session")
def example_swab_sample_results_data_description(pandas_df_to_temporary_csv, swab_barcodes):
    """
    Generate dummy data for example swab sample results
    """
    schema = Schema(schema=get_example_swab_sample_results_data_description(create_mimesis_field(), swab_barcodes))
    pandas_df = pd.DataFrame(schema.create(iterations=60))
    csv_file_path = pandas_df_to_temporary_csv(pandas_df, sep=",")
    processing_function = generate_input_processing_function(
        **example_swab_sample_results_parameters, include_hadoop_read_write=False
    )
    processed_df = processing_function(resource_path=csv_file_path)
    return processed_df


@pytest.fixture(scope="session")
def pandas_df_to_temporary_csv(tmpdir_factory, spark_session):
    """Provides a function to write a pandas dataframe to a temporary csv file with function scope."""

    def _pandas_df_to_temporary_csv(pandas_df, sep=",", filename="temp.csv", header=True):
        in_dev_test = running_in_dev_test()
        temporary_csv_path = Path(tmpdir_factory.mktemp("data") / filename)
        pandas_df.to_csv(temporary_csv_path, sep=sep, header=header, index=False, na_rep="")
        _temporary_csv_path = str(temporary_csv_path.as_posix()).lstrip("/")
        if in_dev_test:
            # copy the csv file from local dir to hdfs when running in DevTest env
            # using spark's temp dir to ensure unique tmp location for each run
            hdfs_filepath = os.path.join(
                spark_session.sparkContext._temp_dir,
                _temporary_csv_path,
            )
            copy_local_to_hdfs(temporary_csv_path, hdfs_filepath)
            return f"hdfs://{hdfs_filepath}"
        else:
            return f"file:///{_temporary_csv_path}"

    return _pandas_df_to_temporary_csv


@pytest.fixture
def regression_test_df(data_regression):
    """
    Regression test both the schema and data from a PySpark dataframe

    Parameters
    ----------
    sort_by
        unique ID column to sort by. Must not contain nulls
    test_file_label
        text used to label regression test outputs
    """

    def _regression_test_df(df, sort_by, test_file_label):
        assert df.filter(df[sort_by].isNull()).count() == 0
        pandas_df = df.toPandas().replace({np.nan: None})  # Convert nan and nat to null
        data_regression.check(
            pandas_df.sort_values(sort_by, axis="index").to_dict("records"), f"{test_file_label}_data_regression"
        )

    return _regression_test_df


@pytest.fixture
def regression_test_df_schema(data_regression):
    """
    Regression test both the schema and data from a PySpark dataframe

    Parameters
    ----------
    sort_by
        unique ID column to sort by. Must not contain nulls
    test_file_label
        text used to label regression test outputs
    """

    def _regression_test_df(df, test_file_label):
        data_regression.check(json.loads(df.schema.json()), f"{test_file_label}_schema_regression")

    return _regression_test_df


def create_mimesis_field():
    """Generate a new field for mimesis data generation, to ensure test data are independently generated"""
    return Field("en-gb", seed=42, providers=[Distribution, CustomRandom])


@pytest.fixture
def prepare_regex_test_cases():
    def _prepare_regex_test_cases(test_data: Dict[str, List[str]]) -> List[Tuple[str, bool]]:
        """Convenience function to prepare regex test cases.

        Parameters
        ----------
        test_data:
            A dictionary with two keys, "positive" & "negative" & a list of strings as values
        """
        test_data_melted = [
            (test_case, pos_or_neg == "positive")
            for pos_or_neg, test_cases in test_data.items()
            for test_case in test_cases
        ]
        return test_data_melted

    return _prepare_regex_test_cases
