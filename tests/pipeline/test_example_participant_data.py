import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_example_participant_data_df(example_participant_data_description, regression_test_df):
    regression_test_df(
        example_participant_data_description.drop("participant_data_source_file"),
        "participant_id",
        "example_participant_data",  # Check
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_example_participant_data_schema(regression_test_df_schema, example_participant_data_description):
    regression_test_df_schema(example_participant_data_description, "example_participant_data")  # check
