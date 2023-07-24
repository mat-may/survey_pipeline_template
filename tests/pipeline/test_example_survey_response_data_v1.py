import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_example_survey_response_data_v1_df(example_survey_response_data_v1_data_description, regression_test_df):
    regression_test_df(
        example_survey_response_data_v1_data_description.drop("survey_response_source_file"),
        "participant_completion_window_id",
        "example_survey_response_data_v1",
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_example_survey_response_data_v1_schema(
    regression_test_df_schema, example_survey_response_data_v1_data_description
):
    regression_test_df_schema(
        example_survey_response_data_v1_data_description, "example_survey_response_data_v1"
    )  # check
