import pytest

from survey_pipeline_template.merge import union_multiple_tables
from survey_pipeline_template.pipeline.covid_event_transformations import covid_event_transformations


@pytest.mark.regression
@pytest.mark.integration
def test_example_covid_transformations(
    example_survey_response_data_v1_data_description,
    example_survey_response_data_v2_data_description,
    regression_test_df,
):
    example_survey_response_data_v1_df = example_survey_response_data_v1_data_description.drop(
        "survey_response_source_file"
    )  # remove source file column, as it varies for our temp dummy data
    example_survey_response_data_v2_df = example_survey_response_data_v2_data_description.drop(
        "survey_response_source_file"
    )  # remove source file column, as it varies for our temp dummy data
    example_union_df = union_multiple_tables([example_survey_response_data_v1_df, example_survey_response_data_v2_df])
    processed_df = covid_event_transformations(example_union_df)
    regression_test_df(
        processed_df,
        "visit_id",
        "example_covid_transformations",
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_example_covid_transformations_schema(
    example_survey_response_data_v1_data_description,
    example_survey_response_data_v2_data_description,
    regression_test_df_schema,
):
    example_survey_response_data_v1_df = example_survey_response_data_v1_data_description.drop(
        "survey_response_source_file"
    )  # remove source file column, as it varies for our temp dummy data
    example_survey_response_data_v2_df = example_survey_response_data_v2_data_description.drop(
        "survey_response_source_file"
    )  # remove source file column, as it varies for our temp dummy data
    example_union_df = union_multiple_tables([example_survey_response_data_v1_df, example_survey_response_data_v2_df])
    processed_df = covid_event_transformations(example_union_df)
    regression_test_df_schema(
        processed_df,
        "example_covid_transformations",
    )
