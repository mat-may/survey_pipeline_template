import pytest

from survey_pipeline_template.merge import union_multiple_tables


@pytest.mark.regression
@pytest.mark.integration
def test_example_union(
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
    regression_test_df(
        example_union_df,
        "visit_id",
        "example_union_survey_response_files",
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_example_union_schema(
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
    regression_test_df_schema(
        example_union_df,
        "example_union_survey_response_files",
    )
