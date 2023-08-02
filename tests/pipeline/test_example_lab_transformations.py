import pytest

from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.lab_transformations import lab_transformations


@pytest.mark.regression
@pytest.mark.integration
def test_example_lab_transformations(
    example_survey_response_data_v1_data_description,
    example_survey_response_data_v2_data_description,
    example_swab_sample_results_data_description,
    regression_test_df,
):
    example_survey_response_data_v1_df = example_survey_response_data_v1_data_description.drop(
        "survey_response_source_file"
    )  # remove source file column, as it varies for our temp dummy data
    example_survey_response_data_v2_df = example_survey_response_data_v2_data_description.drop(
        "survey_response_source_file"
    )  # remove source file column, as it varies for our temp dummy data
    example_union_df = union_multiple_tables([example_survey_response_data_v1_df, example_survey_response_data_v2_df])
    example_swab_results_df = example_swab_sample_results_data_description.drop("swab_results_source_file")
    processed_df = lab_transformations(example_union_df, example_swab_results_df)
    regression_test_df(
        processed_df.filter(~processed_df["visit_id"].isNull()),
        "visit_id",
        "example_lab_transformations",
    )  # remove source file column, as it varies for our temp dummy data


@pytest.mark.regression
@pytest.mark.integration
def test_example_lab_transformations_schema(
    example_survey_response_data_v1_data_description,
    example_survey_response_data_v2_data_description,
    example_swab_sample_results_data_description,
    regression_test_df_schema,
):
    example_survey_response_data_v1_df = example_survey_response_data_v1_data_description.drop(
        "survey_response_source_file"
    )  # remove source file column, as it varies for our temp dummy data
    example_survey_response_data_v2_df = example_survey_response_data_v2_data_description.drop(
        "survey_response_source_file"
    )  # remove source file column, as it varies for our temp dummy data
    example_union_df = union_multiple_tables([example_survey_response_data_v1_df, example_survey_response_data_v2_df])
    example_swab_results_df = example_swab_sample_results_data_description.drop("swab_results_source_file")
    processed_df = lab_transformations(example_union_df, example_swab_results_df)
    regression_test_df_schema(
        processed_df.filter(~processed_df["visit_id"].isNull()),
        "example_lab_transformations",
    )
