import pytest


@pytest.mark.regression
@pytest.mark.integration
def test_example_swab_results_df(example_swab_sample_results_data_description, regression_test_df):
    regression_test_df(
        example_swab_sample_results_data_description.drop("swab_results_source_file"),
        "pcr_result_recorded_datetime",
        "example_swab_results",
    )


@pytest.mark.regression
@pytest.mark.integration
def test_example_swab_results_schema(regression_test_df_schema, example_swab_sample_results_data_description):
    regression_test_df_schema(example_swab_sample_results_data_description, "example_swab_results")
