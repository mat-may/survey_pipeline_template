from cishouseholds.pipeline.mapping import column_name_maps
from cishouseholds.pipeline.mapping import example_participant_data_cast_to_double
from cishouseholds.pipeline.mapping import example_survey_response_data_cast_to_double
from cishouseholds.pipeline.pipeline_stages import generate_input_processing_function
from cishouseholds.pipeline.timestamp_map import example_participant_data_datetime_map
from cishouseholds.pipeline.timestamp_map import example_survey_response_data_v1_datetime_map
from cishouseholds.pipeline.timestamp_map import example_survey_response_data_v2_datetime_map
from cishouseholds.pipeline.timestamp_map import example_swab_sample_results_datetime_map
from cishouseholds.pipeline.validation_schema import validation_schemas
from cishouseholds.pipeline.version_specific_processing.example_participant_data import (
    transform_example_participant_extract,
)
from cishouseholds.pipeline.version_specific_processing.example_survey_response_data_v1 import (
    example_survey_response_data_v1_transformations,
)
from cishouseholds.pipeline.version_specific_processing.example_survey_response_data_v2 import (
    example_survey_response_data_v2_transformations,
)


example_participant_data_parameters = {
    "stage_name": "test_participant_data_ETL",
    "dataset_name": "test_participant_data",
    "id_column": "participant_id",
    "validation_schema": validation_schemas["example_participant_data_schema"],
    "column_name_map": column_name_maps["example_participant_data_name_map"],
    "datetime_column_map": example_participant_data_datetime_map,
    "transformation_functions": [
        transform_example_participant_extract,
    ],
    "sep": "|",
    "cast_to_double_list": example_participant_data_cast_to_double,
    "source_file_column": "participant_data_source_file",
}

example_survey_response_data_v1_parameters = {
    "stage_name": "test_survey_response_data_version_1_ETL",
    "dataset_name": "test_survey_response_data_version_1",
    "id_column": "participant_completion_window_id",
    "validation_schema": validation_schemas["example_survey_response_data_v1_schema"],
    "column_name_map": column_name_maps["example_survey_response_data_v1_name_map"],
    "datetime_column_map": example_survey_response_data_v1_datetime_map,
    "transformation_functions": [
        example_survey_response_data_v1_transformations,
    ],
    "sep": "|",
    "cast_to_double_list": example_survey_response_data_cast_to_double,
    "source_file_column": "survey_response_source_file",
    "survey_table": True,
}

example_survey_response_data_v2_parameters = {
    "stage_name": "test_survey_response_data_version_2_ETL",
    "dataset_name": "test_survey_response_data_version_2",
    "id_column": "participant_completion_window_id",
    "validation_schema": validation_schemas["example_survey_response_data_v2_schema"],
    "column_name_map": column_name_maps["example_survey_response_data_v2_name_map"],
    "datetime_column_map": example_survey_response_data_v2_datetime_map,
    "transformation_functions": [
        example_survey_response_data_v2_transformations,
    ],
    "sep": "|",
    "cast_to_double_list": example_survey_response_data_cast_to_double,
    "source_file_column": "survey_response_source_file",
    "survey_table": True,
}

example_swab_sample_results_parameters = {
    "stage_name": "test_swab_sample_results_ETL",
    "dataset_name": "test_swab_sample_results",
    "id_column": "Sample",
    "validation_schema": validation_schemas["example_swab_sample_results_schema"],
    "column_name_map": column_name_maps["example_swab_sample_results_name_map"],
    "datetime_column_map": example_swab_sample_results_datetime_map,
    "transformation_functions": [],
    "sep": ",",
    "cast_to_double_list": [],
    "source_file_column": "swab_results_source_file",
    "write_mode": "append",
}

for parameters in [
    example_participant_data_parameters,
    example_survey_response_data_v1_parameters,
    example_survey_response_data_v2_parameters,
    example_swab_sample_results_parameters,
]:
    generate_input_processing_function(**parameters)  # type:ignore
