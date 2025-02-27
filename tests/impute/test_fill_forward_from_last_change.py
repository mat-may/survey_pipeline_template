from chispa import assert_df_equality

from survey_pipeline_template.edit import update_to_value_if_any_not_null
from survey_pipeline_template.impute import fill_forward_from_last_change

# from survey_pipeline_template.edit import update_travel_column


def test_fill_forward_from_last_change(spark_session):
    schema = "participant_id integer, visit_datetime string, been_outside_uk_last_country string, been_outside_uk_last_return_date string, been_outside_uk string"
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # checking that if been_outside_uk_last_country and been_outside_uk_last_return_date are different, been_outside_uk has to turn into "yes"
            (1, "2021-01-01", "CountryA", "2020-09-01", None),
            (1, "2021-01-02", None, None, "No"),

            (2, "2021-01-01", None, None, "No"),
            (2, "2021-01-02", None, None, None),
            (2, "2021-01-11", "CountryB", "2020-10-20", "Yes"), # half way through fill forward
            (2, "2021-01-12", None, None, None),

            (3, "2021-01-01", "CountryC", "2020-09-30", None),
            (3, "2021-01-03", None, None, "No"),
            (3, "2021-01-04", None, None, None),

            (4, "2021-01-01", None, None, "No"),
            (4, "2021-01-02", "CountryD", "2020-03-01", "Yes"),
            (4, "2021-01-03", None, None, None),

            (5, "2021-01-02", "CountryD", None, None), # date lacking
            (5, "2021-01-03", None, None, None),

            (6, "2021-01-02", None, "2020-03-01", None), # country lacking
            (6, "2021-01-03", None, None, None),

            (7, "2020-01-01", None, None, None),
            (7, "2021-01-02", "CountryA", "2021-01-01", "Yes"),
            (7, "2021-01-03", "CountryA", None, None),
            # fmt: on
        ],
        schema=schema,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1, "2021-01-01", "CountryA", "2020-09-01", "Yes"),
            (1, "2021-01-02", "CountryA", "2020-09-01", "Yes"),

            (2, "2021-01-01", None, None, "No"),
            (2, "2021-01-02", None, None, "No"),
            (2, "2021-01-11", "CountryB", "2020-10-20", "Yes"),
            (2, "2021-01-12", "CountryB", "2020-10-20", "Yes"),

            (3, "2021-01-01", "CountryC", "2020-09-30", "Yes"),
            (3, "2021-01-03", "CountryC", "2020-09-30", "Yes"),
            (3, "2021-01-04", "CountryC", "2020-09-30", "Yes"),

            (4, "2021-01-01", None, None, "No"),
            (4, "2021-01-02", "CountryD", "2020-03-01", "Yes"),
            (4, "2021-01-03", "CountryD", "2020-03-01", "Yes"),

            (5, "2021-01-02", "CountryD", None, "Yes"),
            (5, "2021-01-03", "CountryD", None, "Yes"),

            (6, "2021-01-02", None, "2020-03-01", "Yes"),
            (6, "2021-01-03", None, "2020-03-01", "Yes"),

            (7, "2020-01-01", None, None, "No"),
            (7, "2021-01-02", "CountryA", "2021-01-01", "Yes"),
            (7, "2021-01-03", "CountryA", None, "Yes"),
            # fmt: on
        ],
        schema=schema,
    )
    temp_df = update_to_value_if_any_not_null(
        df=input_df,
        column_name_to_update="been_outside_uk",
        column_list=["been_outside_uk_last_country", "been_outside_uk_last_return_date"],
        default_values=["Yes", "No"],
    )
    output_df = fill_forward_from_last_change(
        df=temp_df,
        fill_forward_columns=[
            "been_outside_uk_last_country",
            "been_outside_uk_last_return_date",
            "been_outside_uk",
        ],
        participant_id_column="participant_id",
        event_datetime_column="visit_datetime",
        record_changed_column="been_outside_uk",
        record_changed_value="Yes",
    )
    assert_df_equality(output_df, expected_df, ignore_row_order=False, ignore_column_order=True)
