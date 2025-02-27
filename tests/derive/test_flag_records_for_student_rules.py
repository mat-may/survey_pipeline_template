from chispa import assert_df_equality

from survey_pipeline_template.regex.regex_flags import flag_records_for_school_v2_rules


def test_flag_records_for_school_v2_rules(spark_session):
    """Test flag_records_for_school_v2_rules function correctly flags the records"""

    test_cases = [
        (1, None, False),
        (5, 8, True),
        (12, None, False),
        (19, None, False),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases, schema="age_at_visit int, school_year int, actual_flag boolean"
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_school_v2_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
