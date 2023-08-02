from chispa import assert_df_equality

from survey_pipeline_template.impute import edit_multiple_columns_fill_forward


def test_edit_multiple_columns_fill_forward(spark_session):
    schema = """
            id integer,
            received string,
            date string,
            n_doses integer,
            type string,
            type_other string
        """
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1,     "yes",  "2020-01-01",       4,      "not_known",    "not_known_2"),
                (1,     "yes",  "2020-01-02",       4,      "not_known",    "not_known_2"),  # case when all null

                (2,     "yes",  "2020-01-01",       4,      "not_known",    "type_a"),
                (2,     "yes",  "2020-01-02",       4,      "type_b",       "type_a"),  # case when some null from 1 row

                (3,     "yes",  "2020-01-01",       4,      "not_known",    "type_a"),
                (3,     "yes",  "2020-01-02",       None,   "not_known",    "type_b"),
                (3,     "yes",  "2020-01-03",       4,      "not_known",    "type_b"),  # case when some null from multiple row

                (4,     "yes",  "2020-01-01",       4,      "not_known",    "not_known_2"),
                (4,     None,     None,             4,      None,           "not_known_2"),
            # fmt: on
        ],
        schema=schema,
    )

    df_input = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1,     "yes",    '2020-01-01',     4,          "not_known",    "not_known_2"),
                (1,     None,     '2020-01-02',     None,       None,           None), # case when all null

                (2,     "yes",    '2020-01-01',     4,          "not_known",    "type_a"),
                (2,     None,     '2020-01-02',     None,       "type_b",       None), # case when some null from 1 row

                (3,     "yes",    '2020-01-01',     4,          "not_known",    "type_a"),
                (3,     "yes",    '2020-01-02',     None,       "not_known",    "type_b"),
                (3,     None,     '2020-01-03',     None,       "not_known",    None), # case when some null from multiple row

                (4,     "yes",    '2020-01-01',     4,          "not_known",    "not_known_2"),
                (4,     None,     None,             4,          None,           "not_known_2"),
            # fmt: on
        ],
        schema=schema,
    )
    actual_df = edit_multiple_columns_fill_forward(
        df=df_input,
        id="id",
        fill_if_null="received",
        date="date",
        column_fillforward_list=["n_doses", "type", "type_other", "received"],
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
