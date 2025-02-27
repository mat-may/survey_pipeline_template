import re
from functools import reduce
from itertools import chain
from operator import add
from operator import and_
from operator import or_
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Union

from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from survey_pipeline_template.edit import update_column_values_from_map
from survey_pipeline_template.expressions import all_equal
from survey_pipeline_template.expressions import all_equal_or_null
from survey_pipeline_template.expressions import any_column_matches_regex
from survey_pipeline_template.expressions import any_column_not_null
from survey_pipeline_template.merge import null_safe_join
from survey_pipeline_template.pyspark_utils import get_or_create_spark_session


def combine_like_array_columns(df: DataFrame, column_prefix: str):
    """
    Combine all columns with a given prefix into a single string column with
    name of the 'column_prefix'. subsequently drop all the columns used to create the combined column.

    Parameters
    ----------
    df
    column_prefix
    """
    cols = [col for col in df.columns if col.startswith(column_prefix)]
    return df.withColumn(column_prefix, F.concat(*[F.col(col) for col in cols])).drop(
        *[col for col in cols if col != column_prefix]
    )


def assign_columns_from_array(
    df: DataFrame,
    array_column_name: str,
    true_false_values: List[Any],
    prefix: Any = None,
    column_name_map: Dict[str, str] = None,
):
    """
    Convert an array column into a series of columns, optionally apply a prefix to the value in the array
    before generating the column name.

    Parameters
    ----------
    df
    array_column_name
        the name of the array column to split
    prefix
        an optional prefix to apply to each name in the array
    true_false_values
        [<true value>,<false value>]
    """
    # array values become rows
    df = df.withColumn("exploded", F.explode(array_column_name))

    if column_name_map:
        df = update_column_values_from_map(df, "exploded", column_name_map)

    if prefix:
        df = df.withColumn("exploded", F.concat_ws("_", F.lit(prefix), F.col("exploded")))

    df = df.withColumn("exploded", F.lower(F.regexp_replace(F.col("exploded"), r"[^a-zA-Z0-9]{1,}", "_")))

    df = df.withColumn("value", F.lit(true_false_values[0]))
    df = (
        df.groupby(*[col for col in df.columns if col not in ["exploded", "value"]])
        .pivot("exploded")
        .agg(F.first("value"))
        .fillna(true_false_values[1])
    )
    return df.drop("exploded", "value")


def assign_datetime_from_combined_columns(
    df: DataFrame,
    column_name_to_assign: str,
    date_column: str,
    am_pm_column: str,
    hour_column: str = None,
    minute_column: str = None,
    second_column: str = None,
):
    """
    Create a formatted pyspark date column from a series of components.

    Parameters
    ----------
    df
    column_name_to_assign
    date_column
    am_pm_column
    hour_column
        an optional name for a column containing a 1-12 number representing the hour
        if this column is not specified the hour will default to 0
    minute_column
        an optional name for a column containing a 1-12 number representing the minute
        if this column is not specified the minute will default to 0
    second_column
        an optional name for a column containing a 1-12 number representing the second
        if this column is not specified the second will default to 0
    """
    for col_name, temp_col_name in zip([hour_column, minute_column, second_column], ["_hour", "_min", "_sec"]):
        if col_name is None:
            df = df.withColumn(temp_col_name, F.lit(0))
        else:
            df = df.withColumn(temp_col_name, F.col(col_name))

    time = F.concat_ws(":", F.col("_hour"), F.col("_min"), F.col("_sec"))

    df = df.withColumn(
        column_name_to_assign,
        F.concat_ws(
            " ",
            # F.col(date_column),
            F.concat_ws("-", F.year(date_column), F.month(date_column), F.dayofmonth(date_column)),
            F.concat_ws(
                ":",
                F.when(F.col(am_pm_column) == "PM", F.hour(time) + 12).otherwise(F.hour(time)),
                F.minute(time),
                F.second(time),
            ),
        ),
    )
    df = df.withColumn(column_name_to_assign, F.to_timestamp(column_name_to_assign))
    return df.drop("_hour", "_min", "_sec")


def assign_regex_from_map_additional_rules(
    df: DataFrame,
    column_name_to_assign: str,
    reference_columns: List[str],
    map: Mapping,
    priority_map: Mapping,
    disambiguation_conditions: Optional[dict] = None,
    value_map: Optional[dict] = None,
    first_match_only: Optional[bool] = False,
    overwrite_values: Optional[bool] = False,
    default_value: Optional[Any] = "Don't know",
):
    """
    Apply additional logic around the `assign_regex_from_map` function to allow for increased specificity/.

    Parameters
    ----------
    df
    column_name_to_assign
    reference_columns
    map
        the mapping of values to regex patterns
    priority_map
        the priorities of given values; a lower value means the value is more important. only highest priority matches will be retained
    disambiguation_conditions
        a dictionary of values to conditions for which they apply
    value_map
        a dictionary of values to alternate value mappings for handling duplicate keys in map
    first_match_only
        include only the first matching value from the matched list
    overwrite_values
        whether to overwrite all pre-existing values in `column_name_to_assign`
    """

    temp_col = f"{column_name_to_assign}_temp"

    df = assign_regex_from_map(df, temp_col, reference_columns, map, priority_map)
    if first_match_only:
        df = df.withColumn("disambiguated_col", F.lit(None))
        if disambiguation_conditions is not None:
            for val, condition in disambiguation_conditions.items():
                df = df.withColumn(
                    "disambiguated_col",
                    F.when(
                        condition & (F.col("disambiguated_col").isNull()) & F.array_contains(temp_col, val), val
                    ).otherwise(F.col("disambiguated_col")),
                )
        df = df.withColumn(
            temp_col,
            F.coalesce(
                F.col("disambiguated_col"),
                F.when(F.size(temp_col) == 1, F.col(temp_col).getItem(0)).otherwise(default_value),
            ),
        )
    if overwrite_values:
        df = df.withColumn(column_name_to_assign, F.col(temp_col))
    else:
        df = df.withColumn(column_name_to_assign, F.coalesce(F.col(column_name_to_assign), F.col(temp_col)))
    if value_map is not None:
        df = update_column_values_from_map(df, column_name_to_assign, value_map)
    df = df.drop(temp_col, "disambiguated_col")
    return df


def assign_regex_from_map(
    df: DataFrame, column_name_to_assign: str, reference_columns: List[str], map: Mapping, priority_map: Mapping
):
    """
    Derive a column based ordered regex pattern matching.
    Steps:
    > upon checking regex patters against the combined value of a set of reference columns add the static value to an array if the pattern matches
    > disambiguate the resultant array by using the priority map to sort the best maps

    Parameters
    ----------
    df
    column_name_to_assign
    reference_columns
        A list of columns that will combine to form the reference string for regex matching
    map
        A map of static values to regex patterns that if matching the reference column will be added to the array
    priority_map
        A map of static values to prioritise (higher value, higher priority)
    """
    regex_columns = {key: [] for key in [1, *list(priority_map.values())]}  # type: ignore
    for assign, pattern in map.items():
        col = F.when(F.coalesce(F.concat(*reference_columns), F.lit("")).rlike(pattern), assign)
        if assign in priority_map:
            regex_columns[priority_map[assign]].append(col)
        else:
            regex_columns[1].append(col)

    sorted_regex_columns = dict(sorted(regex_columns.items(), key=lambda item: item[0], reverse=True))

    coalesce_cols = []
    for key, cols in sorted_regex_columns.items():
        col_name = f"priority{key}_{column_name_to_assign}"
        coalesce_cols.append(col_name)
        df = df.withColumn(
            col_name,
            F.array(cols),
        )
        filtered_array = F.expr(f"filter({col_name}, x -> x is not null)")
        df = df.withColumn(col_name, F.when(F.size(filtered_array) != 0, filtered_array))
    df = df.withColumn(column_name_to_assign, F.coalesce(*coalesce_cols)).drop(*coalesce_cols)
    return df


def assign_datetime_from_coalesced_columns_and_log_source(
    df: DataFrame,
    column_name_to_assign: str,
    primary_datetime_columns: List[str],
    secondary_date_columns: List[str],
    min_datetime_column_name: str,
    max_datetime_column_name: str,
    reference_datetime_column_name: str,
    source_reference_column_name: str,
    default_timestamp: str,
    min_datetime_offset_value: int = -4,
    max_datetime_offset_value: int = 0,
    reference_datetime_days_offset_value: int = -2,
    final_fallback_column: str = None,
):

    """
    Assign a timestamp column from coalesced list of columns with a default timestamp if timestamp missing in column

    Parameters
    -----------
    df
        The input dataframe
    column_name_to_assign
        Name of the column to assign the result of this function to
    primary_datetime_columns
        A list of datetime column names which are your primary datetime columns
    secondary_date_columns
        A list of datetime column names which are your secondary datetime columns
    min_datetime_column_name
        The column name to define the minimum datetime
    max_datetime_column_name
        The column name to define the maximum datetime
    reference_datetime_column_name
        The column name of the reference varaible to define maximum datetime or to impute datetime value with offset
    source_reference_column_name
        A column name to assign the source of the dates
    default_timestamp
        A default timestamp value to use.
    min_datetime_offset_value
        The number of days to positively offset the minimum datetime
    max_datetime_offset_value
        The number of days to positively offset the maximum datetime
    reference_datetime_days_offset_value
        The number of days to positively offset the reference datetime for use as source of final timestamp column
    final_fallback_column
        a final fallback column to use if all others are null
    """
    MIN_DATE_BOUND = F.col(min_datetime_column_name) + F.expr(f"INTERVAL {min_datetime_offset_value} DAYS")
    MAX_DATE_BOUND = F.col(max_datetime_column_name) + F.expr(f"INTERVAL {max_datetime_offset_value} DAYS")
    REF_DATE_OFFSET = F.col(reference_datetime_column_name) + F.expr(
        f"INTERVAL {reference_datetime_days_offset_value} DAYS"
    )

    coalesce_columns = [
        F.col(datetime_column) for datetime_column in [*primary_datetime_columns, *secondary_date_columns]
    ]
    coalesce_columns = [
        F.when(col.between(MIN_DATE_BOUND, F.least(F.col(reference_datetime_column_name), MAX_DATE_BOUND)), col)
        for col in coalesce_columns
    ]
    coalesce_columns = [*coalesce_columns, REF_DATE_OFFSET]

    column_names = primary_datetime_columns + secondary_date_columns + [reference_datetime_column_name]

    if final_fallback_column is not None:
        coalesce_columns.append(F.col(final_fallback_column))
        column_names.append(final_fallback_column)

    source_columns = [
        F.when(column_object.isNotNull(), column_name)
        for column_object, column_name in zip(coalesce_columns, column_names)
    ]

    df = df.withColumn(source_reference_column_name, F.coalesce(*source_columns))
    df = df.withColumn(
        column_name_to_assign,
        F.to_timestamp(
            F.when(
                F.col(source_reference_column_name).isin(secondary_date_columns),
                F.concat_ws(
                    " ",
                    F.date_format(F.coalesce(*coalesce_columns), "yyyy-MM-dd"),
                    F.lit(default_timestamp),
                ),
            ).otherwise(F.coalesce(*coalesce_columns)),
            format="yyyy-MM-dd HH:mm:ss",
        ),
    )
    return df


def assign_date_from_filename(df: DataFrame, column_name_to_assign: str, filename_column: str):
    """
    Populate a pyspark date column with the date contained in the filename column

    Parameters
    ----------
    df
    column_name_to_assign
    filename_column
    """
    pattern = r"_(\d{8}|\d{4}-\d{2}-\d{2})(_\d{6}|T\d{6})?[.](csv|txt|json)"
    date = F.regexp_extract(F.col(filename_column), pattern, 1)
    date = F.regexp_replace(date, "-", "")
    time = F.when(
        F.regexp_extract(F.col(filename_column), pattern, 2) == "",
        "_000000",
    ).otherwise(F.regexp_extract(F.col(filename_column), pattern, 2))
    time = F.regexp_replace(time, "T", "_")
    df = df.withColumn(
        column_name_to_assign,
        F.to_timestamp(
            F.concat(F.when(date == "", "20221003").otherwise(date), time),
            format="yyyyMMdd_HHmmss",
        ),
    )
    return df


def assign_incremental_order(df: DataFrame, column_name_to_assign: str, id_column: str, order_list: List[str]):
    """
    Assign an incremental count to each id in id_column by occurences within order_list columns

    Parameters
    -------------
    df
    column_name_to_assign
        column to show count
    id_column
        column from which window is created
    order_list
        counting order occurrence. This list should NOT have any possible repetition.
    """
    window = Window.partitionBy(id_column).orderBy(order_list)
    df = df.withColumn(column_name_to_assign, F.row_number().over(window))
    return df


def map_options_to_bool_columns(df: DataFrame, reference_column: str, value_column_name_map: dict, sep: str = ";"):
    """
    map column containing multiple value options to new columns containing true/false based on if their
    value is chosen as the option.

    Parameters
    ----------
    df
    reference_column
        column containing option values
    value_column_name_map
        mapping expression of column names to assign and options within reference column
    """
    array_exists = "array" in dict(df.dtypes)[reference_column]
    if not array_exists:
        df = df.withColumn(reference_column, F.split(F.col(reference_column), sep))
    for val, col in value_column_name_map.items():
        df = df.withColumn(col, F.when(F.array_contains(reference_column, val), "Yes"))
    return df.withColumn(reference_column, F.array_join(reference_column, sep))


def assign_column_value_from_multiple_column_map(
    df: DataFrame,
    column_name_to_assign: str,
    value_to_condition_map: List[List[Any]],
    column_names: List[str],
    override_original: bool = True,
):
    """
    Assign column value based on values of any number of columns in a dictionary

    Parameters
    ----------
    column_name_to_assign
    value_to_condition_map
        a list of column value options to map to each resultant value in the 'column_name_to_assign'.
        multiple sublists are optional within this input and denote the option to have multiple optional values.
    column_names
        a list of column names in the same order as the values expressed in the 'value_to_condition_map' input

    Example
    -------
    A | B
    1 | 0
    1 | 1
    0 | 0
    value_to_condition_map = [
        [Yes,[1,0]],
        [No,[1,1]]
    ]
    column_names = [A,B]
    ~ with a value of 1 and 0 in columns A and B respectively the result column C would be set to Yes and with
    1 and 1 in the same columns the result would b No and an unmapped result yields None. ~
    A | B | C
    1 | 0 | Yes
    1 | 1 | No
    0 | 0 |
    """
    if override_original or column_name_to_assign not in df.columns:
        df = df.withColumn(column_name_to_assign, F.lit(None).cast("string"))
    for row in value_to_condition_map:
        mapped_value = row[0]
        values = row[1]
        logic = []
        for col, val in zip(column_names, values):
            if type(val) == list:
                logic.append(reduce(or_, [F.col(col).eqNullSafe(option) for option in val]))
            else:
                logic.append(F.col(col).eqNullSafe(val))
        df = df.withColumn(
            column_name_to_assign,
            F.when(reduce(and_, logic), mapped_value).otherwise(F.col(column_name_to_assign)),
        )
    return df


def concat_fields_if_true(
    df: DataFrame,
    column_name_to_assign: str,
    column_name_pattern: str,
    true_value: str,
    sep: str = "",
):
    """
    Concat the names of fields where a given condition is met to form a new column

    Parameters
    ----------
    df
    column_name_to_assign
    column_name_pattern
    true_value
    sep
    """
    columns = [col for col in df.columns if re.match(column_name_pattern, col)]
    df = df.withColumn(
        column_name_to_assign,
        F.concat_ws(
            sep,
            *[F.when(F.col(col) == true_value, col).otherwise(None) for col in columns],
        ),
    )
    return df


def assign_fake_id(df: DataFrame, column_name_to_assign: str, reference_column: str):
    """
    Derive an incremental id from a reference column containing an id

    Parameters
    ----------
    df
    column_name_to_assign
    reference_column
    """
    df_unique_id = df.select(reference_column).distinct()
    df_unique_id = df_unique_id.withColumn("TEMP", F.lit(1))
    window = Window.partitionBy(F.col("TEMP")).orderBy(reference_column)
    df_unique_id = df_unique_id.withColumn(column_name_to_assign, F.row_number().over(window))  # or dense_rank()

    df = df.join(df_unique_id, on=reference_column, how="left")
    return df.drop("TEMP")


def assign_distinct_count_in_group(
    df,
    column_name_to_assign: str,
    count_distinct_columns: List[str],
    group_by_columns: List[str],
):
    """
    Window-based count of distinct values by group

    Parameters
    ----------
    df
    column_name_to_assign
    count_distinct_columns
        columns to determine distinct records
    group_by_columns
        columns to group by and count within
    """
    count_distinct_columns_window = Window.partitionBy(*count_distinct_columns).orderBy(F.lit(0))
    group_window = Window.partitionBy(*group_by_columns)
    df = df.withColumn(
        column_name_to_assign,
        F.sum(F.when(F.row_number().over(count_distinct_columns_window) == 1, 1)).over(group_window).cast("integer"),
    )
    return df


def assign_count_by_group(df: DataFrame, column_name_to_assign: str, group_by_columns: List[str]):
    """
    Window-based count of all rows by group

    Parameters
    ----------
    df
    column_name_to_assign
    group_by_columns
        columns to group by and count within
    """
    count_window = Window.partitionBy(*group_by_columns)
    df = df.withColumn(column_name_to_assign, F.count("*").over(count_window).cast("integer"))
    return df


def assign_multigenerational(
    df: DataFrame,
    column_name_to_assign: str,
    participant_id_column,
    household_id_column: str,
    visit_date_column: str,
    date_of_birth_column: str,
    country_column: str,
    age_column_name_to_assign: str = "age_at_visit",
    school_year_column_name_to_assign: str = "school_year",
):
    """
    Assign a column to specify if a given household is multigenerational at the time one of its participants visited.

    Parameters
    ----------
    df
    column_name_to_assign
    participant_id_column
    household_id_column
    visit_date_column
    date_of_birth_column
    country_column
    age_column_name_to_assign
    school_year_column_name_to_assign
    """
    spark_session = get_or_create_spark_session()
    school_year_lookup_df = spark_session.createDataFrame(
        data=[
            ("England", "09", "01", "09", "01"),
            ("Wales", "09", "01", "09", "01"),
            ("Scotland", "08", "15", "03", "01"),
            ("Northern Ireland", "09", "01", "07", "02"),
        ],
        schema=[
            country_column,
            "school_start_month",
            "school_start_day",
            "school_year_ref_month",
            "school_year_ref_day",
        ],
    )
    transformed_df = df.select(household_id_column, visit_date_column).distinct()
    transformed_df = transformed_df.join(
        df.select(
            household_id_column,
            participant_id_column,
            date_of_birth_column,
            country_column,
        ),
        on=household_id_column,
    ).distinct()

    transformed_df = assign_age_at_date(
        df=transformed_df,
        column_name_to_assign=age_column_name_to_assign,
        base_date=F.col(visit_date_column),
        date_of_birth=F.col(date_of_birth_column),
    )

    transformed_df = assign_school_year(
        df=transformed_df,
        column_name_to_assign=school_year_column_name_to_assign,
        reference_date_column=visit_date_column,
        dob_column=date_of_birth_column,
        country_column=country_column,
        school_year_lookup=school_year_lookup_df,
    )
    generation_1 = F.when((F.col(age_column_name_to_assign) > 49), 1).otherwise(0)
    generation_2 = F.when(
        ((F.col(age_column_name_to_assign) <= 49) & (F.col(age_column_name_to_assign) >= 17))
        | (F.col(school_year_column_name_to_assign) >= 12),
        1,
    ).otherwise(0)
    generation_3 = F.when((F.col(school_year_column_name_to_assign) <= 11), 1).otherwise(0)

    window = Window.partitionBy(household_id_column, visit_date_column)
    generation_1_present = F.sum(generation_1).over(window) >= 1
    generation_2_present = F.sum(generation_2).over(window) >= 1
    generation_3_present = F.sum(generation_3).over(window) >= 1

    transformed_df = transformed_df.withColumn(
        column_name_to_assign,
        F.when(generation_1_present & generation_2_present & generation_3_present, 1).otherwise(0),
    )

    df = null_safe_join(
        df,
        transformed_df.select(
            household_id_column,
            column_name_to_assign,
            age_column_name_to_assign,
            school_year_column_name_to_assign,
            participant_id_column,
            visit_date_column,
        ),
        null_safe_on=[participant_id_column, household_id_column, visit_date_column],
        null_unsafe_on=[],
        how="left",
    )
    return df


def assign_household_participant_count(
    df: DataFrame,
    column_name_to_assign: str,
    household_id_column: str,
    participant_id_column: str,
):
    """
    Assign the count of participants within each household.

    Parameters
    ----------
    df
    column_name_to_assign
    household_id_column
    participant_id_column
    """
    household_window = Window.partitionBy(household_id_column)
    df = df.withColumn(
        column_name_to_assign,
        F.size(F.collect_set(F.col(participant_id_column)).over(household_window)),
    )
    return df


def assign_household_age_count(
    df: DataFrame,
    column_name_to_assign: str,
    column_pattern: str,
    condition_column: str,
    min_age: int,
    max_age: int,
):
    """
    Count number of individuals below max_age and above max_age from age columns matching pattern.
    if condition column is 'No' it will only count so long the range of columns ONLY have only 0s or nulls.

    Parameters
    ----------
    df
    column_name_to_assign
    column_pattern
    condition_column
    """
    columns_to_count = [column for column in df.columns if re.match(column_pattern, column)]
    count = reduce(
        add,
        [
            F.when((F.col(column) >= min_age) & (F.col(column) <= max_age), 1).otherwise(0)
            for column in columns_to_count
        ],
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            ((F.col(condition_column) == "Yes") & (~all_equal(columns_to_count, 0)))
            | ((F.col(condition_column) == "No") & (~all_equal_or_null(columns_to_count, 0))),
            count,
        ).otherwise(0),
    )
    return df


def assign_random_day_in_month(
    df: DataFrame, column_name_to_assign: str, month_column: str, year_column: str
) -> DataFrame:
    """
    Assign a random date in a given year and month

    Parameters
    ----------
    df
    column_name_to_assign
    month_column
    year_column
    """
    df = df.withColumn("TEMP_DAY", F.lit(1))
    df = df.withColumn("TEMP_DATE", F.concat_ws("-", year_column, month_column, "TEMP_DAY"))
    df = df.withColumn(
        "TEMP_DAY",
        F.round(F.rand() * (F.date_format(F.last_day("TEMP_DATE"), "d") - 0.5001), 0) + 0.5,
    )
    df = df.withColumn(
        column_name_to_assign,
        F.to_timestamp(
            F.concat_ws(
                "-",
                F.col(year_column),
                F.lpad(F.col(month_column).cast("string"), 2, "0"),
                F.lpad(F.ceil(F.col("TEMP_DAY")).cast("string"), 2, "0"),
            ),
            format="yyyy-MM-dd",
        ),
    )
    return df.drop("TEMP_DATE", "TEMP_DAY")


def assign_first_occurence(
    df: DataFrame, column_name_to_assign: str, id_column: str, event_date_column: str
) -> DataFrame:
    """
    Assign first date occurence in a column for each id in id_column

    Parameters
    ----------
    df
    column_name_to_assign
    id_column
    event_date_column
    """
    window = Window.partitionBy(id_column).orderBy(event_date_column)
    return df.withColumn(column_name_to_assign, F.first(event_date_column, ignorenulls=True).over(window))


def assign_last_occurence(
    df: DataFrame, column_name_to_assign: str, id_column: str, event_date_column: str
) -> DataFrame:
    """
    Assign first date occurence in a column for each id in id_column

    Parameters
    ----------
    df
    column_name_to_assign
    id_column
    event_date_column
    """
    window = Window.partitionBy(id_column).orderBy(F.desc(event_date_column))
    return df.withColumn(column_name_to_assign, F.first(event_date_column, ignorenulls=True).over(window))


def assign_column_given_proportion(
    df: DataFrame,
    column_name_to_assign: str,
    groupby_column: str,
    reference_columns: List[str],
    count_if: List[Union[str, int]],
    true_false_values: List[Union[str, int]],
) -> DataFrame:
    """
    Assign a column boolean 1, 0 when the proportion of values meeting a condition is above 0.3

    Parameters
    ----------
    df
    column_name_to_assign
    groupby_column
    reference_columns
    count_if
    true_false_values
    """
    window = Window.partitionBy(groupby_column)

    df = df.withColumn(column_name_to_assign, F.lit("No"))

    row_result = F.coalesce(*[F.when(F.col(col).isin(count_if), 1) for col in reference_columns])
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (
                F.sum(F.when(row_result == 1, 1).otherwise(0)).over(window)
                / F.sum(F.when(any_column_not_null(reference_columns), 1)).over(window)
                >= 0.3
            ),
            1,
        ).otherwise(0),
    )
    df = df.withColumn(column_name_to_assign, F.max(column_name_to_assign).over(window))
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(column_name_to_assign) == 1, true_false_values[0]).otherwise(true_false_values[1]),
    )
    return df


def count_value_occurrences_in_column_subset_row_wise(
    df: DataFrame,
    column_name_to_assign: str,
    selection_columns: List[str],
    count_if_value: Union[str, int, None],
) -> DataFrame:
    """
    Assign a column to be the count of cells in selection row where condition is true

    Parameters
    ---------
    df
    column_name_to_assign
    selection_columns
    count_if_value
    """
    df = df.withColumn(column_name_to_assign, F.array([F.col(col) for col in selection_columns]))
    if count_if_value is None:
        df = df.withColumn(
            column_name_to_assign,
            F.expr(f"filter({column_name_to_assign}, x -> x is not null)"),
        )
    else:
        df = df.withColumn(column_name_to_assign, F.array_remove(column_name_to_assign, count_if_value))
    df = df.withColumn(
        column_name_to_assign,
        F.lit(len(selection_columns) - F.size(F.col(column_name_to_assign))),
    )
    return df


def assign_condition_around_event(
    df: DataFrame,
    column_name_to_assign: str,
    condition_bool_column: str,
    id_column: str,
    event_date_column: str,
    event_id_column: str,
) -> DataFrame:
    """
    Assign a column with boolean (Yes, No) if condition_bool_column is positive around an event, derived
    from if condition_bool_columns reported any true values -1 +1 from event window

    Parameters
    ----------
    df
    column_name_to_assign
    symptoms_bool_column
    id_column
    visit_date_column
    visit_id_column
    """
    window = Window.partitionBy(id_column).orderBy(event_date_column, event_id_column)
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(condition_bool_column) == "Yes")
            | (F.lag(condition_bool_column, 1).over(window) == "Yes")
            | (F.lag(condition_bool_column, -1).over(window) == "Yes"),
            "Yes",
        ).otherwise("No"),
    )
    return df


def assign_true_if_any(
    df: DataFrame,
    column_name_to_assign: str,
    reference_columns: List[str],
    true_false_values: List[Union[str, int, bool]],
    ignore_nulls: Optional[bool] = False,
) -> DataFrame:
    """
    Assign column the second value of a list containing values for false and true
    if either of a list of reference columns are true
    column_name_to_assign is assigned initially to all false then assigned true when value found
    in any of the reference columns

    Parameters:
    df
    column_name_to_assign
    reference_columns
    true_false_values
    ignore_nulls
    """
    df = df.withColumn(column_name_to_assign, F.lit(true_false_values[1]))
    for col in reference_columns:
        df = df.withColumn(
            column_name_to_assign,
            F.when(
                F.col(col).eqNullSafe(true_false_values[0]),
                true_false_values[0],
            )
            .when(F.col(col).isNull() & F.lit(ignore_nulls), None)
            .otherwise(F.col(column_name_to_assign)),
        )
    return df


def assign_unique_id_column(df: DataFrame, column_name_to_assign: str, concat_columns: List[str]) -> DataFrame:
    """
    Assign a unique column from concatenating multiple input columns

    Parameters
    ----------
    df
    column_name_to_assign
    concat_columns
    """
    return df.withColumn(column_name_to_assign, F.concat_ws("-", *concat_columns))


def assign_filename_column(df: DataFrame, column_name_to_assign: str) -> DataFrame:
    """
    Use inbuilt pyspark function to get name of the file used in the current spark task
    Regular expression removes unnecessary characters to allow checks for processed files

    Parameters
    ----------
    df
    column_name_to_assign
    """
    if column_name_to_assign not in df.columns:
        return df.withColumn(
            column_name_to_assign, F.regexp_replace(F.input_file_name(), r"(?<=:\/{2})(\w+|\d+)(?=\/{1})", "")
        )
    return df


def assign_column_from_mapped_list_key(
    df: DataFrame, column_name_to_assign: str, reference_column: str, map: dict
) -> DataFrame:
    """
    Assign a specific column value using a dictionary of values to assign as keys and
    the list criteria corresponding to when that value should be assign as a value

    Parameters
    ----------
    df
    column_name_to_assign
    reference_column
    map
    """
    df = df.withColumn(column_name_to_assign, F.lit(None))
    for val, key_list in map.items():
        df = df.withColumn(
            column_name_to_assign,
            F.when(F.col(reference_column).isin(*key_list), val).otherwise(F.col(column_name_to_assign)),
        )
    return df


def assign_school_year_september_start(
    df: DataFrame, dob_column: str, visit_date_column: str, column_name_to_assign: str
) -> DataFrame:
    """
    Assign a column for the approximate school year of an individual given their age at the time
    of visit

    Parameters
    ----------
    df
    dob_column
    visit_date_column
    column_name_to_assign
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            ((F.month(F.col(visit_date_column))) >= 9) & ((F.month(F.col(dob_column))) < 9),
            (F.year(F.col(visit_date_column))) - (F.year(F.col(dob_column))) - 3,
        )
        .when(
            (F.month(F.col(visit_date_column)) >= 9) | ((F.month(F.col(dob_column))) >= 9),
            (F.year(F.col(visit_date_column))) - (F.year(F.col(dob_column))) - 4,
        )
        .otherwise((F.year(F.col(visit_date_column))) - (F.year(F.col(dob_column))) - 5),
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(column_name_to_assign) <= 0) | (F.col(column_name_to_assign) > 13),
            None,
        ).otherwise(F.col(column_name_to_assign)),
    )
    return df


def assign_named_buckets(
    df: DataFrame,
    reference_column: str,
    column_name_to_assign: str,
    map: dict,
    use_current_values=False,
) -> DataFrame:
    """
    Assign a new column with named ranges for given integer ranges contained within a reference column

    Parameters
    ----------
    df
    reference_column
    column_name_to_assign
    map
        dictionary containing the map of minimum value in given range (inclusive) to range label string
    use_current_values
        boolean operation preset to False to specify if current values in column_name_to_assign should be carried
        forward if not in range of lookup buckets specified in map
    """
    bucketizer = Bucketizer(
        splits=[float("-Inf"), *list(map.keys()), float("Inf")],
        inputCol=reference_column,
        outputCol="buckets",
    )
    dfb = bucketizer.setHandleInvalid("keep").transform(df)

    bucket_dic = {0.0: F.col(column_name_to_assign) if use_current_values else None}
    for i, value in enumerate(map.values()):
        bucket_dic[float(i + 1)] = value

    mapping_expr = F.create_map([F.lit(x) for x in chain(*bucket_dic.items())])  # type: ignore

    dfb = dfb.withColumn(column_name_to_assign, mapping_expr[dfb["buckets"]])
    return dfb.drop("buckets")


def derive_age_based_columns(df: DataFrame, column_name_to_assign: str) -> DataFrame:
    """
    Transformations involving participant age.
    """
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_5_intervals",
        map={2: "2-11", 12: "12-19", 20: "20-49", 50: "50-69", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_over_16",
        map={16: "16-49", 50: "50-69", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_7_intervals",
        map={2: "2-11", 12: "12-16", 17: "17-24", 25: "25-34", 35: "35-49", 50: "50-69", 70: "70+"},
    )
    df = assign_named_buckets(
        df,
        reference_column=column_name_to_assign,
        column_name_to_assign="age_group_5_year_intervals",
        map={
            2: "2-4",
            5: "5-9",
            10: "10-14",
            15: "15-19",
            20: "20-24",
            25: "25-29",
            30: "30-34",
            35: "35-39",
            40: "40-44",
            45: "45-49",
            50: "50-54",
            55: "55-59",
            60: "60-64",
            65: "65-69",
            70: "70-74",
            75: "75-79",
            80: "80-84",
            85: "85-89",
            90: "90+",
        },
    )

    return df


def assign_age_group_school_year(
    df: DataFrame,
    country_column: str,
    age_column: str,
    school_year_column: str,
    column_name_to_assign: str,
) -> DataFrame:
    """
    Assign column_age_group_school_year using multiple references column values in a specific pattern
    to determine a string coded representation of school year

    Parameters
    ----------
    df:
    country_column
    age_column
    school_year_column
    column_name_to_assign
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            ((F.col(age_column) >= 2) & (F.col(age_column) <= 12))
            & ((F.col(school_year_column) <= 6) | (F.col(school_year_column).isNull())),
            "02-6SY",
        )
        .when(
            ((F.col(school_year_column) >= 7) & (F.col(school_year_column) <= 11))
            | (
                (F.col(country_column).isin("England", "Wales"))
                & ((F.col(age_column) >= 12) & (F.col(age_column) <= 15))
                & (((F.col(school_year_column) <= 6) | (F.col(school_year_column).isNull())))
            )
            | (
                (F.col(country_column).isin("Scotland", "Northern Ireland"))
                & ((F.col(age_column) >= 12) & (F.col(age_column) <= 14))
                & (((F.col(school_year_column) <= 6) | (F.col(school_year_column).isNull())))
            ),
            "07SY-11SY",
        )
        .when(
            (
                (F.col(country_column).isin("England", "Wales"))
                & ((F.col(age_column) >= 16) & (F.col(age_column) <= 24))
                & ((F.col(school_year_column) >= 12) | (F.col(school_year_column).isNull()))
            )
            | (
                (F.col(country_column).isin("Scotland", "Northern Ireland"))
                & ((F.col(age_column) >= 15) & (F.col(age_column) <= 24))
                & ((F.col(school_year_column) >= 12) | (F.col(school_year_column).isNull()))
            ),
            "12SY-24",
        )
        .otherwise(None),
    )
    df = assign_named_buckets(
        df,
        age_column,
        column_name_to_assign,
        {25: "25-34", 35: "35-49", 50: "50-69", 70: "70+"},
        True,
    )
    return df


def assign_ethnicity_white(df: DataFrame, column_name_to_assign: str, ethnicity_group_column_name: str):
    """
    Assign string variable for ethnicity white / non-white based on the 5 major ethnicity groups
    """

    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(ethnicity_group_column_name) == "White", "White")
        .when(F.col(ethnicity_group_column_name) != "White", "Non-White")
        .otherwise(None)
        .cast("string"),
    )
    return df


def assign_taken_column(df: DataFrame, column_name_to_assign: str, reference_column: str) -> DataFrame:
    """
    Uses references column value to assign a taken column "yes" or "no" depending on whether
    reference is Null

    Parameters
    ----------
    df
    column_name_to_assign
    reference_column
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(reference_column).isNull(), "No").otherwise("Yes"),
    )

    return df


def assign_outward_postcode(df: DataFrame, column_name_to_assign: str, reference_column: str) -> DataFrame:
    """
    Assign column outer postcode with cleaned data from reference postcode column.
    take only left part of postcode and capitalise

    Parameters
    ----------
    df
    column_name_to_assign
    reference_column
    """
    df = df.withColumn(
        column_name_to_assign,
        F.rtrim(F.regexp_replace(F.col(reference_column), r".{3}$", "")),
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.length(column_name_to_assign) > 4, None).otherwise(F.col(column_name_to_assign)),
    )

    return df


def assign_school_year(
    df: DataFrame,
    column_name_to_assign: str,
    reference_date_column: str,
    dob_column: str,
    country_column: str,
    school_year_lookup: DataFrame,
) -> DataFrame:
    """
    Assign school year based on date of birth and visit date, accounting for schooling differences by DA.
    From households_aggregate_processes.xlsx, derivation number 31.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be created
    reference_date_column
        Name of column to calculate school year with respect to that point in time
    dob_column
        Name of column specifying date of birth
    country_column
        Name of column specifying country
    school_year_lookup:
        Lookup table defining the school year start day/month and the school year
        reference day/month (which year participant in by dob) by country
    """

    df = df.join(F.broadcast(school_year_lookup), on=country_column, how="left").withColumn(
        "school_start_date",
        F.when(
            (F.month(dob_column) > F.col("school_year_ref_month"))
            | (
                (F.month(dob_column) == F.col("school_year_ref_month"))
                & (F.dayofmonth(dob_column) >= F.col("school_year_ref_day"))
            ),
            F.to_date(
                F.concat(F.year(dob_column) + 5, F.col("school_start_month"), F.col("school_start_day")),
                format="yyyyMMdd",
            ),
        ).otherwise(
            F.to_date(
                F.concat(F.year(dob_column) + 4, F.col("school_start_month"), F.col("school_start_day")),
                format="yyyyMMdd",
            )
        ),
    )
    df = (
        df.withColumn(
            column_name_to_assign,
            F.when(
                (F.month(reference_date_column) > F.month("school_start_date"))
                | (
                    (F.dayofmonth(reference_date_column) >= F.dayofmonth("school_start_date"))
                    & (F.month(reference_date_column) == F.month("school_start_date"))
                ),
                F.year(reference_date_column) - F.year("school_start_date"),
            ).otherwise(F.year(reference_date_column) - F.year("school_start_date") - 1),
        )
        .withColumn(
            column_name_to_assign,
            F.when(
                (F.col(column_name_to_assign) >= F.lit(14)) | (F.col(column_name_to_assign) < F.lit(0)), None
            ).otherwise(F.col(column_name_to_assign)),
        )
        .drop(
            "school_start_month",
            "school_start_day",
            "school_year_ref_month",
            "school_year_ref_day",
            "school_start_date",
        )
    )
    return df


def assign_mean_across_columns(df: DataFrame, new_column_name: str, column_names: list) -> DataFrame:
    """
    Create a new column containing the mean of multiple existing columns.

    # Caveat:
    # 0 values are treated as nulls.

    Parameters
    ----------
    df
    new_column_name
        name of column to be created
    column_names
        list of column names to calculate mean across
    """
    columns = [F.col(name) for name in column_names]

    df = df.withColumn("temporary_column_count", F.lit(0))
    for column in column_names:
        df = df.withColumn(
            "temporary_column_count",
            F.when((F.col(column) > 0), F.col("temporary_column_count") + 1).otherwise(F.col("temporary_column_count")),
        )

    # Sum with NULL values removed
    average_expression = sum(F.coalesce(column, F.lit(0)) for column in columns) / F.col("temporary_column_count")
    df = df.withColumn(new_column_name, average_expression)
    df = df.drop("temporary_column_count")
    return df


def assign_date_difference(
    df: DataFrame,
    column_name_to_assign: str,
    start_reference_column: str,
    end_reference_column: str,
    format: Optional[str] = "days",
) -> DataFrame:
    """
    Calculate the difference in days between two dates.
    From households_aggregate_processes.xlsx, derivation number 27.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    start_reference_column
        First date column name.
    end_reference_column
        Second date column name.
    format
        time format (days, weeks, fortnights)

    Return
    ------
    pyspark.sql.DataFrame
    """
    allowed_formats = ["days", "weeks", "fortnights"]
    if format in allowed_formats:
        if start_reference_column == "survey start":
            start = F.to_timestamp(F.lit("2020-05-11 00:00:00"))
        else:
            start = F.col(start_reference_column)
        modifications = {
            "weeks": F.floor(F.col(column_name_to_assign) / 7),
            "fortnights": F.floor(F.col(column_name_to_assign) / 14),
        }
        df = df.withColumn(
            column_name_to_assign,
            F.datediff(end=F.col(end_reference_column), start=start),
        )
        if format in modifications:
            df = df.withColumn(column_name_to_assign, modifications[format])
        return df.withColumn(column_name_to_assign, F.col(column_name_to_assign).cast("integer"))
    else:
        raise TypeError(f"{format} format not supported")


def assign_column_uniform_value(df: DataFrame, column_name_to_assign: str, uniform_value) -> DataFrame:
    """
    Assign a column with a uniform value.
    From households_aggregate_processes.xlsx, derivation number 11.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    uniform_value
        Value to be set in column.

    Return
    ------
    pyspark.sql.DataFrame


    Notes
    -----
    uniform_value will work as int, float, bool, str, datetime -
            iterables/collections raise errors.
    """
    return df.withColumn(column_name_to_assign, F.lit(uniform_value))


def assign_column_regex_match(
    df: DataFrame, column_name_to_assign: str, reference_column: str, pattern: str
) -> DataFrame:
    """
    Assign a boolean column based on a regex match on reference column.
    From households_aggregate_processes.xlsx, derivation number 12.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    reference_column
        Name of column that will be matched
    pattern
        Regular expression pattern as a string
        Needs to be a raw string literal (preceded by r"")

    Returns
    -------
    pyspark.sql.DataFrame
    """
    return df.withColumn(column_name_to_assign, F.col(reference_column).rlike(pattern))


def assign_column_to_date_string(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    time_format: str = "yyyy-MM-dd",
    lower_case: bool = False,
) -> DataFrame:
    """
    Assign a column with a TimeStampType to a formatted date string.gg
    Does not use a DateType object, as this is incompatible with out HIVE tables.
    From households_aggregate_processes.xlsx, derivation number 13.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    reference_column
        Name of column of TimeStamp type to be converted
    time_format
        as a string and by using the accepted characters of Pyspark, define what time format is required
        by default, it will be yyyy-MM-dd, e.g. 2021-01-03

    Returns
    -------
    pyspark.sql.DataFrame
    """
    df = df.withColumn(column_name_to_assign, F.date_format(F.col(reference_column), time_format))

    if lower_case:
        df = df.withColumn(column_name_to_assign, F.lower(F.col(column_name_to_assign)))
    return df


def assign_isin_list(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    values_list: List[Union[str, int]],
    true_false_values: List[Union[str, int]],
) -> DataFrame:
    """
    Create a new column containing either 1 or 0 derived from values in a list, matched
    with existing values in the database (null values will be carried forward as null)
    From households_aggregate_processes.xlsx, derivation number 10.

    Parameters
    ----------
    df
    column_name_to _assign
        new or existing
    reference_column_name
        name of column to check for list values
    values_list
        list of values to check against reference column
    true_false_values
        true value (index 0), false value (index 1)

    Return
    ------
    pyspark.sql.DataFrame
    """
    return df.withColumn(
        column_name_to_assign,
        F.when((F.col(reference_column).isin(values_list)), true_false_values[0])
        .when((~F.col(reference_column).isin(values_list)), true_false_values[1])
        .otherwise(None),
    )


def assign_age_at_date(df: DataFrame, column_name_to_assign: str, base_date, date_of_birth) -> DataFrame:
    """
    Assign a new column containing age at a specified date
    Assume that parameters will be in date format
    The function will not correctly account for leap years

    Parameters
    ----------
    pyspark.sql.DataFrame
    base_date
    date_of_birth

    Return
    ------
    pyspark.sql.DataFrame
    """

    df = df.withColumn("date_diff", F.datediff(base_date, date_of_birth)).withColumn(
        column_name_to_assign, F.floor(F.col("date_diff") / 365.25).cast("integer")
    )

    return df.drop("date_diff")


def assign_correct_age_at_date(df: DataFrame, column_name_to_assign, reference_date, date_of_birth) -> DataFrame:
    """
    Uses correct logic to calculate complete years elapsed between 2 dates
    """
    df = df.withColumn(
        "month_more",
        F.when(F.month(F.col(reference_date)) > F.month(F.col(date_of_birth)), 2).otherwise(
            F.when(F.month(F.col(reference_date)) == F.month(F.col(date_of_birth)), 1).otherwise(0)
        ),
    )
    df = df.withColumn(
        "day_more",
        F.when(
            F.date_format(F.col(reference_date), "d") >= F.date_format(F.col(date_of_birth), "d"),
            1,
        ).otherwise(0),
    )
    df = df.withColumn(
        column_name_to_assign,
        F.year(F.col(reference_date))
        - F.year(F.col(date_of_birth))
        - F.lit(1)
        + F.round((F.col("month_more") + F.col("day_more")) / 3, 0).cast("int"),
    )
    return df.drop("month_more", "day_more")


def assign_raw_copies(df: DataFrame, reference_columns: list, suffix: str = "raw") -> DataFrame:
    """Create a copy of each column in a list, with a new suffix."""
    for column in reference_columns:
        df = df.withColumn(column + "_" + suffix, F.col(column).cast(df.schema[column].dataType))
    return df


def assign_last_non_null_value_from_col_list(df: DataFrame, column_name_to_assign: str, column_list: List[str]):
    """
    Assigns a single new value to a column by evaluating values in the column_list, assigning the last non null
    value in ascending order, having removed any nulls.
    Parameters
    ----------
    df
    column_name_to_assign: column name of the derived column
    column_list: the columns you want to order and select the last value when ordered ascending
    columns should be of same type
    """
    df = df.withColumn("temp_array", F.array_sort(F.array(column_list)))
    df = df.withColumn(column_name_to_assign, F.element_at(F.expr("filter(temp_array, x -> x is not null)"), -1))
    df = df.drop("temp_array")
    return df


def aggregated_output_groupby(
    df: DataFrame,
    column_group: str,
    apply_function_list: List[str],
    column_name_list: List[str],
    column_name_to_assign_list: List[str],
) -> DataFrame:
    """
    Parameters
    ----------
    df
    column_group
    apply_function_list
    column_name_list
    column_name_to_assign_list
    """
    function_object_list = [
        getattr(F, function)(col_name) for col_name, function in zip(column_name_list, apply_function_list)
    ]
    return df.groupBy(column_group).agg(
        *[
            apply_function.alias(column_name_to_assign)
            for apply_function, column_name_to_assign in zip(function_object_list, column_name_to_assign_list)
        ]
    )


def aggregated_output_window(
    df: DataFrame,
    column_window_list: List[str],
    column_name_list: List[str],
    apply_function_list: List,
    column_name_to_assign_list: List[str],
    order_column_list: List[str] = [],
) -> DataFrame:
    """
    Parameters
    ----------
    df
    column_window_list
    column_name_list
    apply_function_list
    column_name_to_assign_list
    order_column_list
    """
    window = Window.partitionBy(*column_window_list).orderBy(*order_column_list)
    function_object_list = [
        getattr(F, function)(col_name) for col_name, function in zip(column_name_list, apply_function_list)
    ]
    for apply_function, column_name_to_assign in zip(function_object_list, column_name_to_assign_list):
        df = df.withColumn(column_name_to_assign, apply_function.over(window))
    return df


def regex_match_result(
    columns_to_check_in: List[str],
    positive_regex_pattern: str,
    negative_regex_pattern: Optional[str] = None,
    debug_mode: bool = False,
) -> Union[F.Column, Tuple[F.Column, F.Column, F.Column]]:
    """
    A generic function which applies the user provided RegEx patterns to a list of columns. If a value in any
    of the columns matches the `positive_regex_pattern` pattern but not the `negative_regex_pattern` pattern
    then the result of the match will be set to (bool) True, False otherwise.

    The Truth Table below shows how the final pattern matching result is arrived at.

    #+----------------------+----------------------+-----+
    #|positive_regex_pattern|negative_regex_pattern|final|
    #+----------------------+----------------------+-----+
    #|                  true|                  true|false|
    #|                  true|                 false| true|
    #|                 false|                  true|false|
    #|                 false|                 false|false|
    #+----------------------+----------------------+-----+

    Parameters
    ----------
    columns_to_check_in
        a list of columns in which to look for the `positive_regex_pattern`
    positive_regex_pattern
        the Spark-compatible regex pattern match against
    negative_regex_pattern
        (optional) the Spark-compatible regex pattern to NOT match against.
    debug_mode:
        If True and `negative_regex_pattern` is not None, then result of applying positive_regex_pattern and
        negative_regex_pattern are turned in addition to the final result.

    Returns
    -------
    Column
        The final result of applying positive_regex_pattern and negative_regex_pattern (if given)
    Tuple[Column, Column, Column]
        Returned when `debug_mode` is True and `negative_regex_pattern` is not None. First element
        is the result of applying `positive_regex_pattern`, second element is the result of applying
        `negative_regex_pattern` and 3rd is the final result of applying positive_regex_pattern and
        negative_regex_pattern
    """
    positive_regex_match_result = any_column_matches_regex(columns_to_check_in, positive_regex_pattern)

    if negative_regex_pattern is None:
        result = positive_regex_match_result
    else:
        negative_regex_match_result = any_column_matches_regex(columns_to_check_in, negative_regex_pattern)
        result = positive_regex_match_result & ~negative_regex_match_result

        if debug_mode:
            result = positive_regex_match_result, negative_regex_match_result, result

    return result


def assign_regex_match_result(
    df: DataFrame,
    columns_to_check_in: List[str],
    positive_regex_pattern: str,
    column_name_to_assign: str,
    negative_regex_pattern: Optional[str] = None,
    debug_mode: bool = False,
) -> DataFrame:
    """
    A generic function which applies the user provided RegEx patterns to a list of columns. If a value in any
    of the columns matches the `positive_regex_pattern` pattern but not the `negative_regex_pattern` pattern
    then the result of the match will be set to (bool) True, False otherwise.

    The Truth Table below shows how the final pattern matching result is assigned.

    #+----------------------+----------------------+-----+
    #|positive_regex_pattern|negative_regex_pattern|final|
    #+----------------------+----------------------+-----+
    #|                  true|                  true|false|
    #|                  true|                 false| true|
    #|                 false|                  true|false|
    #|                 false|                 false|false|
    #+----------------------+----------------------+-----+

    Parameters
    ----------
    df
        The input dataframe to process
    columns_to_check_in
        a list of columns in which to look for the `positive_regex_pattern`
    positive_regex_pattern
        the Spark-compatible regex pattern match against
    negative_regex_pattern
        (optional) the Spark-compatible regex pattern to NOT match against. If given and `column_name_to_assign` is not
        None, then two additional columns of the form: f"{column_name_to_assign}_positive" &
        f"{column_name_to_assign}_negative" are created which track the matches against the positive and negative regex
        patterns respectively. Set `debug_mode` to True to expose these columns in the returned dataframe.
    column_name_to_assign
        (optional) if this is none, then we return a PySpark Column object containing the result of the RegEx pattern
        search, otherwise we return a DataFrame with `column_name_to_assign` as the column containing the result.
    debug_mode:
        Only relevant when `column_name_to_assign` is not None - See `negative_regex_pattern` above.

    See Also
    --------
    regex_match_result: `assign_regex_match_result` wraps around `regex_match_result`
    """
    match_result = regex_match_result(
        columns_to_check_in=columns_to_check_in,
        positive_regex_pattern=positive_regex_pattern,
        negative_regex_pattern=negative_regex_pattern,
        debug_mode=debug_mode,
    )

    if type(match_result) == tuple:
        # a tuple is returned when debug_mode is True
        positive_regex_match_result, negative_regex_match_result, result = match_result

        df = (
            df.withColumn(f"{column_name_to_assign}_positive", positive_regex_match_result)
            .withColumn(
                f"{column_name_to_assign}_negative",
                negative_regex_match_result,
            )
            .withColumn(column_name_to_assign, result)
        )
    else:
        df = df.withColumn(column_name_to_assign, match_result)

    return df


def get_keys_by_value(input_dict: Dict, values_to_lookup: List) -> List:
    """
    Returns a list of keys from the input dictionary if the dictionary's values are in the
    given list of values.

    Parameters
    ----------
    input_dict
        the input dictionary
    values_to_lookup
        a list of values to search for in the `input_dict` and return matching keys

    Raises
    ------
    ValueError
        If none of the values in `values_to_lookup` are found as values of the `input_dict`.
    """
    result = [k for k, v in input_dict.items() if v in values_to_lookup]
    if len(result) == 0:
        raise ValueError("None of the values in `values_to_lookup` are found in `input_dict`")
    return result


def assign_population_projections(
    current_projection_df: DataFrame, previous_projection_df: DataFrame, month: int, m_f_columns: List[str]
):
    """
    Use a given input month to create a scalar between previous and current values within m and f
    subscript columns and update values accordingly
    """
    for col in m_f_columns:
        current_projection_df = current_projection_df.withColumnRenamed(col, f"{col}_new")
    current_projection_df = current_projection_df.join(
        previous_projection_df.select("id", *m_f_columns), on="id", how="left"
    )
    if month < 6:
        a = 6 - month
        b = 6 + month
    else:
        a = 18 - month
        b = month - 6

    for col in m_f_columns:
        current_projection_df = current_projection_df.withColumn(
            col, F.lit(1 / 12) * ((a * F.col(col)) + (b * F.col(f"{col}_new")))
        )
        current_projection_df = current_projection_df.drop(f"{col}_new")
    return current_projection_df


def get_matches(old_sample_df: DataFrame, new_sample_df: DataFrame, selection_columns: List[str], barcode_column: str):
    """
    assign column to denote whether the data of a given set of columns (selection_columns) matches
    between old and new sample df's (old_sample_df) (new_sample_df)
    """
    select_df = old_sample_df.select(barcode_column, *selection_columns)
    for col in select_df.columns:
        if col != barcode_column:
            select_df = select_df.withColumnRenamed(col, col + "_OLD")
    joined_df = new_sample_df.join(select_df, on=barcode_column, how="left")
    for col in selection_columns:
        joined_df = joined_df.withColumn(f"MATCHED_{col}", F.when(F.col(col) == F.col(f"{col}_OLD"), 1).otherwise(None))
    return joined_df
