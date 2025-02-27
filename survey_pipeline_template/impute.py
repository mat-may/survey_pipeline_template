import logging
import sys
from datetime import datetime
from itertools import chain
from typing import Callable
from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from survey_pipeline_template.derive import assign_random_day_in_month
from survey_pipeline_template.expressions import any_column_not_null
from survey_pipeline_template.merge import union_multiple_tables
from survey_pipeline_template.pipeline.load import extract_from_table
from survey_pipeline_template.pipeline.load import update_table
from survey_pipeline_template.pyspark_utils import get_or_create_spark_session


def generate_sample_proportional_to_size_udf(spark):
    def sample_proportional_to_size(imputation_variables, expected_frequencies, number):
        """UDF for sampling proportional to size."""
        import numpy as np

        results = np.random.choice(a=imputation_variables, p=expected_frequencies, replace=False, size=number).tolist()
        return results

    spark.udf.register("sample_proportional_to_size_udf_reg", sample_proportional_to_size)
    return F.udf(sample_proportional_to_size, ArrayType(StringType()))


sample_proportional_to_size_udf = generate_sample_proportional_to_size_udf(get_or_create_spark_session())


def fill_forward_target_columns(
    df: DataFrame,
    target_indicator_column: str,
    target_date_column: str,
    target_date_tolerance: int,
    detail_columns: List[str],
    participant_id_column: str,
    event_datetime_column: str,
    event_id_column: str,
    use_hdfs: bool = True,
):
    """
    Fill forwards all target columns assocaited with a target event.
    Disambiguate events by earliest recorded expression of an event and
    take the latest event for each visit_datetime forward across all records.
    Parameters
    ----------
    df
    target_indicator_column
        column indicating if the targetted event took place
    target_date_column
        date of the target event
    detail_columns
        additional columns relating to the target event
    participant_id_column
    event_datetime_column
    event_id_column
    """
    event_columns = [target_date_column, target_indicator_column, *detail_columns]
    ordering_window = Window.partitionBy(participant_id_column).orderBy(event_datetime_column)
    window = Window.partitionBy(participant_id_column, event_datetime_column, event_id_column).orderBy("VISIT_DIFF")
    completed_sections = []
    events_df = None
    # ~~ Pre process dataframe to remove unworkable rows ~~ #
    filtered_df = df.filter(
        (F.col(event_datetime_column).isNotNull())
        & (F.col(target_date_column).isNotNull())
        & (F.col(target_date_column) <= F.col(event_datetime_column))
    ).distinct()
    null_df = df.filter(F.col(event_datetime_column).isNull())
    df = df.filter(F.col(event_datetime_column).isNotNull())
    # ~~ Normalise dates ~~ #
    # create an expression for when a series of dates has been normalised
    # gets the date difference between the first row by visit datetime in the filtered dataset
    apply_logic = (
        F.abs(F.datediff(F.first(F.col(target_date_column)).over(ordering_window), F.col(target_date_column)))
        <= target_date_tolerance
    )
    # optimise the filtered_df schema and cache to improve spark plan creation / runtime
    filtered_df = filtered_df.select(
        participant_id_column, event_datetime_column, event_id_column, *event_columns
    ).cache()
    # loops until there are no rows left to normalise getting the first row where that satisfies the `apply_logic` condition
    i = 0
    while filtered_df.count() > 0:
        filtered_df = filtered_df.withColumn("LOGIC_APPLIED", apply_logic)
        temp_df = filtered_df.withColumn("ROW", F.row_number().over(ordering_window))
        if not use_hdfs:
            completed_sections.append(temp_df.filter(F.col("LOGIC_APPLIED") & (F.col("ROW") == 1)).drop("ROW"))
        else:
            mode = "overwrite" if i == 0 else "append"
            update_table(
                temp_df.filter(F.col("LOGIC_APPLIED") & (F.col("ROW") == 1)).drop("ROW"),
                f"{target_indicator_column}_temp_lookup",
                mode,
            )
        filtered_df = filtered_df.filter(~F.col("LOGIC_APPLIED"))
        i += 1
    # combine the sections of normalised dates together and rejoin the null data
    if not use_hdfs:
        if len(completed_sections) >= 2:
            events_df = union_multiple_tables(completed_sections)
        elif len(completed_sections) == 1:
            events_df = completed_sections[0]
    else:
        events_df = extract_from_table(f"{target_indicator_column}_temp_lookup", True)

    # ~~ Construct resultant dataframe by fill forwards ~~#

    # use this columns to override the original dataframe

    if events_df is not None and events_df.count() > 0:
        df = (
            df.drop(*event_columns)
            .join(events_df.select(participant_id_column, *event_columns), on=participant_id_column, how="left")
            .withColumn("DROP_EVENT", (F.col(target_date_column) > F.col(event_datetime_column)))
        )
    else:
        df = df.withColumn("DROP_EVENT", F.lit(False))
    # add the additional detail columns to the original dataframe
    for col in event_columns:
        df = df.withColumn(col, F.when(F.col("DROP_EVENT"), None).otherwise(F.col(col)))

    # pick the best row to retain based upon proximity to visit date
    df = df.withColumn(target_indicator_column, F.when(F.col(target_date_column).isNull(), "No").otherwise("Yes"))
    df = df.withColumn(
        "VISIT_DIFF",
        F.coalesce(F.abs(F.datediff(F.col(target_date_column), F.col(event_datetime_column))), F.lit(float("inf"))),
    )
    df = df.drop("DROP_EVENT").distinct().withColumn("ROW", F.row_number().over(window)).drop("VISIT_DIFF")
    df = df.filter(F.col("ROW") == 1).drop("ROW")
    df = df.unionByName(null_df)
    return df


def generate_fill_forward_df(
    df: DataFrame,
    fill_forward_columns: List[str],
    participant_id_column: str,
    event_datetime_column: str,
    record_changed_column: str,
    record_changed_value: str,
) -> DataFrame:
    """
    Generate fill-forward dataframe

    Parameters
    ----------
    df
        The input dataframe containing the columns to impute
    fill_forward_columns
        The list of columns to fill-forward
    participant_id_column
        The column to use to partition rows - typically this will be the participant id
    event_datetime_column
        The visit datetime column
    record_changed_column
        An indicator column that tells us whether a record needs to be fill-forward if it equals `record_changed_value
    record_changed_value
        See `record_changed_column` above

    """
    window = Window.partitionBy(participant_id_column).orderBy(F.col(event_datetime_column).asc())
    df = df.withColumn("ROW_NUMBER", F.row_number().over(window))

    fill_from_condition = ((F.col(record_changed_column) == record_changed_value)) | (F.col("ROW_NUMBER") == 1)
    df_fill_forwards_from = (
        df.where(fill_from_condition)
        .select(participant_id_column, event_datetime_column, *fill_forward_columns)
        .withColumnRenamed(participant_id_column, "id_right")
        .drop("ROW_NUMBER")
    )
    return df_fill_forwards_from


def fill_forward_from_last_change_process(
    df: DataFrame,
    fill_forward_columns: List[str],
    participant_id_column: str,
    event_datetime_column: str,
    df_fill_forwards_from: DataFrame,
) -> DataFrame:
    """
    Fill forwards, by time, a list of columns from records that are indicated to have changed.
    The first record for each participant is taken to be the first "change".

    All fields in `fill_forward_columns` are carried forwards, regardless of their value
    (i.e. includes filling forward Null).

    Parameters
    ----------
    fill_forward_columns
        list of column names to include in fill forwards
    participant_id_column
        column used to identify the group to fill within
    event_datetime_column
        column used to order for fill forwards
    record_changed_column
        column that indicates a change in the current record
    record_changed_value
        value in `record_changed_column` that indicates a change in the current record
    dateset_version_column
        column containing dataset version number
    minimum_dateset_version
        minimum dataset version that should be filled from
    """

    df_fill_forwards_from = df_fill_forwards_from.withColumnRenamed(event_datetime_column, "start_datetime")
    window_lag = Window.partitionBy("id_right").orderBy(F.col("start_datetime").asc())

    df_fill_forwards_from = df_fill_forwards_from.withColumn(
        "end_datetime", F.lead(F.col("start_datetime"), 1).over(window_lag)
    )
    df = df.drop(*fill_forward_columns, "ROW_NUMBER")

    df = df.join(
        df_fill_forwards_from,
        how="left",
        on=(
            (df[participant_id_column] == df_fill_forwards_from["id_right"])
            & (df[event_datetime_column] >= df_fill_forwards_from.start_datetime)
            & (
                (df[event_datetime_column] < df_fill_forwards_from.end_datetime)
                | (df_fill_forwards_from.end_datetime.isNull())
            )
        ),
    )
    return df.drop("id_right", "start_datetime", "end_datetime")


def fill_forward_from_last_change(
    df: DataFrame,
    fill_forward_columns: List[str],
    participant_id_column: str,
    event_datetime_column: str,
    record_changed_column: str,
    record_changed_value: str,
) -> DataFrame:
    """
    Call the fill forwards from last change function with a subset of the available rows
    to fill forward from. This set is generated by combining rows where record has changed and
    the first row of each participant.

    Parameters
    ----------
    fill_forward_columns
        list of column names to include in fill forwards
    participant_id_column
        column used to identify the group to fill within
    event_datetime_column
        column used to order for fill forwards
    record_changed_column
        column that indicates a change in the current record
    record_changed_value
        value in `record_changed_column` that indicates a change in the current record
    """
    df_fill_forwards_from = generate_fill_forward_df(
        df,
        fill_forward_columns,
        participant_id_column,
        event_datetime_column,
        record_changed_column,
        record_changed_value,
    )

    return fill_forward_from_last_change_process(
        df, fill_forward_columns, participant_id_column, event_datetime_column, df_fill_forwards_from
    )


def fill_forward_only_to_nulls(
    df: DataFrame,
    id: str,
    date: str,
    list_fill_forward: List[str],
) -> DataFrame:
    """
    Fill forward to not nulls for the whole dataset.
    """

    window = Window.partitionBy(id).orderBy(date)

    for fill_forward_column in list_fill_forward:
        df = df.withColumn(
            fill_forward_column,
            F.coalesce(
                F.last(fill_forward_column, ignorenulls=True).over(window),
            ),
        )
    return df


def fill_backwards_overriding_not_nulls(
    df: DataFrame,
    column_identity,
    ordering_column: str,
    dataset_column: str,
    column_list: List[str],
) -> DataFrame:
    """
    Fill/impute missing values working backwards from the latest known non-null value

    Parameters
    ----------
    df
    column_identity
        The column to use to partition records
    ordering_column
        The column to use to sort records within a partition
    dataset_column
        The colum that identifies which dataset we are dealing with
    column_list
        List of columns to impute
    """
    window = (
        Window.partitionBy(column_identity)
        .orderBy(F.col(dataset_column).desc(), F.col(ordering_column).desc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    for column in column_list:
        df = df.withColumn(
            column,
            F.first(F.col(column), ignorenulls=True).over(window),
        )
    return df


def impute_by_distribution(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    group_by_columns: List[str],
    first_imputation_value: Union[str, bool, int, float],
    second_imputation_value: Union[str, bool, int, float],
    rng_seed: int = None,
) -> DataFrame:
    """
    Calculate a imputation value from a missing value using a probability
    threshold determined by the proportion of a sub group.

    N.B. Defined only for imputing a binary column.

    Parameters
    ----------
    df
    column_name_to_assign
        The column that will be created with the imputed values
    reference_column
        The column for which imputation values will be calculated
    group_columns
        Grouping columns used to determine the proportion of the reference values
    first_imputation_value
        Imputation value if random number less than proportion
    second_imputation_value
        Imputation value if random number greater than or equal to proportion
    rng_seed
        Random number generator seed for making function deterministic.

    Notes
    -----
    Function provides a column value for each record that needs to be imputed.
    Where the value does not need to be imputed the column value created will be null.
    """
    # .rowsBetween(-sys.maxsize, sys.maxsize) fixes null issues for counting proportions
    window = Window.partitionBy(*group_by_columns).orderBy(reference_column).rowsBetween(-sys.maxsize, sys.maxsize)

    df = df.withColumn(
        "numerator", F.sum(F.when(F.col(reference_column) == first_imputation_value, 1).otherwise(0)).over(window)
    )

    df = df.withColumn("denominator", F.sum(F.when(F.col(reference_column).isNotNull(), 1).otherwise(0)).over(window))

    df = df.withColumn("proportion", F.col("numerator") / F.col("denominator"))

    df = df.withColumn("random", F.rand(rng_seed))

    df = df.withColumn(
        "individual_impute_value",
        F.when(F.col("proportion") > F.col("random"), first_imputation_value)
        .when(F.col("proportion") <= F.col("random"), second_imputation_value)
        .otherwise(None),
    )

    # Make flag easier (non null values will be flagged)
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(reference_column).isNull(), F.col("individual_impute_value")).otherwise(None),
    )

    return df.drop("proportion", "random", "denominator", "numerator", "individual_impute_value")


def impute_and_flag(df: DataFrame, imputation_function: Callable, reference_column: str, **kwargs) -> DataFrame:
    """
    Wrapper function for calling imputations, flagging imputed records and recording imputation methods.

    Parameters
    ----------
    df
    imputation_function
        The function that calculates the imputation for a given
        reference column
    reference_column
        The column that will be imputed
    **kwargs
        Key word arguments for imputation_function

    Notes
    -----
    imputation_function is expected to create new column with the values to
    be imputed, and NULL where imputation is not needed.
    """
    df = imputation_function(
        df, column_name_to_assign="temporary_imputation_values", reference_column=reference_column, **kwargs
    )
    status_column = reference_column + "_is_imputed"
    status_other = F.col(status_column) if status_column in df.columns else F.lit(None)
    imputed_this_run = F.col("temporary_imputation_values").isNotNull() & F.col(reference_column).isNull()

    df = df.withColumn(
        status_column,
        F.when(status_other == 1, 1).when(imputed_this_run, 1).otherwise(0).cast("integer"),
    )
    method_column = reference_column + "_imputation_method"
    method_other = F.col(method_column) if method_column in df.columns else None
    df = df.withColumn(
        method_column,
        F.when(imputed_this_run, imputation_function.__name__).otherwise(method_other).cast("string"),
    )
    df = df.withColumn(reference_column, F.coalesce(reference_column, "temporary_imputation_values"))

    return df.drop("temporary_imputation_values")


def impute_by_mode(df: DataFrame, column_name_to_assign: str, reference_column: str, group_by_column: str) -> DataFrame:
    """
    Get imputation value from given column by most commonly occurring value.
    Results in None when multiple modes are found.

    Parameters
    ----------
    df
    column_name_to_assign
        The column that will be created with the impute values
    reference_column
        The column to be imputed
    group_by_column
        Column name for the grouping

    Notes
    -----
    Function provides a column value for each record that needs to be imputed.
    Where the value does not need to be imputed the column value created will be null.
    """
    value_count_by_group = (
        df.groupBy(group_by_column, reference_column)
        .agg(F.count(reference_column).alias("_value_count"))
        .filter(F.col(reference_column).isNotNull())
    )

    group_window = Window.partitionBy(group_by_column)
    deduplicated_modes = (
        value_count_by_group.withColumn("_is_mode", F.col("_value_count") == F.max("_value_count").over(group_window))
        .filter(F.col("_is_mode"))
        .withColumn("_is_tied_mode", F.count(reference_column).over(group_window) > 1)
        .filter(~F.col("_is_tied_mode"))
        .withColumnRenamed(reference_column, "_imputed_value")
        .drop("_value_count", "_is_mode", "_is_tied_mode")
    )

    imputed_df = (
        df.join(deduplicated_modes, on=group_by_column, how="left")
        .withColumn(column_name_to_assign, F.when(F.col(reference_column).isNull(), F.col("_imputed_value")))
        .drop("_imputed_value")
    )

    return imputed_df


def impute_by_ordered_fill_forward(
    df: DataFrame,
    column_name_to_assign: str,
    column_identity: str,
    reference_column: str,
    order_by_column: str,
    order_type="asc",
) -> DataFrame:
    """
    Impute the last observation of a given field by given identity column.

    Parameters
    ----------
    df
    column_name_to_assign
        The column that will be created with the impute values
    column_identity
        Identifies any records that the reference_column is missing forward
        This column is normally intended for user_id, participant_id, etc.
    reference_column
        The column for which imputation values will be calculated.
    orderby_column
        the "direction" of the observation will be defined by a ordering column
        within the dataframe. For example: date.
    order_type
        the "direction" of the observation can be ascending by default or
        descending. Chose ONLY 'asc' or 'desc'.
    Notes
    ----
    If the observation carried forward by a specific column like date, and
        the type of order (order_type) is descending, the direction will be
        reversed and the function would do a last observation carried backwards.
    """
    if order_type == "asc":
        ordering_expression = F.col(order_by_column).asc()
    elif order_type == "desc":
        ordering_expression = F.col(order_by_column).desc()
    else:
        raise ValueError("order_type must be 'asc' or 'desc'")

    window = (
        Window.partitionBy(column_identity)
        .orderBy(ordering_expression)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(reference_column).isNull(), F.last(F.col(reference_column), ignorenulls=True).over(window)),
    )
    return df


def edit_multiple_columns_fill_forward(
    df: DataFrame, id, fill_if_null: str, date: str, column_fillforward_list: List[str]
) -> DataFrame:
    """
    This function does the same thing as impute_by_ordered_fill_forward() but fills forward a list of columns
    based on fill_if_null, if fill_if_null is null will fill forwards from late observation ordered by date column.

    Parameters
    ----------
    df
    id
        The column to use to partition records
    fill_if_null
        The column to impute if null
    date
        The column to order records by
    column_fillforward_list
        A list of column names to fill forward
    """
    window = Window.partitionBy(id).orderBy(date).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    for column_name in column_fillforward_list:
        df = df.withColumn(
            column_name,
            F.when(F.col(fill_if_null).isNull(), F.last(F.col(column_name), ignorenulls=True).over(window)).otherwise(
                F.col(column_name)
            ),
        )

    return df


def merge_previous_imputed_values(
    df: DataFrame,
    imputed_value_lookup_df: DataFrame,
    id_column_name: str,
) -> DataFrame:
    """
    Retrieve and coalesce imputed values and associated flags from a lookup table.
    Includes the imputed value, imputation status and imputation method.

    Keeps raw value if available, otherwise takes lookup value if it has been previously imputed.

    Parameters
    ----------
    df
        dataframe to impute values onto
    imputed_value_lookup_df
        previously imputed values to carry forward
    id_column_name
        column that should be used to join previously imputed values on
    """
    imputed_value_lookup_df = imputed_value_lookup_df.toDF(
        *[f"_{column}" if column != id_column_name else column for column in imputed_value_lookup_df.columns]
    )  # _ prevents ambiguity in join, but is sliced out when referencing the original columns

    df = df.join(imputed_value_lookup_df, on=id_column_name, how="left")
    columns_for_editing = [
        (column.replace("_imputation_method", ""), column.replace("_imputation_method", "_is_imputed"), column)
        for column in imputed_value_lookup_df.columns
        if column.endswith("_imputation_method")
    ]
    for value_column, status_column, method_column in columns_for_editing:
        fill_condition = (
            F.col(value_column[1:]).isNull()
            & F.col(value_column).isNotNull()
            & F.col(f"{value_column}_imputation_method").isNotNull()
        )
        df = df.withColumn(status_column[1:], F.when(fill_condition, F.lit(1)).otherwise(F.lit(0)))
        df = df.withColumn(
            method_column[1:], F.when(fill_condition, F.col(method_column)).otherwise(F.lit(None)).cast("string")
        )
        df = df.withColumn(
            value_column[1:], F.when(fill_condition, F.col(value_column)).otherwise(F.col(value_column[1:]))
        )

    return df.drop(*[name for name in imputed_value_lookup_df.columns if name != id_column_name])


def _create_log(start_time: datetime, log_path: str):
    """Create logger for logging KNN imputation details"""
    log = log_path + "/KNN_imputation_" + start_time.strftime("%d-%m-%Y %H:%M:%S") + ".log"
    logging.basicConfig(
        filename=log, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%d/%m/%Y %H:%M:%S"
    )
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.info("\n KNN imputation started")


def _validate_donor_group_variables(
    df, reference_column, donor_group_columns, donor_group_variable_weights, donor_group_variable_conditions
):
    """
    Validate that donor group column values are within given bounds and data type conditions.
    Also reports summary statistics for group variables.
    """
    # variable to impute not in required impute variables
    if reference_column in donor_group_columns:
        message = "Imputed variable should not be in given impute variables."
        logging.warning(message)
        raise ValueError(message)

    # impute variables and weights are the same length
    if len(donor_group_columns) != len(donor_group_variable_weights):
        message = "Impute weights needs to be the same length as given impute variables."
        logging.warning(message)
        raise ValueError(message)

    df_dtypes = dict(df.dtypes)
    for var in donor_group_variable_conditions.keys():
        if len(donor_group_variable_conditions[var]) != 3:
            message = f"Missing boundary conditions for {var}. Needs to be in format [Min, Max, Dtype]"
            logging.warning(message)
            raise ValueError(message)

        var_min, var_max, var_dtype = donor_group_variable_conditions[var]
        if var_dtype is not None:
            if var_dtype != df_dtypes[var]:
                logging.warning(f"{var} dtype is {df_dtypes[var]} and not the required {var_dtype}")

        if var_min is not None:
            var_min_count = df.filter(F.col(var) > var_min).count()
            if var_min_count > 0:
                logging.warning(f"{var_min_count} rows have {var} below {var_min}" % (var_min_count, var, var_min))

        if var_max is not None:
            var_max_count = df.filter(F.col(var) < var_max).count()
            if var_max_count > 0:
                logging.warning(f"{var_max_count} rows have {var} above {var_max}")

    logging.info("Summary statistics for donor group variables:")
    logging.info(df.select(donor_group_columns).summary().toPandas())


def weighted_distance(df, group_id, donor_group_columns, donor_group_column_weights):
    """
    Calculate weighted distance between donors and records to be imputed.
    Expects corresponding donor group columns to be prefixed with 'don_'.

    Parameters
    ----------
    df
    group_id
        ID for donor group
    donor_group_columns
        Columns used to assign donor group
    donor_group_variable_weights
        Weight to apply to distance of each group column
    """
    df = df.withColumn(
        "distance",
        sum(
            [
                (~F.col(var).eqNullSafe(F.col("don_" + var))).cast(DoubleType()) * donor_group_column_weights[i]
                for i, var in enumerate(donor_group_columns)
            ]
        ),
    )
    distance_window = Window.partitionBy(group_id).orderBy("distance")
    df = df.withColumn("min_distance", F.first("distance").over(distance_window))
    df = df.filter(F.col("distance") <= F.col("min_distance")).drop("min_distance")
    return df


def impute_by_k_nearest_neighbours(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    donor_group_columns: list,
    log_file_path: str,
    minimum_donors: int = 1,
    donor_group_column_weights: list = None,
    donor_group_column_conditions: dict = None,
    maximum_distance: int = 4999,
):
    """
    Minimal PySpark implementation of RBEIS, for K-nearest neighbours imputation.
    Uses a weighted distance to select unique groups of donors. Then applies probability
    proportional to size (PPS) sampling from final pool of donors.

    Parameters
    ----------
    df
    column_name_to_assign
        column to store imputed values
    reference_column
        column that missing values should be imputed for
    donor_group_columns
        variables used to form unique donor groups to impute from
    donor_group_column_weights
        list of weights per ``donor_group_variables``
    donor_group_column_conditions
        list of boundary and data type conditions per ``donor_group_variables``
        in the form "variable": [minimum, maximum, "dtype"]
    log_path
        location the log file is written to
    minimum_donors
        minimum number of donors required in each imputation pool, must be >= 0
    maximum_distance
        maximum sum weighted distance for a valid donor. Set to None for no maximum.

    Note
    ----
    Uses a broadcast join to apply selected values for imputation, so assumes that the number of values to be imputed
    is relatively low.
    """

    if reference_column not in df.columns:
        message = f"Variable to impute ({reference_column}) is not in in columns."
        raise ValueError(message)

    if not all(column in df.columns for column in donor_group_columns):
        message = f"Imputation columns ({donor_group_columns}) are not all found in input dataset."
        raise ValueError(message)

    to_impute_condition = F.col(reference_column).isNull()
    donor_df = df.filter(~to_impute_condition)
    donor_df = donor_df.withColumn("unique_donor_group", F.concat_ws("-", *donor_group_columns))
    # Leave this on the original df, for joining imputed values on
    df = df.withColumn("unique_imputation_group", F.when(to_impute_condition, F.concat_ws("-", *donor_group_columns)))
    imputing_df = df.filter(to_impute_condition)

    input_df_length = df.count()
    impute_count = imputing_df.count()
    donor_count = donor_df.count()

    assert (
        impute_count + donor_count == input_df_length
    ), "Donor and imputing records don't sum to the whole df length"  # noqa: E501

    if impute_count == 0:
        return df.withColumn(column_name_to_assign, F.lit(None).cast(df.schema[reference_column].dataType))
    _create_log(start_time=datetime.now(), log_path=log_file_path)
    logging.info(f"Function parameters:\n{locals()}")

    logging.info(f"Input dataframe length: {input_df_length}")
    logging.info(f"Records to impute: {impute_count}")
    logging.info(f"Donor records: {donor_count}")

    if donor_count < impute_count:
        message = "Overall number of donor records is less than the number of records to impute."
        logging.warning(message)
        raise ValueError(message)

    if donor_group_column_weights is None:
        donor_group_column_weights = [1] * len(donor_group_columns)
        logging.warning(f"No imputation weights specified, using default: {donor_group_column_weights}")

    if donor_group_column_conditions is None:
        donor_group_column_conditions = {var: [None, None, None] for var in donor_group_columns}
        logging.warning(f"No bounds for impute variables specified, using default: {donor_group_column_conditions}")

    _validate_donor_group_variables(
        df, reference_column, donor_group_columns, donor_group_column_weights, donor_group_column_conditions
    )

    imputing_df_unique = imputing_df.dropDuplicates(donor_group_columns).select(
        donor_group_columns + ["unique_imputation_group"]
    )
    donor_df_unique = donor_df.dropDuplicates(donor_group_columns).select(donor_group_columns + ["unique_donor_group"])

    for var in donor_group_columns + [reference_column]:
        donor_df_unique = donor_df_unique.withColumnRenamed(var, "don_" + var)
        donor_df = donor_df.withColumnRenamed(var, "don_" + var)

    joined_uniques = imputing_df_unique.crossJoin(donor_df_unique)
    joined_uniques = joined_uniques.repartition("unique_imputation_group")

    candidates = weighted_distance(
        joined_uniques, "unique_imputation_group", donor_group_columns, donor_group_column_weights
    ).select("unique_imputation_group", "unique_donor_group")
    if maximum_distance is not None:
        candidates = candidates.where(F.col("distance") <= maximum_distance)

    frequencies = donor_df.groupby("unique_donor_group", "don_" + reference_column).agg(
        F.count("*").alias("donor_group_value_frequency")
    )
    frequencies = frequencies.join(candidates, on="unique_donor_group")
    frequencies = frequencies.join(
        imputing_df.groupby("unique_imputation_group").agg(F.count("*").alias("imputation_group_size")),
        on="unique_imputation_group",
    )

    frequencies.cache().count()

    no_donors = imputing_df_unique.join(frequencies, on="unique_imputation_group", how="left_anti")
    no_donors_count = no_donors.count()
    if no_donors_count != 0:
        message = f"{no_donors_count} donor pools with no donors"
        logging.error(message)
        logging.error(no_donors.toPandas())
        raise ValueError(message)

    unique_imputation_group_window = Window.partitionBy("unique_imputation_group")
    frequencies = frequencies.withColumn(
        "total_donor_pool_size", F.sum("donor_group_value_frequency").over(unique_imputation_group_window)
    )

    below_minimum_donor_count = frequencies.filter(F.col("total_donor_pool_size") < minimum_donors)
    below_minimum_donor_count_count = below_minimum_donor_count.count()
    if below_minimum_donor_count_count > 0:
        message = (
            f"{below_minimum_donor_count_count} donor pools found with less than the required {minimum_donors} "
            "minimum donor(s)"
        )
        logging.error(message)
        logging.error(frequencies.filter(F.col("donor_group_value_frequency") < minimum_donors).toPandas())
        raise ValueError(message)

    frequencies = frequencies.withColumn(
        "probability", F.col("donor_group_value_frequency") / F.col("total_donor_pool_size")
    )

    frequencies = frequencies.withColumn("expected_frequency", F.col("probability") * F.col("imputation_group_size"))
    frequencies = frequencies.withColumn("expected_frequency_integer_part", F.floor(F.col("expected_frequency")))
    frequencies = frequencies.withColumn(
        "expected_frequency_decimal_part", F.col("expected_frequency") - F.col("expected_frequency_integer_part")
    )

    total_integer_part_window = Window.partitionBy("unique_imputation_group")
    frequencies = frequencies.withColumn(
        "total_expected_frequency_integer_part",
        F.sum(F.col("expected_frequency_integer_part")).over(total_integer_part_window),
    )

    frequencies = frequencies.withColumn(
        "required_decimal_donor_count", F.col("imputation_group_size") - F.col("total_expected_frequency_integer_part")
    )

    integer_part_donors = frequencies.select(
        "expected_frequency_integer_part", "unique_imputation_group", "don_" + reference_column
    )
    integer_part_donors = integer_part_donors.filter(F.col("expected_frequency_integer_part") >= 1)
    integer_part_donors = integer_part_donors.withColumn(
        "expected_frequency_integer_part",
        F.expr("explode(array_repeat(expected_frequency_integer_part, int(expected_frequency_integer_part)))"),
    )

    decimal_part_donors = frequencies.filter(F.col("expected_frequency_decimal_part") > 0).select(
        "expected_frequency_decimal_part",
        "unique_imputation_group",
        "don_" + reference_column,
        "required_decimal_donor_count",
    )

    rescale_window = Window.partitionBy("unique_imputation_group")

    decimal_part_donors = decimal_part_donors.withColumn(
        "expected_frequency_decimal_part_total", F.sum(F.col("expected_frequency_decimal_part")).over(rescale_window)
    )
    decimal_part_donors = decimal_part_donors.withColumn(
        "expected_frequency_decimal_part",
        F.col("expected_frequency_decimal_part") / F.col("expected_frequency_decimal_part_total"),
    )

    decimal_parts_grouped = decimal_part_donors.groupby("unique_imputation_group").agg(
        F.collect_list("don_" + reference_column).alias("don_" + reference_column),
        F.collect_list("expected_frequency_decimal_part").alias("expected_frequency_decimal_part"),
        F.first("required_decimal_donor_count").alias("required_decimal_donor_count"),
    )

    decimals_to_impute = decimal_parts_grouped.withColumn(
        "don_" + reference_column,
        sample_proportional_to_size_udf(
            "don_" + reference_column, "expected_frequency_decimal_part", "required_decimal_donor_count"
        ),
    )

    decimals_to_impute = decimals_to_impute.select("unique_imputation_group", "don_" + reference_column)

    decimal_part_donors = decimals_to_impute.withColumn(
        "don_" + reference_column, F.explode(F.col("don_" + reference_column))
    )

    to_impute_df = integer_part_donors.select("unique_imputation_group", "don_" + reference_column).unionByName(
        decimal_part_donors.select("unique_imputation_group", "don_" + reference_column)
    )
    rand_uniques_window = Window.partitionBy("unique_imputation_group").orderBy(F.rand())
    to_impute_df = to_impute_df.withColumn("donor_row_id", F.row_number().over(rand_uniques_window))

    to_impute_df = to_impute_df.withColumnRenamed("don_" + reference_column, column_name_to_assign)

    unique_imputation_group_window = Window.partitionBy("unique_imputation_group").orderBy(F.lit(None))
    df = df.withColumn("donor_row_id", F.row_number().over(unique_imputation_group_window))
    df = df.join(to_impute_df, on=["unique_imputation_group", "donor_row_id"], how="left").drop(
        "unique_imputation_group", "donor_row_id"
    )

    output_df_length = df.cache().count()
    logging.info(
        f"Summary statistics for imputed values ({column_name_to_assign}) and donor values ({reference_column}):"
    )
    logging.info(df.select(column_name_to_assign, reference_column).summary().toPandas())
    if output_df_length != input_df_length:
        raise ValueError(
            f"{output_df_length} records are found in the output, which is not equal to {input_df_length} in the input."  # noqa: E501
        )

    missing_count = df.filter(F.col(reference_column).isNull() & F.col(column_name_to_assign).isNull()).count()
    if missing_count != 0:
        raise ValueError(f"{missing_count} records still have missing '{reference_column}' after imputation.")

    logging.info("KNN imputation completed\n")
    return df


def impute_date_by_k_nearest_neighbours(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    donor_group_columns: List[str],
    log_file_path: str,
    minimum_donors: int = 1,
    donor_group_column_weights: list = None,
    donor_group_column_conditions: dict = None,
    maximum_distance: int = 4999,
) -> DataFrame:
    """Impute dates by K-nearest neighbour

    Parameters
    ----------
    df
    column_name_to_assign
    reference_column
    donor_group_columns
    log_file_path
    minimum_donors
    donor_group_column_weights
    donor_group_column_conditions
    maximum_distance

    See Also
    --------
    impute_by_k_nearest_neighbours - for a description of the arguments of this function

    """
    df = df.withColumn("_month", F.month(reference_column))
    df = df.withColumn("_year", F.year(reference_column))

    df = impute_by_k_nearest_neighbours(
        df=df,
        column_name_to_assign="_IMPUTED_month",
        reference_column="_month",
        donor_group_columns=donor_group_columns,
        log_file_path=log_file_path,
        minimum_donors=minimum_donors,
        donor_group_column_weights=donor_group_column_weights,
        donor_group_column_conditions=donor_group_column_conditions,
        maximum_distance=maximum_distance,
    ).custom_checkpoint()

    df = impute_by_k_nearest_neighbours(
        df=df,
        column_name_to_assign="_IMPUTED_year",
        reference_column="_year",
        donor_group_columns=donor_group_columns,
        log_file_path=log_file_path,
        minimum_donors=minimum_donors,
        donor_group_column_weights=donor_group_column_weights,
        donor_group_column_conditions=donor_group_column_conditions,
        maximum_distance=maximum_distance,
    ).custom_checkpoint()

    df = df.drop("_month", "_year")

    df = assign_random_day_in_month(
        df=df,
        column_name_to_assign=column_name_to_assign,
        month_column="_IMPUTED_month",
        year_column="_IMPUTED_year",
    )
    return df.drop("_IMPUTED_month", "_IMPUTED_year")


def post_imputation_wrapper(key_columns_imputed_df: DataFrame):
    """
    Prepare imputed value lookup and join imputed values onto survey responses.
    """
    imputed_columns = [
        column.replace("_imputation_method", "")
        for column in key_columns_imputed_df.columns
        if column.endswith("_imputation_method")
    ]
    imputed_values_df = key_columns_imputed_df.filter(
        any_column_not_null([f"{column}_imputation_method" for column in imputed_columns])
    )
    lookup_columns = chain(*[(column, f"{column}_imputation_method") for column in imputed_columns])
    new_imputed_value_lookup = imputed_values_df.select(
        "participant_id",
        *lookup_columns,
    )
    return new_imputed_value_lookup
