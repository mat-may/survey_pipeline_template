from pyspark.sql import DataFrame

from cishouseholds.derive import assign_condition_around_event
from cishouseholds.derive import assign_date_difference
from cishouseholds.derive import assign_true_if_any
from cishouseholds.derive import count_value_occurrences_in_column_subset_row_wise
from cishouseholds.edit import nullify_columns_before_date


def covid_event_transformations(df: DataFrame) -> DataFrame:
    """Apply all transformations related to covid event columns in order."""
    df = derive_new_columns(df)
    df = data_dependent_derivations(df)
    return df


def derive_new_columns(df: DataFrame) -> DataFrame:
    """
    New columns:
    - days_since_think_had_covid
    - days_since_think_had_covid_group
    - think_have_covid_symptom_count
    - think_had_covid_symptom_count
    - think_have_covid_symptom_any
    - think_have_covid_cghfevamn_symptom_group
    - any_think_have_covid_symptom_or_now

    Reference columns:
    - think_had_covid_symptom_*
    """
    df = assign_date_difference(df, "days_since_think_had_covid", "think_had_covid_onset_date", "visit_datetime")

    original_think_have_symptoms = [
        "think_have_covid_symptom_fever",
        "think_have_covid_symptom_muscle_ache",
        "think_have_covid_symptom_fatigue",
        "think_have_covid_symptom_sore_throat",
        "think_have_covid_symptom_cough",
        "think_have_covid_symptom_shortness_of_breath",
        "think_have_covid_symptom_headache",
        "think_have_covid_symptom_nausea_or_vomiting",
        "think_have_covid_symptom_abdominal_pain",
        "think_have_covid_symptom_diarrhoea",
        "think_have_covid_symptom_loss_of_taste",
        "think_have_covid_symptom_loss_of_smell",
        "think_have_covid_symptom_more_trouble_sleeping",
        "think_have_covid_symptom_runny_nose_or_sneezing",
        "think_have_covid_symptom_noisy_breathing",
        "think_have_covid_symptom_loss_of_appetite",
    ]
    df = count_value_occurrences_in_column_subset_row_wise(
        df=df,
        column_name_to_assign="think_have_covid_symptom_count",
        selection_columns=original_think_have_symptoms,
        count_if_value="Yes",
    )
    df = count_value_occurrences_in_column_subset_row_wise(
        df=df,
        column_name_to_assign="think_had_covid_symptom_count",
        selection_columns=[
            "think_had_covid_symptom_fever",
            "think_had_covid_symptom_muscle_ache",
            "think_had_covid_symptom_fatigue",
            "think_had_covid_symptom_sore_throat",
            "think_had_covid_symptom_cough",
            "think_had_covid_symptom_shortness_of_breath",
            "think_had_covid_symptom_headache",
            "think_had_covid_symptom_nausea_or_vomiting",
            "think_had_covid_symptom_abdominal_pain",
            "think_had_covid_symptom_diarrhoea",
            "think_had_covid_symptom_loss_of_taste",
            "think_had_covid_symptom_loss_of_smell",
            "think_had_covid_symptom_more_trouble_sleeping",
            "think_had_covid_symptom_runny_nose_or_sneezing",
            "think_had_covid_symptom_noisy_breathing",
            "think_had_covid_symptom_loss_of_appetite",
        ],
        count_if_value="Yes",
    )
    df = assign_true_if_any(
        df=df,
        column_name_to_assign="any_think_have_covid_symptom_or_now",
        reference_columns=["think_have_covid_symptom_any", "think_have_covid"],
        true_false_values=["Yes", "No"],
    )

    df = assign_true_if_any(
        df=df,
        column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
        reference_columns=[
            "think_have_covid_symptom_cough",
            "think_have_covid_symptom_fever",
            "think_have_covid_symptom_loss_of_smell",
            "think_have_covid_symptom_loss_of_taste",
        ],
        true_false_values=["Yes", "No"],
    )

    df = assign_true_if_any(
        df=df,
        column_name_to_assign="think_had_covid_cghfevamn_symptom_group",
        reference_columns=[
            "think_had_covid_symptom_cough",
            "think_had_covid_symptom_fever",
            "think_had_covid_symptom_loss_of_smell",
            "think_had_covid_symptom_loss_of_taste",
        ],
        true_false_values=["Yes", "No"],
    )

    df = assign_condition_around_event(
        df=df,
        column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
        id_column="participant_id",
        condition_bool_column="think_have_covid_cghfevamn_symptom_group",
        event_date_column="visit_datetime",
        event_id_column="visit_id",
    )
    return df


def data_dependent_derivations(df: DataFrame) -> DataFrame:
    df = assign_condition_around_event(
        df=df,
        column_name_to_assign="any_symptoms_around_visit",
        condition_bool_column="any_think_have_covid_symptom_or_now",
        id_column="participant_id",
        event_date_column="visit_datetime",
        event_id_column="visit_id",
    )
    df = assign_condition_around_event(
        df=df,
        column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
        condition_bool_column="think_have_covid_cghfevamn_symptom_group",
        id_column="participant_id",
        event_date_column="visit_datetime",
        event_id_column="visit_id",
    )
    df = nullify_columns_before_date(
        df,
        column_list=[
            "think_had_covid_symptom_loss_of_appetite",
            "think_had_covid_symptom_runny_nose_or_sneezing",
            "think_had_covid_symptom_noisy_breathing",
            "think_had_covid_symptom_more_trouble_sleeping",
        ],
        date_column="visit_datetime",
        date="2021-08-26",
    )
    df = nullify_columns_before_date(
        df,
        column_list=[
            "think_had_covid_symptom_chest_pain",
            "think_had_covid_symptom_difficulty_concentrating",
            "think_had_covid_symptom_low_mood",
            "think_had_covid_symptom_memory_loss_or_confusion",
            "think_had_covid_symptom_palpitations",
            "think_had_covid_symptom_vertigo_or_dizziness",
            "think_had_covid_symptom_anxiety",
        ],
        date_column="visit_datetime",
        date="2022-01-26",
    )
    return df
