import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from survey_pipeline_template.derive import assign_column_uniform_value
from survey_pipeline_template.derive import assign_column_value_from_multiple_column_map
from survey_pipeline_template.derive import assign_date_from_filename
from survey_pipeline_template.derive import assign_datetime_from_combined_columns
from survey_pipeline_template.derive import assign_raw_copies
from survey_pipeline_template.derive import map_options_to_bool_columns
from survey_pipeline_template.edit import add_prefix
from survey_pipeline_template.edit import apply_value_map_multiple_columns
from survey_pipeline_template.pipeline.mapping import transformation_maps


def example_survey_response_data_v1_transformations(df: DataFrame) -> DataFrame:
    """
    Wrapper function for example survey response data v1 transformations
    """
    df = pre_processing(df)
    df = derive_additional_columns(df)
    return df


def pre_processing(df: DataFrame) -> DataFrame:
    """
    Sets categories to map for digital specific variables to Voyager 0/1/2 equivalent
    """
    phm_free_text_columns = [
        "work_main_job_title",
        "work_main_job_role",
        "work_sector_other",
        "cis_covid_vaccine_type_other",
    ]
    for col in phm_free_text_columns:
        df = df.withColumn(col, F.regexp_replace(col, r"[\r\n]", ""))

    df = df.dropDuplicates(
        [col for col in df.columns if col not in ["survey_response_source_file", "backup_source_file"]]
    )

    raw_copy_list = [
        "work_sector",
        "ability_to_socially_distance_at_work_or_education",
        "face_covering_work_or_education",
        "cis_covid_vaccine_type",
        "cis_covid_vaccine_type_other",
        "cis_covid_vaccine_number_of_doses",
    ]

    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])

    vaccine_type_map = {
        "Pfizer / BioNTech": "Pfizer/BioNTech",
        "Oxford / AstraZeneca": "Oxford/AstraZeneca",
        "Janssen / Johnson&Johnson": "Janssen/Johnson&Johnson",
        "Another vaccine please specify": "Other / specify",
        "I don't know the type": "Don't know type",
        "Or Another vaccine please specify": "Other / specify",  # changed from "Other /specify"
        "I do not know the type": "Don't know type",
        "Or do you not know which one you had?": "Don't know type",
        "I don&#39;t know the type": "Don't know type",
        "I don&amp;#39;t know the type": "Don't know type",
        "I dont know the type": "Don't know type",
        "Janssen / Johnson&amp;Johnson": "Janssen/Johnson&Johnson",
        "Janssen / Johnson&amp;amp;Johnson": "Janssen/Johnson&Johnson",
        "Oxford also known as AstraZeneca": "Oxford/AstraZeneca",
        "Pfizer also known as BioNTech": "Pfizer/BioNTech",
    }

    column_editing_map = {
        "work_status_employment": {
            "Currently working - this includes if you are on sick leave or other leave for less than 4 weeks": "Currently working. This includes if you are on sick or other leave for less than 4 weeks"
        },
        "work_sector": {
            "Social Care": "Social care",
            "Transport. This includes storage and logistics": "Transport (incl. storage, logistic)",
            "Retail sector. This includes wholesale": "Retail sector (incl. wholesale)",
            "Hospitality - for example hotels or restaurants or cafe": "Hospitality (e.g. hotel, restaurant)",
            "Food production and agriculture. This includes farming": "Food production, agriculture, farming",
            "Personal Services - for example hairdressers or tattooists": "Personal services (e.g. hairdressers)",
            "Information technology and communication": "Information technology and communication",
            "Financial services. This includes insurance": "Financial services incl. insurance",
            "Civil Service or Local Government": "Civil service or Local Government",
            "Arts or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Other employment sector please specify": "Other occupation sector",
        },
        "work_health_care_area": {
            "Secondary care for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services?": "Other",
            "Primary care - for example in a GP or dentist": "Primary",
            "Yes, in primary care, e.g. GP, dentist": "Primary",
            "Secondary care - for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services": "Other",  # noqa: E501
        },
        "work_location": {
            "From home meaning in the same grounds or building as your home": "Working from home",
            "Somewhere else meaning not at your home": "Working somewhere else (not your home)",
            "Both from home and work somewhere else": "Both (from home and somewhere else)",
        },
        "transport_to_work_or_education": {
            "Bus or minibus or coach": "Bus, minibus, coach",
            "Motorbike or scooter or moped": "Motorbike, scooter or moped",
            "Taxi or minicab": "Taxi/minicab",
            "Underground or Metro or Light Rail or Tram": "Underground, metro, light rail, tram",
        },
        "ability_to_socially_distance_at_work_or_education": {
            "Difficult to maintain 2 metres apart. But you can usually be at least 1 metre away from other people": "Difficult to maintain 2m, but can be 1m",
            # noqa: E501
            "Easy to maintain 2 metres apart. It is not a problem to stay this far away from other people": "Easy to maintain 2m",
            # noqa: E501
            "Relatively easy to maintain 2 metres apart. Most of the time you can be 2 meters away from other people": "Relatively easy to maintain 2m",
            # noqa: E501
            "Very difficult to be more than 1 metre away. Your work means you are in close contact with others on a regular basis": "Very difficult to be more than 1m away",
        },
        "face_covering_work_or_education": {
            "Prefer not to say": None,
            "Yes sometimes": "Yes, sometimes",
            "Yes always": "Yes, always",
            "I am not going to my place of work or education": "Not going to place of work or education",
            "I cover my face for other reasons - for example for religious or cultural reasons": "My face is already covered",
            # noqa: E501
        },
        "cis_covid_vaccine_type": vaccine_type_map,
    }

    df = apply_value_map_multiple_columns(df, column_editing_map)

    df = assign_datetime_from_combined_columns(
        df=df,
        column_name_to_assign="swab_taken_datetime",
        date_column="swab_taken_date",
        hour_column="swab_taken_time_hour",
        minute_column="swab_taken_time_minute",
        am_pm_column="swab_taken_am_pm",
    )

    df = add_prefix(df, column_name_to_update="swab_sample_barcode_user_entered", prefix="SWT")
    return df


def derive_additional_columns(df: DataFrame) -> DataFrame:
    """
    New columns:
    - visit_datetime
    - visit_date
    - visit_id
    - think_have_covid_any_symptoms
    - think_have_any_symptoms_new_or_worse
    - think_have_long_covid_any_symptoms
    - think_had_covid_any_symptoms
    - think_had_flu_any_symptoms
    - think_had_other_infection_any_symptoms
    - think_have_covid_any_symptom_list
    - think_have_symptoms_new_or_worse_list
    - think_have_long_covid_symptom_list
    - think_had_covid_any_symptom_list
    - think_had_other_infection_symptom_list
    - think_had_flu_symptom_list
    - work_status_v2
    - work_status_v1
    - work_status_v0
    - swab_sample_barcode_user_entered
    - blood_sample_barcode_user_entered
    - times_outside_shopping_or_socialising_last_7_days
    - face_covering_outside_of_home
    - cis_covid_vaccine_number_of_doses
    - from_date

    Reference columns:
    - participant_completion_window_id
    - survey_completion_status_flushed
    - survey_completed_datetime
    - currently_smokes_or_vapes_description
    - blood_not_taken_could_not_reason
    - transport_shared_outside_household_last_28_days
    - phm_think_had_respiratory_infection_type
    - think_have_covid_any_symptom_list_1
    - think_have_covid_any_symptom_list_2
    - think_have_symptoms_new_or_worse_list_1
    - think_have_symptoms_new_or_worse_list_2
    - think_have_long_covid_symptom_list_1
    - think_have_long_covid_symptom_list_2
    - think_have_long_covid_symptom_list_3
    - think_had_covid_any_symptom_list_1
    - think_had_covid_any_symptom_list_2
    - think_had_other_infection_symptom_list_1
    - think_had_other_infection_symptom_list_2
    - think_had_flu_symptom_list_1
    - think_had_flu_symptom_list_2

    Edited columns:
    - work_not_from_home_days_per_week
    - phm_covid_vaccine_number_of_doses
    """
    df = df.withColumn("visit_id", F.col("participant_completion_window_id"))
    df = df.withColumn("visit_datetime", F.col("survey_completed_datetime"))
    df = assign_date_from_filename(df, "file_date", "survey_response_source_file")
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 1)
    df = df.withColumn("visit_date", F.to_timestamp(F.to_date(F.col("visit_datetime"))))

    map_to_bool_columns_dict = {
        "think_have_covid_any_symptom_list_1": "think_have_covid",
        "think_have_covid_any_symptom_list_2": "think_have_covid",
        "think_had_covid_any_symptom_list_1": "think_had_covid",
        "think_had_covid_any_symptom_list_2": "think_had_covid",
    }
    for col_to_map, prefix in map_to_bool_columns_dict.items():
        if ("symptom" in col_to_map) & ("list_" in col_to_map):
            if "long_covid" in col_to_map:
                dict_to_retrieve = f"long_covid_symptoms_list_{col_to_map[-1:]}"
            else:
                dict_to_retrieve = f"symptoms_list_{col_to_map[-1:]}"
            value_column_map = {key: prefix + value for key, value in transformation_maps[dict_to_retrieve].items()}
        else:
            value_column_map = transformation_maps[col_to_map]
        df = df.withColumn(col_to_map, F.regexp_replace(col_to_map, r"[^a-zA-Z0-9\^,\- ]", "")).withColumn(
            col_to_map, F.regexp_replace(col_to_map, r", ", ";")
        )
        df = map_options_to_bool_columns(df, col_to_map, value_column_map, ";")

    column_list = ["work_status_digital", "work_status_employment", "work_status_unemployment", "work_status_education"]
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v2",
        [
            [
                "Employed and currently working",
                [
                    "Employed",
                    [
                        "Currently not working due to sickness lasting 4 weeks or more",
                        "Currently not working for other reasons such as maternity or paternity lasting 4 weeks or more",
                    ],
                    None,
                    None,
                ],
            ],
            [
                "Employed and currently not working",
                [
                    "Employed",
                    [
                        "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                        # noqa: E501
                        "Or currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks?",
                        # noqa: E501
                    ],
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently working",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently not working",
                [
                    "Self-employed",
                    [
                        "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                        # noqa: E501
                        "Or currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks?",
                        # noqa: E501
                    ],
                    None,
                    None,
                ],
            ],
            [
                "Looking for paid work and able to start",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working and not looking for work",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    [
                        "Not looking for paid work due to long-term sickness or disability",
                        "Not looking for paid work for reasons such as looking after the home or family or not wanting a job",
                    ],
                    # noqa: E501
                    None,
                ],
            ],
            [
                "Retired",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Retired", "Or retired?"],
                    None,
                ],
            ],
            [
                "Child under 4-5y not attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and not attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "Child under 4-5y attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and attending a nursery or a pre-school or childminder",
                ],
            ],
            [
                "4-5y and older at school/home-school",
                [
                    ["In education", None],
                    None,
                    None,
                    ["A child aged 4 or over at school", "A child aged 4 or over at home-school"],
                ],
            ],
            [
                "Attending college or FE (including if temporarily absent)",
                [
                    ["In education", None],
                    None,
                    None,
                    "Attending a college or other further education provider including apprenticeships",
                ],
            ],
            [
                "Attending university (including if temporarily absent)",
                [
                    ["In education", None],
                    None,
                    None,
                    ["Attending university", "Or attending university?"],
                ],
            ],
        ],
        column_list,
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v1",
        [
            [
                "Employed and currently working",
                [
                    "Employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Employed and currently not working",
                [
                    "Employed",
                    [
                        "Currently not working due to sickness lasting 4 weeks or more",
                        "Currently not working for other reasons such as maternity or paternity lasting 4 weeks or more",
                    ],
                    # noqa: E501
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently working",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Self-employed and currently not working",
                [
                    "Self-employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Looking for paid work and able to start",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working and not looking for work",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    [
                        "Not looking for paid work due to long-term sickness or disability",
                        "Not looking for paid work for reasons such as looking after the home or family or not wanting a job",
                    ],
                    None,
                ],
            ],
            [
                "Retired",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Or retired?", "Retired"],
                    None,
                ],
            ],
            [
                "Child under 5y not attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and not attending a nursery or pre-school or childminder",
                ],
            ],
            [
                "Child under 5y attending child care",
                [
                    ["In education", None],
                    None,
                    None,
                    "A child below school age and attending a nursery or a pre-school or childminder",
                ],
            ],
            [
                "5y and older in full-time education",
                [
                    ["In education", None],
                    None,
                    None,
                    [
                        "A child aged 4 or over at school",
                        "A child aged 4 or over at home-school",
                        "Attending a college or other further education provider including apprenticeships",
                        "Attending university",
                    ],
                ],
            ],
        ],
        column_list,
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_status_v0",
        [
            [
                "Employed",
                [
                    "Employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Employed",
                    [
                        "Currently not working due to sickness lasting 4 weeks or more",
                        "Currently not working for other reasons such as maternity or paternity lasting 4 weeks or more",
                    ],
                    # noqa: E501
                    None,
                    None,
                ],
            ],
            ["Employed", ["Employed", None, None, None]],
            [
                "Self-employed",
                [
                    "Self-employed",
                    "Currently working. This includes if you are on sick or other leave for less than 4 weeks",
                    None,
                    None,
                ],
            ],
            ["Self-employed", ["Self-employed", None, None, None]],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Self-employed",
                    "Currently not working -  for example on sick or other leave such as maternity or paternity for longer than 4 weeks",
                    # noqa: E501,
                    None,
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    "Looking for paid work and able to start",
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    [
                        "Not looking for paid work due to long-term sickness or disability",
                        "Not looking for paid work for reasons such as looking after the home or family or not wanting a job",
                    ],
                    # noqa: E501
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    ["Retired", "Or retired?"],
                    None,
                ],
            ],
            [
                "Not working (unemployed, retired, long-term sick etc.)",
                [
                    "Not in paid work. This includes being unemployed or retired or doing voluntary work",
                    None,
                    None,
                    None,
                ],
            ],
            [
                "Student",
                [
                    ["In education", None],
                    None,
                    None,
                    [
                        "A child below school age and not attending a nursery or pre-school or childminder",
                        "A child below school age and attending a nursery or a pre-school or childminder",
                        "A child aged 4 or over at school",
                        "A child aged 4 or over at home-school",
                        "Attending a college or other further education provider including apprenticeships",
                        "Attending university",
                    ],
                ],
            ],
            ["Student", ["In education", None, None, None]],
        ],
        column_list,
    )

    df = df.withColumn(
        "work_not_from_home_days_per_week",
        F.greatest("work_not_from_home_days_per_week", "education_in_person_days_per_week"),
    )

    return df
