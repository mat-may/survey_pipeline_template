# flake8: noqa
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_raw_copies
from cishouseholds.derive import assign_taken_column
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import update_column_values_from_map


def derive_work_status_columns(df: DataFrame) -> DataFrame:
    work_status_dict = {
        "work_status_v0": {
            "5y and older in full-time education": "Student",
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "Student",
            # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",
            # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed",
            # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed",
            # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "Student",  # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working (unemployed, retired, long-term sick etc.)",
            "Retired (include doing voluntary work here)": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Looking for paid work and able to start": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Furloughed (temporarily not working)",
            # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Student",  # noqa: E501
            "Retired": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
            "Attending university (including if temporarily absent)": "Student",  # noqa: E501
            "Not working and not looking for work": "Not working (unemployed, retired, long-term sick etc.)",
            # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Student",  # noqa: E501
        },
        "work_status_v1": {
            "Child under 5y attending child care": "Child under 5y attending child care",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Child under 5y attending child care",
            # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Child under 5y attending child care",
            # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Child under 5y not attending child care",
            # noqa: E501
            "Child under 5y not attending child care": "Child under 5y not attending child care",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Child under 5y not attending child care",
            # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Employed and currently not working",
            # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed and currently working",
            # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working and not looking for work",
            # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working and not looking for work",
            "Not working and not looking for work": "Not working and not looking for work",  # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed and currently working",
            # noqa: E501
            "Retired (include doing voluntary work here)": "Retired",  # noqa: E501
            "Looking for paid work and able to start": "Looking for paid work and able to start",  # noqa: E501
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "5y and older in full-time education",
            # noqa: E501
            "Attending university (including if temporarily absent)": "5y and older in full-time education",
            # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "5y and older in full-time education",
            # noqa: E501
        },
        "work_status_v2": {
            "Retired (include doing voluntary work here)": "Retired",  # noqa: E501
            "Attending college or other further education provider (including apprenticeships) (including if temporarily absent)": "Attending college or FE (including if temporarily absent)",
            # noqa: E501
            "Attending university (including if temporarily absent)": "Attending university (including if temporarily absent)",
            # noqa: E501
            "Child under 5y attending child care": "Child under 4-5y attending child care",  # noqa: E501
            "Child under 5y attending nursery or pre-school or childminder": "Child under 4-5y attending child care",
            # noqa: E501
            "Child under 4-5y attending nursery or pre-school or childminder": "Child under 4-5y attending child care",
            # noqa: E501
            "Child under 5y not attending nursery or pre-school or childminder": "Child under 4-5y not attending child care",
            # noqa: E501
            "Child under 5y not attending child care": "Child under 4-5y not attending child care",  # noqa: E501
            "Child under 4-5y not attending nursery or pre-school or childminder": "Child under 4-5y not attending child care",
            # noqa: E501
            "4-5y and older at school/home-school (including if temporarily absent)": "4-5y and older at school/home-school",
            # noqa: E501
            "Employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Employed and currently not working",
            # noqa: E501
            "Employed and currently working (including if on leave or sick leave for less than 4 weeks)": "Employed and currently working",
            # noqa: E501
            "Not in paid work and not looking for paid work (include doing voluntary work here)": "Not working and not looking for work",
            # noqa: E501
            "Not working and not looking for work (including voluntary work)": "Not working and not looking for work",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently not working (e.g. on leave due to the COVID-19 pandemic (furloughed) or sick leave for 4 weeks or longer or maternity/paternity leave)": "Self-employed and currently not working",
            # noqa: E501
            "Self-employed and currently working (include if on leave or sick leave for less than 4 weeks)": "Self-employed and currently working",
            # noqa: E501
            "5y and older in full-time education": "4-5y and older at school/home-school",  # noqa: E501
        },
    }

    column_list = ["work_status_v0", "work_status_v1"]
    for column in column_list:
        df = df.withColumn(column, F.col("work_status_v2"))
        df = update_column_values_from_map(df=df, column_name_to_update=column, map=work_status_dict[column])

    df = update_column_values_from_map(
        df=df, column_name_to_update="work_status_v2", map=work_status_dict["work_status_v2"]
    )
    return df


def clean_survey_responses_version_2(df: DataFrame) -> DataFrame:

    column_editing_map = {
        "work_location": {
            "Work from home (in the same grounds or building as your home)": "Working from home",
            "Working from home (in the same grounds or building as your home)": "Working from home",
            "From home (in the same grounds or building as your home)": "Working from home",
            "Work somewhere else (not your home)": "Working somewhere else (not your home)",
            "Somewhere else (not at your home)": "Working somewhere else (not your home)",
            "Somewhere else (not your home)": "Working somewhere else (not your home)",
            "Both (working from home and working somewhere else)": "Both (from home and somewhere else)",
            "Both (work from home and work somewhere else)": "Both (from home and somewhere else)",
        },
        "work_sector": {
            "Social Care": "Social care",
            "Transport (incl. storage or logistic)": "Transport (incl. storage, logistic)",
            "Transport (incl. storage and logistic)": "Transport (incl. storage, logistic)",
            "Transport (incl. storage and logistics)": "Transport (incl. storage, logistic)",
            "Retail Sector (incl. wholesale)": "Retail sector (incl. wholesale)",
            "Hospitality (e.g. hotel or restaurant or cafe)": "Hospitality (e.g. hotel, restaurant)",
            "Food Production and agriculture (incl. farming)": "Food production, agriculture, farming",
            "Food production and agriculture (incl. farming)": "Food production, agriculture, farming",
            "Personal Services (e.g. hairdressers or tattooists)": "Personal services (e.g. hairdressers)",
            "Information technology and communication": "Information technology and communication",
            "Financial services (incl. insurance)": "Financial services incl. insurance",
            "Financial Services (incl. insurance)": "Financial services incl. insurance",
            "Civil Service or Local Government": "Civil service or Local Government",
            "Arts or Entertainment or Recreation": "Arts,Entertainment or Recreation",
            "Art or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Arts or entertainment or recreation": "Arts,Entertainment or Recreation",
            "Other employment sector (specify)": "Other occupation sector",
            "Other occupation sector (specify)": "Other occupation sector",
        },
        "work_health_care_area": {
            "Primary Care (e.g. GP or dentist)": "Primary",
            "Primary care (e.g. GP or dentist)": "Primary",
            "Secondary Care (e.g. hospital)": "Secondary",
            "Secondary care (e.g. hospital.)": "Secondary",
            "Secondary care (e.g. hospital)": "Secondary",
            "Other Healthcare (e.g. mental health)": "Other",
            "Other healthcare (e.g. mental health)": "Other",
            "Participant Would Not/Could Not Answer": None,
            "Primary care for example in a GP or dentist": "Primary",
        },
        "face_covering_outside_of_home": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes at work/school only": "Yes, at work/school only",
            "Yes in other situations only (including public transport/shops)": "Yes, in other situations only",
            "Yes usually both at work/school and in other situations": "Yes, usually both Work/school/other",
            "Yes in other situations only (including public transport or shops)": "Yes, in other situations only",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "face_covering_work_or_education": {
            "My face is already covered for other reasons (e.g. religious or cultural reasons)": "My face is already covered",
            "Yes always": "Yes, always",
            "Yes sometimes": "Yes, sometimes",
        },
        "ability_to_socially_distance_at_work_or_education": {
            "Difficult to maintain 2 meters - but I can usually be at least 1m from other people": "Difficult to maintain 2m, but can be 1m",
            "Difficult to maintain 2m - but you can usually be at least 1m from other people": "Difficult to maintain 2m, but can be 1m",
            "Easy to maintain 2 meters - it is not a problem to stay this far away from other people": "Easy to maintain 2m",
            "Easy to maintain 2m - it is not a problem to stay this far away from other people": "Easy to maintain 2m",
            "Relatively easy to maintain 2 meters - most of the time I can be 2m away from other people": "Relatively easy to maintain 2m",
            "Relatively easy to maintain 2m - most of the time you can be 2m away from other people": "Relatively easy to maintain 2m",
            "Very difficult to be more than 1 meter away as my work means I am in close contact with others on a regular basis": "Very difficult to be more than 1m away",
            "Very difficult to be more than 1m away as your work means you are in close contact with others on a regular basis": "Very difficult to be more than 1m away",
        },
        "transport_to_work_or_education": {
            "Bus or Minibus or Coach": "Bus, minibus, coach",
            "Bus or minibus or coach": "Bus, minibus, coach",
            "Bus": "Bus, minibus, coach",
            "Motorbike or Scooter or Moped": "Motorbike, scooter or moped",
            "Motorbike or scooter or moped": "Motorbike, scooter or moped",
            "Car or Van": "Car or van",
            "Taxi/Minicab": "Taxi/minicab",
            "On Foot": "On foot",
            "Underground or Metro or Light Rail or Tram": "Underground, metro, light rail, tram",
            "Other Method": "Other method",
        },
    }
    df = apply_value_map_multiple_columns(df, column_editing_map)

    df = df.withColumn("swab_sample_barcode", F.upper(F.col("swab_sample_barcode")))
    return df


def transform_survey_responses_version_2_delta(df: DataFrame) -> DataFrame:
    """
    Transformations that are specific to version 2 survey responses.
    """
    raw_copy_list = ["cis_covid_vaccine_number_of_doses"]

    df = assign_raw_copies(df, [column for column in raw_copy_list if column in df.columns])

    df = assign_taken_column(df=df, column_name_to_assign="swab_taken", reference_column="swab_sample_barcode")

    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 2)

    df = derive_work_status_columns(df)
    return df
