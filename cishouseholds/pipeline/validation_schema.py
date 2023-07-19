from typing import Any
from typing import Dict

validation_schemas: Dict[str, Dict[str, Any]]

string_dict = {"type": "string"}

integer_dict = {"type": "integer"}

long_dict = {"type": "long"}

float_dict = {"type": "double"}

bool_dict = {"type": "boolean"}

array_string_dict = {"type": "array<string>"}

swab_allowed_pcr_results = ["Inconclusive", "Negative", "Positive", "Rejected"]

projections_column_map = {
    "laname_21": string_dict,
    "ladcode_21": string_dict,
    "region9charcode": string_dict,
    "regionname": string_dict,
    "country9charcode_09": string_dict,
    "countryname_09": string_dict,
    "m0": integer_dict,
    "m1": integer_dict,
    "m2": integer_dict,
    "m3": integer_dict,
    "m4": integer_dict,
    "m5": integer_dict,
    "m6": integer_dict,
    "m7": integer_dict,
    "m8": integer_dict,
    "m9": integer_dict,
    "m10": integer_dict,
    "m11": integer_dict,
    "m12": integer_dict,
    "m13": integer_dict,
    "m14": integer_dict,
    "m15": integer_dict,
    "m16": integer_dict,
    "m17": integer_dict,
    "m18": integer_dict,
    "m19": integer_dict,
    "m20": integer_dict,
    "m21": integer_dict,
    "m22": integer_dict,
    "m23": integer_dict,
    "m24": integer_dict,
    "m25": integer_dict,
    "m26": integer_dict,
    "m27": integer_dict,
    "m28": integer_dict,
    "m29": integer_dict,
    "m30": integer_dict,
    "m31": integer_dict,
    "m32": integer_dict,
    "m33": integer_dict,
    "m34": integer_dict,
    "m35": integer_dict,
    "m36": integer_dict,
    "m37": integer_dict,
    "m38": integer_dict,
    "m39": integer_dict,
    "m40": integer_dict,
    "m41": integer_dict,
    "m42": integer_dict,
    "m43": integer_dict,
    "m44": integer_dict,
    "m45": integer_dict,
    "m46": integer_dict,
    "m47": integer_dict,
    "m48": integer_dict,
    "m49": integer_dict,
    "m50": integer_dict,
    "m51": integer_dict,
    "m52": integer_dict,
    "m53": integer_dict,
    "m54": integer_dict,
    "m55": integer_dict,
    "m56": integer_dict,
    "m57": integer_dict,
    "m58": integer_dict,
    "m59": integer_dict,
    "m60": integer_dict,
    "m61": integer_dict,
    "m62": integer_dict,
    "m63": integer_dict,
    "m64": integer_dict,
    "m65": integer_dict,
    "m66": integer_dict,
    "m67": integer_dict,
    "m68": integer_dict,
    "m69": integer_dict,
    "m70": integer_dict,
    "m71": integer_dict,
    "m72": integer_dict,
    "m73": integer_dict,
    "m74": integer_dict,
    "m75": integer_dict,
    "m76": integer_dict,
    "m77": integer_dict,
    "m78": integer_dict,
    "m79": integer_dict,
    "m80": integer_dict,
    "m81": integer_dict,
    "m82": integer_dict,
    "m83": integer_dict,
    "m84": integer_dict,
    "m85": integer_dict,
    "f0": integer_dict,
    "f1": integer_dict,
    "f2": integer_dict,
    "f3": integer_dict,
    "f4": integer_dict,
    "f5": integer_dict,
    "f6": integer_dict,
    "f7": integer_dict,
    "f8": integer_dict,
    "f9": integer_dict,
    "f10": integer_dict,
    "f11": integer_dict,
    "f12": integer_dict,
    "f13": integer_dict,
    "f14": integer_dict,
    "f15": integer_dict,
    "f16": integer_dict,
    "f17": integer_dict,
    "f18": integer_dict,
    "f19": integer_dict,
    "f20": integer_dict,
    "f21": integer_dict,
    "f22": integer_dict,
    "f23": integer_dict,
    "f24": integer_dict,
    "f25": integer_dict,
    "f26": integer_dict,
    "f27": integer_dict,
    "f28": integer_dict,
    "f29": integer_dict,
    "f30": integer_dict,
    "f31": integer_dict,
    "f32": integer_dict,
    "f33": integer_dict,
    "f34": integer_dict,
    "f35": integer_dict,
    "f36": integer_dict,
    "f37": integer_dict,
    "f38": integer_dict,
    "f39": integer_dict,
    "f40": integer_dict,
    "f41": integer_dict,
    "f42": integer_dict,
    "f43": integer_dict,
    "f44": integer_dict,
    "f45": integer_dict,
    "f46": integer_dict,
    "f47": integer_dict,
    "f48": integer_dict,
    "f49": integer_dict,
    "f50": integer_dict,
    "f51": integer_dict,
    "f52": integer_dict,
    "f53": integer_dict,
    "f54": integer_dict,
    "f55": integer_dict,
    "f56": integer_dict,
    "f57": integer_dict,
    "f58": integer_dict,
    "f59": integer_dict,
    "f60": integer_dict,
    "f61": integer_dict,
    "f62": integer_dict,
    "f63": integer_dict,
    "f64": integer_dict,
    "f65": integer_dict,
    "f66": integer_dict,
    "f67": integer_dict,
    "f68": integer_dict,
    "f69": integer_dict,
    "f70": integer_dict,
    "f71": integer_dict,
    "f72": integer_dict,
    "f73": integer_dict,
    "f74": integer_dict,
    "f75": integer_dict,
    "f76": integer_dict,
    "f77": integer_dict,
    "f78": integer_dict,
    "f79": integer_dict,
    "f80": integer_dict,
    "f81": integer_dict,
    "f82": integer_dict,
    "f83": integer_dict,
    "f84": integer_dict,
    "f85": integer_dict,
}
soc_schema = {
    "work_main_job_title": string_dict,
    "work_main_job_role": string_dict,
    "standard_occupational_classification_code": string_dict,
}
validation_schemas = {
    "test_participant_data_schema": {
        "ons_household_id": string_dict,
        "participant_survey_status": string_dict,
        "withdrawn_reason": string_dict,
        "withdrawn_type": string_dict,
        "participant_id": string_dict,
        "participant_id_numeric": string_dict,
        "title": string_dict,
        "first_name": string_dict,
        "middle_name": string_dict,
        "last_name": string_dict,
        "date_of_birth": string_dict,
        "sex": string_dict,
        "ethnic_group": string_dict,
        "ethnicity": string_dict,
        "ethnicity_other": string_dict,
        "original_invite_cohort": string_dict,
        "consent_contact_extra_research_yn": string_dict,
        "consent_use_of_surplus_blood_samples_yn": string_dict,
        "consent_blood_samples_if_positive_yn": string_dict,
        "existing_participant_digital_opt_in_status": string_dict,
        "existing_participant_digital_opt_in_datetime": string_dict,
        "nhs_data_share": string_dict,
        "nhs_share_opt_out_date": string_dict,
        "household_invited_to_digital": string_dict,
        "household_digital_enrolment_invited_datetime": string_dict,
        "participant_invited_to_digital": string_dict,
        "participant_enrolled_digital": string_dict,
        "participant_digital_enrolment_datetime": string_dict,
        "digital_entry_pack_sent_datetime": string_dict,
        "digital_entry_pack_status": string_dict,
        "existing_participant_digital_opt_in_reminder_1_due_datetime": string_dict,
        "existing_participant_digital_opt_in_reminder_1_sent_datetime": string_dict,
        "existing_participant_digital_opt_in_reminder_1_status": string_dict,
        "existing_participant_digital_opt_in_reminder_2_due_datetime": string_dict,
        "existing_participant_digital_opt_in_reminder_2_sent_datetime": string_dict,
        "existing_participant_digital_opt_in_reminder_2_status": string_dict,
        "household_digital_opt_in_invitation_sent": string_dict,
        "household_digital_opt_in_date": string_dict,
        "household_digital_enrollment_date": string_dict,
        "street": string_dict,
        "city": string_dict,
        "county": string_dict,
        "postcode": string_dict,
        "household_members_under_2_years": string_dict,
        "infant_1": string_dict,
        "infant_2": string_dict,
        "infant_3": string_dict,
        "infant_4": string_dict,
        "infant_5": string_dict,
        "infant_6": string_dict,
        "infant_7": string_dict,
        "infant_8": string_dict,
        "household_members_over_2_and_not_present": string_dict,
        "person_1": string_dict,
        "person_2": string_dict,
        "person_3": string_dict,
        "person_4": string_dict,
        "person_5": string_dict,
        "person_6": string_dict,
        "person_7": string_dict,
        "person_8": string_dict,
        "person_1_not_consenting_age": string_dict,
        "person1_reason_for_not_consenting": string_dict,
        "person_2_not_consenting_age": string_dict,
        "person2_reason_for_not_consenting": string_dict,
        "person_3_not_consenting_age": string_dict,
        "person3_reason_for_not_consenting": string_dict,
        "person_4_not_consenting_age": string_dict,
        "person4_reason_for_not_consenting": string_dict,
        "person_5_not_consenting_age": string_dict,
        "person5_reason_for_not_consenting": string_dict,
        "person_6_not_consenting_age": string_dict,
        "person6_reason_for_not_consenting": string_dict,
        "person_7_not_consenting_age": string_dict,
        "person7_reason_for_not_consenting": string_dict,
        "person_8_not_consenting_age": string_dict,
        "person8_reason_for_not_consenting": string_dict,
        "person_9_not_consenting_age": string_dict,
        "person9_reason_for_not_consenting": string_dict,
        "count_of_non_consenting": string_dict,
        "participant_digital_type_preference": string_dict,
        "participant_digital_communication_preference": string_dict,
        "participant_digital_sample_return_preference": string_dict,
        "participant_digital_language_preference": string_dict,
        "participant_digital_study_cohort": string_dict,
        "participant_digital_voucher_preference": string_dict,
    },
    "test_survey_response_data_version_1_schema": {
        "survey_start_datetime": string_dict,
        "survey_completed_datetime": string_dict,
        "ons_household_id": string_dict,
        "participant_id": string_dict,
        "participant_completion_window_id": string_dict,
        "participant_completion_window_start_date": string_dict,
        "participant_completion_window_end_date": string_dict,
        "work_status_digital": string_dict,
        "work_status_employment": string_dict,
        "work_status_unemployment": string_dict,
        "work_status_education": string_dict,
        "work_in_additional_paid_employment": string_dict,
        "work_main_job_changed": string_dict,
        "work_main_job_title": string_dict,
        "work_main_job_role": string_dict,
        "work_sector": string_dict,
        "work_sector_other": string_dict,
        "work_health_care_area": string_dict,
        "work_direct_contact_patients_or_clients": string_dict,
        "work_nursing_or_residential_care_home": string_dict,
        "work_location": string_dict,
        "work_not_from_home_days_per_week": string_dict,
        "education_in_person_days_per_week": string_dict,
        "transport_to_work_or_education": string_dict,
        "ability_to_socially_distance_at_work_or_education": string_dict,
        "think_have_covid_any_symptom_list_1": array_string_dict,
        "think_have_covid_any_symptom_list_2": array_string_dict,
        "think_have_covid_onset_date": string_dict,
        "swab_taken": string_dict,
        "swab_not_taken_reason": string_dict,
        "swab_sample_barcode_correct": string_dict,
        "swab_sample_barcode_user_entered": string_dict,
        "swab_taken_date": string_dict,
        "swab_taken_time_hour": string_dict,
        "swab_taken_time_minute": string_dict,
        "swab_taken_am_pm": string_dict,
        "swab_returned": string_dict,
        "swab_return_date": string_dict,
        "swab_return_future_date": string_dict,
        "cis_covid_vaccine_received": string_dict,
        "cis_covid_vaccine_type": string_dict,
        "cis_covid_vaccine_type_other": string_dict,
        "cis_covid_vaccine_date": string_dict,
        "been_outside_uk": string_dict,
        "been_outside_uk_last_country": string_dict,
        "been_outside_uk_last_return_date": string_dict,
        "think_had_covid_onset_date": string_dict,
        "think_had_covid_any_symptom_list_1": array_string_dict,
        "think_had_covid_any_symptom_list_2": array_string_dict,
        "face_covering_work_or_education": string_dict,
    },
    "test_survey_response_data_version_2_schema": {
        "Visit_Date_Time": string_dict,
        "ons_household_id": string_dict,
        "Participant_id": string_dict,
        "Visit_ID": string_dict,
        "Visit Status": string_dict,
        "Swab_Barcode_1": string_dict,
        "Date_Time_Samples_Taken": string_dict,
        "What_is_the_title_of_your_main_job": string_dict,
        "What_do_you_do_in_your_main_job_business": string_dict,
        "Occupations_sectors_do_you_work_in": string_dict,
        "occupation_sector_other": string_dict,
        "Work_in_a_nursing_residential_care_home": string_dict,
        "Do_you_currently_work_in_healthcare": string_dict,
        "Direct_contact_patients_clients_resid": string_dict,
        "Have_physical_mental_health_or_illnesses": string_dict,
        "physical_mental_health_or_illness_reduces_activity_ability": string_dict,
        "What_is_your_current_working_status": string_dict,
        "Paid_employment": string_dict,
        "Main_Job_Changed": string_dict,
        "Where_are_you_mainly_working_now": string_dict,
        "How_often_do_you_work_elsewhere": string_dict,
        "How_do_you_get_to_and_from_work_school": string_dict,
        "Can_you_socially_distance_at_work": string_dict,
        "Had_symptoms_in_the_last_7_days": string_dict,
        "Which_symptoms_in_the_last_7_days": string_dict,
        "Date_of_first_symptom_onset": string_dict,
        "Symptoms_7_Fever": string_dict,
        "Symptoms_7_Muscle_ache_myalgia": string_dict,
        "Symptoms_7_Fatigue_weakness": string_dict,
        "Symptoms_7_Sore_throat": string_dict,
        "Symptoms_7_Cough": string_dict,
        "Symptoms_7_Shortness_of_breath": string_dict,
        "Symptoms_7_Headache": string_dict,
        "Symptoms_7_Nausea_vomiting": string_dict,
        "Symptoms_7_Abdominal_pain": string_dict,
        "Symptoms_7_Diarrhoea": string_dict,
        "Symptoms_7_Loss_of_taste": string_dict,
        "Symptoms_7_Loss_of_smell": string_dict,
        "Symptoms_7_More_trouble_sleeping_than_usual": string_dict,
        "Symptoms_7_Runny_nose_sneezing": string_dict,
        "Symptoms_7_Noisy_breathing_wheezing": string_dict,
        "Symptoms_7_Loss_of_appetite_or_eating_less_than_usual": string_dict,
        "Symptoms_7_Chest_pain": string_dict,
        "Symptoms_7_Palpitations": string_dict,
        "Symptoms_7_Vertigo_dizziness": string_dict,
        "Symptoms_7_Worry_anxiety": string_dict,
        "Symptoms_7_Low_mood_not_enjoying_anything": string_dict,
        "Symptoms_7_Memory_loss_or_confusion": string_dict,
        "Symptoms_7_Difficulty_concentrating": string_dict,
        "Do_you_think_you_have_Covid_Symptoms": string_dict,
        "Face_Covering_or_Mask_outside_of_home": string_dict,
        "Face_Mask_Work_Place": string_dict,
        "Face_Mask_Other_Enclosed_Places": string_dict,
        "Do_you_think_you_have_had_Covid_19": string_dict,
        "think_had_covid_19_any_symptoms": string_dict,
        "think_had_covid_19_which_symptoms": string_dict,
        "Previous_Symptoms_Fever": string_dict,
        "Previous_Symptoms_Muscle_ache_myalgia": string_dict,
        "Previous_Symptoms_Fatigue_weakness": string_dict,
        "Previous_Symptoms_Sore_throat": string_dict,
        "Previous_Symptoms_Cough": string_dict,
        "Previous_Symptoms_Shortness_of_breath": string_dict,
        "Previous_Symptoms_Headache": string_dict,
        "Previous_Symptoms_Nausea_vomiting": string_dict,
        "Previous_Symptoms_Abdominal_pain": string_dict,
        "Previous_Symptoms_Diarrhoea": string_dict,
        "Previous_Symptoms_Loss_of_taste": string_dict,
        "Previous_Symptoms_Loss_of_smell": string_dict,
        "Previous_Symptoms_More_trouble_sleeping_than_usual": string_dict,
        "Previous_Symptoms_Runny_nose_sneezing": string_dict,
        "Previous_Symptoms_Noisy_breathing_wheezing": string_dict,
        "Previous_Symptoms_Loss_of_appetite_or_eating_less_than_usual": string_dict,
        "Previous_Symptoms_Chest_pain": string_dict,
        "Previous_Symptoms_Palpitations": string_dict,
        "Previous_Symptoms_Vertigo_dizziness": string_dict,
        "Previous_Symptoms_Worry_anxiety": string_dict,
        "Previous_Symptoms_Low_mood_not_enjoying_anything": string_dict,
        "Previous_Symptoms_Memory_loss_or_confusion": string_dict,
        "Previous_Symptoms_Difficulty_concentrating": string_dict,
        "If_yes_Date_of_first_symptoms": string_dict,
        "Have_you_had_a_swab_test": string_dict,
        "If_Yes_What_was_result": string_dict,
        "If_positive_Date_of_1st_ve_test": string_dict,
        "If_all_negative_Date_last_test": string_dict,
        "Have_you_been_offered_a_vaccination": string_dict,
        "Vaccinated_Against_Covid": string_dict,
        "Type_Of_Vaccination": string_dict,
        "Vaccination_Other": string_dict,
        "Number_Of_Doses": string_dict,
        "Date_Of_Vaccination": string_dict,
        "Type_Of_Vaccination_1": string_dict,
        "Vaccination_Other_1": string_dict,
        "Date_Of_Vaccination_1": string_dict,
        "Type_Of_Vaccination_2": string_dict,
        "Vaccination_Other_2": string_dict,
        "Date_Of_Vaccination_2": string_dict,
        "Type_Of_Vaccination_3": string_dict,
        "Vaccination_Other_3": string_dict,
        "Date_Of_Vaccination_3": string_dict,
        "Type_Of_Vaccination_4": string_dict,
        "Vaccination_Other_4": string_dict,
        "Date_Of_Vaccination_4": string_dict,
        "Have_you_been_outside_UK_since_April": string_dict,
        "been_outside_uk_last_country": string_dict,
        "been_outside_uk_last_date": string_dict,
        "Have_you_been_outside_UK_Lastspoke": string_dict,
    },
    "test_swab_sample_results_schema": {
        "Sample": {"type": "string", "regex": r"ONS\d{8}"},
        "Result": {"type": "string", "allowed": ["Negative", "Positive", "Void"]},
        "Date Tested": {"type": "string", "nullable": True},
        "Lab ID": string_dict,
        "testKit": string_dict,
        "CH1-Target": {"type": "string", "allowed": ["ORF1ab"]},
        "CH1-Result": {"type": "string", "allowed": swab_allowed_pcr_results},
        "CH1-Cq": {"type": "double", "nullable": True, "min": 0},
        "CH2-Target": {"type": "string", "allowed": ["N gene"]},
        "CH2-Result": {"type": "string", "allowed": swab_allowed_pcr_results},
        "CH2-Cq": {"type": "double", "nullable": True, "min": 0},
        "CH3-Target": {"type": "string", "allowed": ["S gene"]},
        "CH3-Result": {"type": "string", "allowed": swab_allowed_pcr_results},
        "CH3-Cq": {"type": "double", "nullable": True, "min": 0},
        "CH4-Target": {"type": "string", "allowed": ["MS2"]},
        "CH4-Result": {"type": "string", "allowed": swab_allowed_pcr_results},
        "CH4-Cq": {"type": "double", "nullable": True, "min": 0},
        "voidReason": string_dict,
    },
    "csv_lookup_schema": {
        "id": string_dict,
        "dataset_name": string_dict,
        "target_column_name": string_dict,
        "old_value": string_dict,
        "new_value": string_dict,
    },
    "csv_lookup_schema_extended": {
        "id_column_name": string_dict,
        "id": string_dict,
        "dataset_name": string_dict,
        "target_column_name": string_dict,
        "old_value": string_dict,
        "new_value": string_dict,
    },
    "vaccine_capture_schema": {  # Referenced in primary or secondary config
        "participant_id": string_dict,
        "ons_household_id": string_dict,
        "covid_vaccine_date_1": string_dict,
        "covid_vaccine_type_1": string_dict,
        "covid_vaccine_type_other_1": string_dict,
        "covid_vaccine_date_2": string_dict,
        "covid_vaccine_type_2": string_dict,
        "covid_vaccine_type_other_2": string_dict,
    },
    "cohort_schema": {  # Referenced in primary or secondary config
        "participant_id": string_dict,
        "old_cohort": string_dict,
        "new_cohort": string_dict,
    },
    "soc_resolution_schema": {  # Referenced in primary or secondary config
        "job_title": string_dict,
        "main_job_responsibilities": string_dict,
        "Gold SOC2010 code": string_dict,
    },
    "blood_past_positive_schema": {  # Referenced in primary or secondary config
        "ons_household_id": string_dict,
        "blood_past_positive": integer_dict,
    },
    "travel_schema": {  # Referenced in primary or secondary config
        "been_outside_uk_last_country_old": string_dict,
        "been_outside_uk_last_country_new": string_dict,
    },
    "tenure_schema": {  # Referenced in primary or secondary config
        "UAC": string_dict,
        "GOR9D": string_dict,
        "ten1": string_dict,
        "tenure_group": integer_dict,
        "DVHSize": integer_dict,
        "NumChild": integer_dict,
        "NumAdult": integer_dict,
        "sample": string_dict,
        "dweight_hh": string_dict,
        "ons_household_id": string_dict,
        "UAC_new": string_dict,
        "UA19NM": string_dict,
        "LHB19CD": string_dict,
        "health_board": string_dict,
    },
    "imputation_lookup_schema": {  # Referenced in primary or secondary config
        "participant_id": string_dict,
        "ethnicity_white": string_dict,
        "ethnicity_white_imputation_method": string_dict,
        "sex": string_dict,
        "sex_imputation_method": string_dict,
        "date_of_birth": string_dict,
        "date_of_birth_imputation_method": string_dict,
    },
    "address_schema": {  # Referenced in primary or secondary config
        "UPRN": string_dict,
        "ORGANISATION_NAME": string_dict,
        "ADDRESS_LINE1": string_dict,
        "ADDRESS_LINE2": string_dict,
        "ADDRESS_LINE3": string_dict,
        "TOWN_NAME": string_dict,
        "POSTCODE": string_dict,
        "ABP_CODE": string_dict,
        "ADDRESS_TYPE": string_dict,
        "COUNCIL_TAX": string_dict,
        "EXTRACT": string_dict,
        "CTRY18NM": string_dict,
        "CTRY18CD": string_dict,
        "EW": string_dict,
        "EPOCH": string_dict,
        "LA_CODE": string_dict,
        "UDPRN": string_dict,
        "LOGICAL_STATUS": string_dict,
        "ADDRESSBASE_POSTAL": string_dict,
    },
    "postcode_schema": {  # Referenced in primary or secondary config
        "pcd": string_dict,
        "pcd2": string_dict,
        "pcds": string_dict,
        "dointr": string_dict,
        "doterm": string_dict,
        "usertype": string_dict,
        "oseast1m": string_dict,
        "osnrth1m": string_dict,
        "osgrdind": string_dict,
        "oa11": string_dict,
        "cty": string_dict,
        "ced": string_dict,
        "laua": string_dict,
        "ward": string_dict,
        "hlthau": string_dict,
        "nhser": string_dict,
        "ctry": string_dict,
        "rgn": string_dict,
        "pcon": string_dict,
        "eer": string_dict,
        "teclec": string_dict,
        "ttwa": string_dict,
        "pct": string_dict,
        "itl": string_dict,
        "park": string_dict,
        "lsoa11": string_dict,
        "msoa11": string_dict,
        "wz11": string_dict,
        "ccg": string_dict,
        "bua11": string_dict,
        "buasd11": string_dict,
        "ru11ind": string_dict,
        "oac11": string_dict,
        "lat": string_dict,
        "long": string_dict,
        "lep1": string_dict,
        "lep2": string_dict,
        "pfa": string_dict,
        "imd": string_dict,
        "calncv": string_dict,
        "stp": string_dict,
    },
    "lsoa_cis_schema": {  # Referenced in primary or secondary config
        "LSOA11CD": string_dict,
        "LSOA11NM": string_dict,
        "CIS20CD": string_dict,
        "RGN19CD": string_dict,
    },
    "rural_urban_schema": {  # Referenced in primary or secondary config
        "lower_super_output_area_code_11": string_dict,
        "cis_rural_urban_classification": string_dict,
    },
    "cis_phase_schema": {  # Referenced in primary or secondary config
        "phase_name": string_dict,
        "phase_visits": string_dict,
        "phase_sample": string_dict,
        "country": string_dict,
        "source": string_dict,
        "issued_wc": string_dict,
        "fieldwork_wc": string_dict,
    },
    "country_schema": {  # Referenced in primary or secondary config
        "LAD20CD": string_dict,
        "LAD20NM": string_dict,
        "CTRY20CD": string_dict,
        "CTRY20NM": string_dict,
    },
    "old_sample_file_schema": {  # Referenced in primary or secondary config
        "sample": string_dict,
        "UAC": string_dict,
        "GOR9D": string_dict,
        "DVHSize": string_dict,
        "UA19NM": string_dict,
        "LHB19CD": string_dict,
        "LHB19NM": string_dict,
        "country_sample": string_dict,
        "laua": string_dict,
        "rgn": string_dict,
        "lsoa11": string_dict,
        "msoa11": string_dict,
        "imd": string_dict,
        "sample_direct": string_dict,
        "tranche": string_dict,
        "interim_id": string_dict,
        "dweight_HH": string_dict,
        "dweight_hh_atb": string_dict,
    },
    "new_sample_file_schema": {
        "UAC": string_dict,
        "sample": string_dict,
        "LA_CODE": string_dict,
        "Bloods": string_dict,
        "oa11": string_dict,
        "laua": string_dict,
        "ctry": string_dict,
        "CUSTODIAN_REGION_CODE": string_dict,
        "lsoa11": string_dict,
        "msoa11": string_dict,
        "ru11ind": string_dict,
        "oac11": string_dict,
        "rgn": string_dict,
        "imd": string_dict,
        "interim_id": string_dict,
    },
    "master_sample_file_schema": {  # Referenced in primary or secondary config
        "ons_household_id": string_dict,
        "FULL_NAME": string_dict,
        "ADDRESS_LINE_1": string_dict,
        "ADDRESS_LINE_2": string_dict,
        "ADDRESS_LINE_3": string_dict,
        "POSTCODE": string_dict,
        "REGION": string_dict,
        "COUNTRY": string_dict,
        "BLOODS": string_dict,
        "WEEK": string_dict,
        "SAMPLE_TYPE": string_dict,
        "UPRN": string_dict,
        "UDPRN": string_dict,
        "TOWN_NAME": string_dict,
    },
    "tranche_schema": {
        "cis20_samp": string_dict,
        "cis20_name": string_dict,
        "preference": string_dict,
        "ons_household_id": string_dict,
        "hh_enrol_date": string_dict,
        "laua_sample": string_dict,
        "laua_name": string_dict,
        "gor_name": string_dict,
        "country": string_dict,
        "is_nw_hh": string_dict,
        "random": string_dict,
        "number_of_adults": string_dict,
        "tranche": integer_dict,
    },
    "aps_schema": {  # Referenced in primary or secondary config
        "caseno": integer_dict,
        "age": integer_dict,
        "country": integer_dict,
        "ethgbeul": integer_dict,
        "eth11ni": string_dict,  # double check
        "pwta18": integer_dict,
    },
    "sample_direct_eng_wc_schema": {  # Referenced in primary or secondary config
        "unique_access_code": {"type": "string", "regex": r"^\d{12}"},
        "local_authority_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
        "in_blood_cohort": {"type": "integer", "min": 0, "max": 1},
        "output_area_code": {"type": "string", "regex": r"^[E,W,S]00\d{6}"},
        "local_authority_unity_authority_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
        "country_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
        "custodian_region_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
        "lower_super_output_area_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
        "middle_super_output_area_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
        "rural_urban_classification": {"type": "string", "regex": r"^([a-zA-Z]\d{1}|\d{1})"},
        "census_output_area_classification": {"type": "string", "regex": r"^\d{1}[a-zA-Z]\d{1}"},
        "region_code": {"type": "string", "regex": r"^[E,W,S]\d{8}"},
        "index_multiple_deprivation": {"type": "integer", "min": 0, "max": 32844},
        "cis_area_indicator": {"type": "integer", "min": 1, "max": 128},
    },
    "sample_northern_ireland_schema": {  # Referenced in primary or secondary config
        "unique_access_code": {"type": "string", "regex": r"^\d{12}"},
        "sample_week_indicator": {"type": "string", "regex": r"^\d{1}[a-zA-Z]{3}"},
        "output_area_code": {"type": "string", "regex": r"^N00\d{6}"},
        "local_authority_unity_authority_code": {"type": "string", "regex": r"^N\d{8}"},
        "country_code": {"type": "string", "regex": r"^N\d{8}"},
        "custodian_region_code": {"type": "string", "regex": r"^N\d{8}"},
        "lower_super_output_area_code": {"type": "string", "regex": r"^N\d{8}"},
        "middle_super_output_area_code": {"type": "string", "regex": r"^N\d{8}"},
        "census_output_area_classification": {"type": "string", "regex": r"^\d{1}[a-zA-Z]\d{1}"},
        "lower_super_output_area_name": {"type": "string", "regex": r"[a-zA-Z]{2,}}"},
        "cis_area_code": {"type": "string", "regex": r"^J\d{8}"},
        "region_code": {"type": "string", "regex": r"^N\d{8}"},
        "index_multiple_deprivation": {"type": "integer", "min": 1, "max": 890},
        "cis_area_indicator": {"type": "integer", "min": 999, "max": 999},
    },
}
