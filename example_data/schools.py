"""
Generate fake data for schools infection survey raw input data.
"""
from mimesis.schema import Field, Schema
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

_ = Field('en-gb', seed=42)


def generate_survey_schools(directory, file_date, records):
    """
    Generate survey schools file.
    """
    survey_schools_description = (
        lambda: {
            'schl_nm': " ".join(_('text.words', quantity=4)).title(),
            'schl_post_cde': _('address.postal_code'),
            'schl_urn': _('random.custom_code', mask='######', digit='#'),
            'studyconsent': _('numbers.integer_number', start=0, end=1),
            'schl_child_cnt': _('numbers.integer_number', start=50, end=100),
            # Don't think we use individual year counts in the pipeline
            'head_teacher_nm': _('full_name'),
            'school_telephone_number': _('person.telephone'),
            'school_contact_email': _('person.email', domains=['gsnail.ac.uk']),
            'information_consent': _('numbers.integer_number', start=0, end=1),
            'schl_la_nm': _('address.state'),
            'change_indicator': _('numbers.integer_number', start=0, end=1),
            'last_change_datetime': _('datetime.formatted_datetime', fmt="%d/%m/%Y %H:%M:%S", start=1800, end=1802),
            'record_created_date': _('datetime.formatted_datetime', fmt="%d/%m/%Y %H:%M:%S", start=1800, end=1802),
        }
    )

    schema = Schema(schema=survey_schools_description)
    survey_schools = pd.DataFrame(schema.create(iterations=records))
    survey_schools.to_csv(directory / f"survey_schools_{file_date}.csv", index=False)
    return survey_schools


def generate_survey_participants(directory, file_date, records, school_urns):
    """
    Generate survey participants file. Depends on survey schools file.
    """
    survey_participants_description = (
        lambda: {
            'participant_type': _('choice', items=['type_1', 'type_2']),
        # Assume we don't need real types, but do need enrolment questions per type
            'participant_id': _('random.custom_code', mask='P#########', digit='#'),
            'parent_participant_id': _('random.custom_code', mask='P#########', digit='#'),
            'participant_first_nm': _('person.first_name'),
            'participant_family_name': _('person.last_name'),
            'email_addrs': _('person.email', domains=['gsnail.ac.uk']),
            'schl_urn': _('choice', items=list(school_urns)),
            'consent': _('numbers.integer_number', start=0, end=1),
            'change_date': _('datetime.formatted_datetime', fmt="%d/%m/%Y %H:%M:%S", start=1800, end=1802),
            'record_created_date': _('datetime.formatted_datetime', fmt="%d/%m/%Y %H:%M:%S", start=1800, end=1802),
        }
    )

    schema = Schema(schema=survey_participants_description)
    survey_participants = pd.DataFrame(schema.create(iterations=records))

    # Type 2 doesn't have registered parents
    survey_participants.loc[survey_participants["participant_type"] == "type_2", "parent_participant_id"] = pd.NA

    survey_participants.to_csv(directory / f"survey_participants_{file_date}.csv", index=False)
    return survey_participants


def generate_survey_responses(directory, file_date, records, participant_ids, school_ids):
    """
    Generate survey responses file. Depends on survey participants and schools files.
    """
    survey_responses_description = (
        lambda: {
            'participant_id': _('choice', items=list(participant_ids)),
            'question_id': _('random.custom_code', mask='Q#####', digit='#'),
            'question_response_text': _('text.sentence'),
            'last_change_datetime': _('datetime.formatted_datetime', fmt="%d/%m/%Y %H:%M:%S", start=1800, end=1802),
            'record_created_datetime': _('datetime.formatted_datetime', fmt="%d/%m/%Y %H:%M:%S", start=1800, end=1802)
        }
    )

    schema = Schema(schema=survey_responses_description)
    survey_responses = pd.DataFrame(schema.create(iterations=records))
    survey_responses.to_csv(directory / f"survey_responses_{file_date}.csv", index=False)
    return survey_responses


def generate_lab_swabs(directory, file_date, records):
    """
    Generate lab swabs file.
    """
    lab_swabs_description = (
        lambda: {
            'Sample':_('random.custom_code', mask='SIS########', digit='#'),
            'Result': _('choice', items=["Positive", "Negative"]),
            'Date Tested': _('datetime.formatted_datetime', fmt="%Y-%m-%d %H:%M:%S UTC", start=1800, end=1802),
            'Seq-Target': "A gene",
            'Seq-Result': _('choice', items=["Positive", "Negative"])
        }
    )

    schema = Schema(schema=lab_swabs_description)
    lab_swabs = pd.DataFrame(schema.create(iterations=records))
    lab_swabs.to_csv(directory / f"lab_swabs_{file_date}.csv", index=False)
    return lab_swabs


def generate_lab_bloods(directory, file_date, records):
    """
    Generate lab bloods file.
    """
    lab_bloods_description = (
        lambda: {
            'specimenId': _('random.custom_code', mask='#########THR', digit='#'),
            'specimenProcessedDate': _('datetime.formatted_datetime', fmt="%Y-%m-%dT%H:%M:%SZ", start=1800, end=1802),
            'testResult': _('choice', items=['Positive', 'Negative'])
        }
    )

    schema = Schema(schema=lab_bloods_description)
    lab_bloods = pd.DataFrame(schema.create(iterations=records))
    lab_bloods.to_csv(directory / f"lab_bloods_{file_date}.csv", index=False)
    return lab_bloods


def generate_lab_saliva(directory, file_date, records):
    """
    Generate lab saliva file.
    """
    lab_saliva_description = (
        lambda: {
            'ORDPATNAME': _('random.custom_code', mask='SIS########', digit='#'),
            'SAMPLEID': _('random.custom_code', mask='H#########', digit='#'),
            'IgG Capture Result': _('choice', items=['#r', '#n', '#e'])
        }
    )

    schema = Schema(schema=lab_saliva_description)
    lab_saliva = pd.DataFrame(schema.create(iterations=records))
    lab_saliva.to_csv(directory / f"lab_saliva_{file_date}.csv", index=False)
    return lab_saliva


def generate_survey_visits(directory, file_date, records, participant_ids, swab_barcodes, blood_barcodes, saliva_barcodes):
    """
    Generate survey visits file. Depends on survey participants and schools files.
    """
    survey_visits_description = (
        lambda: {
            'participant_id': _('choice', items=list(participant_ids)),
            'visit_date': _('datetime.formatted_datetime', fmt="%Y-%m-%d %H:%M:%S UTC", start=1800, end=1802),
            'swab_Sample_barcode': _('choice', items=list(swab_barcodes) + [pd.NA]),
            'blood_thriva_barcode': _('choice', items=list(blood_barcodes) + [pd.NA]),
            'oral_swab_barcode': _('choice', items=list(saliva_barcodes)+ [pd.NA]),
            'last_change_datetime': _('datetime.formatted_datetime', fmt="%d/%m/%Y %H:%M:%S", start=1800, end=1802),
            'record_created_datetime': _('datetime.formatted_datetime', fmt="%d/%m/%Y %H:%M:%S", start=1800, end=1802)
        }
    )

    schema = Schema(schema=survey_visits_description)
    survey_visits = pd.DataFrame(schema.create(iterations=records))
    survey_visits.to_csv(directory / f"survey_visits_{file_date}.csv", index=False)
    return survey_visits


def generate_question_lookup(directory, file_date, records, question_ids):
    """
    Generate question id to name lookup. Depends on survey responses file.
    """
    question_lookup_description = (
        lambda: {
            'question_id': _('choice', items=question_ids),
            'new_variables_names': "_".join(_('text.words', quantity=4)).lower()
        }
    )

    schema = Schema(schema=question_lookup_description)
    question_lookup = pd.DataFrame(schema.create(iterations=records))
    question_lookup.to_csv(directory / f"question_lookup_{file_date}.csv", index=False)
    return question_lookup


if __name__ == "__main__":
    raw_dir = Path("raw_schools")
    swab_dir = raw_dir / "swab"
    blood_dir = raw_dir / "blood"
    saliva_dir = raw_dir / "saliva"
    survey_dir = raw_dir / "survey"
    lookup_dir = raw_dir / "lookup"
    for directory in [swab_dir, blood_dir, saliva_dir, survey_dir, lookup_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    file_date = datetime.strptime("18010101", "%Y%m%d")
    lab_date_1 = datetime.strftime(file_date - timedelta(days=1), format="%Y%m%d")
    lab_date_2 = datetime.strftime(file_date - timedelta(days=2), format="%Y%m%d")
    file_date = datetime.strftime(file_date, format="%Y%m%d")

    schools = generate_survey_schools(survey_dir, file_date, 10)

    participants = generate_survey_participants(
        survey_dir,
        file_date,
        40,
        schools["schl_urn"].unique().tolist()
    )

    responses = generate_survey_responses(
        survey_dir,
        file_date,
        100,
        participants["participant_id"].unique().tolist(),
        participants["schl_urn"].unique().tolist()
    )

    lab_swabs_1 = generate_lab_swabs(swab_dir, file_date, 10)
    lab_swabs_2 = generate_lab_swabs(swab_dir, lab_date_1, 10)
    lab_swabs_3 = generate_lab_swabs(swab_dir, lab_date_2, 10)
    lab_swabs = pd.concat([lab_swabs_1, lab_swabs_2, lab_swabs_3])

    lab_bloods_1 = generate_lab_bloods(blood_dir, file_date, 10)
    lab_bloods_2 = generate_lab_bloods(blood_dir, lab_date_1, 10)
    lab_bloods_3 = generate_lab_bloods(blood_dir, lab_date_2, 10)
    lab_bloods = pd.concat([lab_bloods_1, lab_bloods_2, lab_bloods_3])

    lab_saliva_1 = generate_lab_saliva(saliva_dir, file_date, 10)
    lab_saliva_2 = generate_lab_saliva(saliva_dir, lab_date_1, 10)
    lab_saliva_3 = generate_lab_saliva(saliva_dir, lab_date_2, 10)
    lab_saliva = pd.concat([lab_saliva_1, lab_saliva_2, lab_saliva_3])

    visits = generate_survey_visits(
        survey_dir,
        file_date,
        100,
        participants["participant_id"].unique().tolist(),
        lab_swabs["Sample"].unique().tolist(),
        lab_bloods["specimenId"].unique().tolist(),
        lab_saliva["ORDPATNAME"].unique().tolist(),
        )

    question_ids = responses["question_id"].unique().tolist()
    generate_question_lookup(
        lookup_dir,
        file_date,
        len(question_ids),
        question_ids
        )
