"""
Generate fake data for schools infection survey raw input data.
"""
from mimesis.schema import Field, Schema
import pandas as pd

_ = Field('en-gb', seed=42)


def generate_survey_schools(file_date, records):
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
    survey_schools.to_csv(f"survey_schools_{file_date}.csv", index=False)
    return survey_schools


def generate_survey_participants(file_date, records, school_urns):
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

    survey_participants.to_csv(f"survey_participants_{file_date}.csv", index=False)
    return survey_participants


def generate_survey_responses(file_date, records, participant_ids, school_ids):
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
    survey_responses.to_csv(f"survey_responses_{file_date}.csv", index=False)
    return survey_responses


def generate_lab_swabs(file_date, records):
    """
    Generate lab swabs file. Depends on survey participants file.
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
    lab_swabs.to_csv(f"lab_swabs_{file_date}.csv", index=False)
    return lab_swabs


def generate_lab_bloods(file_date, records):
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
    lab_bloods.to_csv(f"lab_bloods_{file_date}.csv", index=False)
    return lab_bloods


def generate_lab_saliva(file_date, records):
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
    lab_saliva.to_csv(f"lab_saliva_{file_date}.csv", index=False)
    return lab_saliva

  
def generate_survey_visits(file_date, records, participant_ids, swab_barcodes, blood_barcods, saliva_barcodes):
    """
    Generate survey visits file. Depends on survey participants and schools files.
    """
    survey_visits_description = (
        lambda: {
            'participant_id': _('choice', items=list(participant_ids)),
            'visit_date': _('datetime.formatted_datetime', fmt="%Y-%m-%d %H:%M:%S UTC", start=1800, end=1802),
            'swab_Sample_barcode': _('choice', items=list(swab_barcodes) + [""]),
            'blood_thriva_barcode': _('choice', items=list(blood_barcods) + [""]),
            'oral_swab_barcode': _('choice', items=list(saliva_barcodes)+ [""]),
            'last_change_datetime': _('datetime.formatted_datetime', fmt="%d/%m/%Y %H:%M:%S", start=1800, end=1802),
            'record_created_datetime': _('datetime.formatted_datetime', fmt="%d/%m/%Y %H:%M:%S", start=1800, end=1802)
        }
    )

    schema = Schema(schema=survey_visits_description)
    survey_visits = pd.DataFrame(schema.create(iterations=records))
    survey_visits.to_csv(f"survey_visits_{file_date}.csv", index=False)
    return survey_visits


if __name__ == "__main__":
    file_date = "18010101"

    schools = generate_survey_schools(file_date, 10)

    participants = generate_survey_participants(
        file_date,
        40,
        schools["schl_urn"].unique().tolist()
    )

    responses = generate_survey_responses(
        file_date,
        100,
        participants["participant_id"].unique().tolist(),
        participants["schl_urn"].unique().tolist()
    )

    lab_swabs = generate_lab_swabs(file_date, 10)
    lab_bloods = generate_lab_bloods(file_date, 10)
    lab_saliva = generate_lab_saliva(file_date, 10)
  

    visits = generate_survey_visits(
        file_date,
        100,
        participants["participant_id"].unique().tolist(),
        lab_swabs["Sample"].unique().tolist(),
        lab_bloods["specimenId"].unique().tolist(),
        lab_saliva["ORDPATNAME"].unique().tolist(),
        )
