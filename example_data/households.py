"""
Generate fake data for households survey raw input data.
"""
from mimesis.schema import Field, Schema
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

_ = Field('en-gb', seed=42)


def generate_lab_bloods(directory, file_date, records):
    """
    Generate lab bloods file.
    """
    lab_bloods_description = lambda: {
        'ons_id': _('random.custom_code', mask='ONS########', digit='#'),
        'run_date_tdi': _('datetime.formatted_datetime', fmt="%d/%m/%Y", start=1800, end=1802),
        'interpretation_tdi': _('choice', items=['Positive', 'Negative'])
    }

    schema = Schema(schema=lab_bloods_description)
    lab_bloods = pd.DataFrame(schema.create(iterations=records))
    lab_bloods.to_excel(directory / f"lab_bloods_{file_date}.xlsx", index=False)
    return lab_bloods


def generate_lab_swabs(directory, file_date, records):
    """
    Generate lab swabs file.
    """
    lab_swabs_description = (
        lambda: {
            'Sample':_('random.custom_code', mask='ONN########', digit='#'),
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


def generate_survey_v0_data(directory, file_date, records, swab_barcodes, blood_barcodes):
    """
    Generate survey v0 data. Depends on lab swabs and lab bloods.
    """
    v0_data_description = (
        lambda: {
            'ONS Household ID': _('random.custom_code', mask='############', digit='#'),
            'Visit ID': _('random.custom_code', mask='DVS-##########', digit='#'),
            'Type of Visit': _('choice', items=['Follow-up', 'First']),
            'Visit Date/Time': _('datetime.formatted_datetime', fmt="%Y-%m-%d %H:%M:%S UTC", start=1800, end=1802),
            'Street': _('address.street_name'),
            'City': _('address.city'),
            'County': _('address.state'),
            'Postcode': _('address.postal_code'),
            'Full_name': _('person.full_name'),
            'Email': _('person.email', domains=['gsnail.ac.uk']),
            'Swab Barcode 1': _('choice', items=swab_barcodes),
            'Bloods Barcode 1': _('choice', items=blood_barcodes)
        }
    )

    schema = Schema(schema=v0_data_description)
    survey_v0 = pd.DataFrame(schema.create(iterations=records))

    survey_v0.to_csv(directory / f"survey_v0_{file_date}.csv", index=False)
    return survey_v0


def generate_survey_v1_data(directory, file_date, records, swab_barcodes, blood_barcodes):
    """
    Generate survey v1 data. Depends on lab swabs and lab bloods.
    """
    v1_data_description = (
        lambda: {
            'ONS Household ID': _('random.custom_code', mask='############', digit='#'),
            'Visit ID': _('random.custom_code', mask='DVS-##########', digit='#'),
            'Type of Visit': _('choice', items=['Follow-up', 'First']),
            'Visit Date/Time': _('datetime.formatted_datetime', fmt="%Y-%m-%d %H:%M:%S UTC", start=1800, end=1802),
            'Street': _('address.street_name'),
            'City': _('address.city'),
            'County': _('address.state'),
            'Postcode': _('address.postal_code'),
            'Title': _('person.title'),
            'First_name': _('person.first_name'),
            'Last_name': _('person.last_name'),
            'Email': _('person.email', domains=['gsnail.ac.uk']),
            'Swab_Barcode_1': _('choice', items=swab_barcodes),
            'bloods_barcode_1': _('choice', items=blood_barcodes)
        }
    )

    schema = Schema(schema=v1_data_description)
    survey_v1 = pd.DataFrame(schema.create(iterations=records))

    survey_v1.to_csv(directory / f"survey_v1_{file_date}.csv", index=False)
    return survey_v1


def generate_survey_v2_data(directory, file_date, records, swab_barcodes, blood_barcodes):
    """
    Generate survey v2 data. Depends on lab swabs and lab bloods.
    """
    v2_data_description = (
        lambda: {
            'ONS Household ID': _('random.custom_code', mask='############', digit='#'),
            'Visit ID': _('random.custom_code', mask='DVS-##########', digit='#'),
            'Type of Visit': _('choice', items=['Follow-up', 'First']),
            'Visit Date/Time': _('datetime.formatted_datetime', fmt="%Y-%m-%d %H:%M:%S UTC", start=1800, end=1802),
            'Street': _('address.street_name'),
            'City': _('address.city'),
            'County': _('address.state'),
            'Postcode': _('address.postal_code'),
            'Title': _('person.title'),
            'First_name': _('person.first_name'),
            'Middle_name': _('person.name'),
            'Last_name': _('person.last_name'),
            'Email': _('person.email', domains=['gsnail.ac.uk']),
            'Swab_Barcode_1': _('choice', items=swab_barcodes),
            'bloods_barcode_1': _('choice', items=blood_barcodes)
        }
    )

    schema = Schema(schema=v2_data_description)
    survey_v2 = pd.DataFrame(schema.create(iterations=records))

    survey_v2.to_csv(directory / f"survey_v2_{file_date}.csv", index=False)
    return  survey_v2


if __name__ == "__main__":
    raw_dir = Path("raw_households")
    swab_dir = raw_dir / "swab"
    blood_dir = raw_dir / "blood"
    survey_dir = raw_dir / "survey"
    for directory in [swab_dir, blood_dir, survey_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    file_date = datetime.strptime("18010101", "%Y%m%d")
    lab_date_1 = datetime.strftime(file_date - timedelta(days=1), format="%Y%m%d")
    lab_date_2 = datetime.strftime(file_date - timedelta(days=2), format="%Y%m%d")
    file_date = datetime.strftime(file_date, format="%Y%m%d")

    lab_swabs_1 = generate_lab_swabs(swab_dir, file_date, 10)
    lab_swabs_2 = generate_lab_swabs(swab_dir, lab_date_1, 10)
    lab_swabs_3 = generate_lab_swabs(swab_dir, lab_date_2, 10)
    lab_swabs = pd.concat([lab_swabs_1, lab_swabs_2, lab_swabs_3])

    lab_bloods_1 = generate_lab_bloods(blood_dir, file_date, 10)
    lab_bloods_2 = generate_lab_bloods(blood_dir, lab_date_1, 10)
    lab_bloods_3 = generate_lab_bloods(blood_dir, lab_date_2, 10)
    lab_bloods = pd.concat([lab_bloods_1, lab_bloods_2, lab_bloods_3])

    v0 = generate_survey_v0_data(
        survey_dir,
        file_date,
        50,
        lab_swabs["Sample"].unique().tolist(),
        lab_bloods["ons_id"].unique().tolist()
        )

    v1 = generate_survey_v1_data(
        survey_dir,
        file_date,
        50,
        lab_swabs["Sample"].unique().tolist(),
        lab_bloods["ons_id"].unique().tolist()
        )

    v2 = generate_survey_v2_data(
        survey_dir,
        file_date,
        50,
        lab_swabs["Sample"].unique().tolist(),
        lab_bloods["ons_id"].unique().tolist()
        )
