"""
Generate fake data for households survey raw input data.
"""
from mimesis.schema import Field, Schema
import pandas as pd

_ = Field('en-gb', seed=42)


def generate_survey_v0_data(file_date, records):
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

        }
    )

    schema = Schema(schema=v0_data_description)
    survey_v0 = pd.DataFrame(schema.create(iterations=records))

    survey_v0.to_csv(f"survey_v0_{file_date}.csv", index=False)
    return survey_v0


def generate_survey_v1_data(file_date, records):
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

        }
    )

    schema = Schema(schema=v1_data_description)
    survey_v1 = pd.DataFrame(schema.create(iterations=records))

    survey_v1.to_csv(f"survey_v1_{file_date}.csv", index=False)
    return survey_v1


def generate_survey_v2_data(file_date, records):
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

        }
    )

    schema = Schema(schema=v2_data_description)
    survey_v2 = pd.DataFrame(schema.create(iterations=records))

    survey_v2.to_csv(f"survey_v2_{file_date}.csv", index=False)
    return  survey_v2

if __name__ == "__main__":
    file_date = "18010101"

    v0 = generate_survey_v0_data(file_date, 10)

    v1 = generate_survey_v1_data(file_date, 10)

    v2 = generate_survey_v2_data(file_date, 10)