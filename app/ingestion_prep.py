# %%
import pandas as pd
import uuid
import numpy as np


def remove_overlaps(table1_path: str, table2_path: str, new_table2_path: str):
    # Load both CSVs
    csv1 = pd.read_csv(table1_path)
    csv2 = pd.read_csv(table2_path)

    # Find duplicate emails in both CSVs
    duplicate_emails = csv2[csv2['Email'].isin(csv1['Email'])]

    # Print duplicate emails
    print("Duplicate Emails:")
    print(duplicate_emails)

    # Remove duplicate emails from the second CSV
    csv2_cleaned = csv2[~csv2['Email'].isin(csv1['Email'])]

    # Dedupe second CSV
    csv2_deduped = csv2_cleaned.drop_duplicates()

    # Save the cleaned second CSV to a new file
    csv2_cleaned.to_csv(new_table2_path, index=False)

    print("Successfully deduped table 2")


def harmonize_column_titles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Harmonizes column titles by converting them to a standard set of titles.
    
    Args:
        df (pd.DataFrame): The input DataFrame whose column titles need to be harmonized.
    
    Returns:
        pd.DataFrame: A new DataFrame with harmonized column titles.
    """
    # Define the mapping of possible column names to standardized column names
    column_mapping = {
        "First Name": "First Name",
        "first_name": "First Name",
        "fname": "First Name",
        "Last Name": "Last Name",
        "last_name": "Last Name",
        "lname": "Last Name",
        "Full Name": "Full Name",
        "full_name": "Full Name",
        "Email": "Email",
        "email": "Email",
        "Phone": "Phone",
        "phone": "Phone",
        "Social Media": "Social Media",
        "social_media": "Social Media",
        "DOB": "DOB",
        "dob": "DOB",
        "Title": "Title",
        "title": "Title",
        "Organization": "Organization",
        "Organisation": "Organization",
        "organization": "Organization",
        "organisation": "Organization",
        "Industry": "Industry",
        "industry": "Industry",
        "Description": "Description",
        "description": "Description",
        "Location": "Location",
        "location": "Location",
        "Organization's Mailing Address": "Organization's Mailing Address",
        "organization's mailing address": "Organization's Mailing Address",
        "Website": "Website",
        "website": "Website",
        "Domain": "Domain",
        "domain": "Domain",
        "LinkedIn URL": "LinkedIn URL",
        "linkedin url": "LinkedIn URL",
        "Specialties": "Specialties",
        "specialties": "Specialties",
        "Size": "Size",
        "size": "Size",
        "Interest Areas": "Interest Areas",
        "Areas of Interest": "Interest Areas",
        "Area of Interests": "Interest Areas",
        "interest_areas": "Interest Areas",
        "Lead Source": "Lead Source",
        "lead_source": "Lead Source",
        "Event Name": "Event Name",
        "event_name": "Event Name",
        "Mailing Address": "Mailing Address",
        "mailing_address": "Mailing Address",
        "Purchasing Agent": "Purchasing Agent",
        "purchasing_agent": "Purchasing Agent",
        "Validated Lead Status": "Validated Lead Status",
        "validated_lead_status": "Validated Lead Status",
        "Ingestion Tag": "Ingestion Tag",
        "ingestion_tag": "Ingestion Tag",
        "Keyword": "Keyword",
        "keyword": "keyword",
        "Name": "Name",
        "name": "Name",
        "Type": "Type",
        "type": "Type",
        "Description": "Description",
        "description": "Description",
        "Synonyms": "Synonyms",
        "synonyms": "Synonyms",
        "Industry": "Industry",
        "industry": "Industry",
        "Created At": "Created At",
        "created_at": "Created At",
        "Last Updated At": "Last Updated At",
        "last_updated_at": "Last Updated At"
    }

    # Rename the columns in the DataFrame
    df = df.rename(columns=column_mapping)
    
    return df


def add_uuid_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a UUID column to the DataFrame. Each row in the DataFrame gets a unique UUID.
    
    Args:
        df (pd.DataFrame): The input DataFrame to which the UUID column will be added.
    
    Returns:
        pd.DataFrame: The DataFrame with an additional 'UUID' column.
    """
    # Generate a UUID for each row and create a new column
    df['UUID'] = [str(uuid.uuid4()) for _ in range(df.shape[0])]
    
    # Reorder columns to make 'UUID' the first column
    columns = ['UUID'] + [col for col in df.columns if col != 'UUID']
    df = df[columns]

    return df


def add_ingestion_tag_column(df: pd.DataFrame, tag: str) -> pd.DataFrame:
    df['Ingestion Tag'] = tag
    return df


def process_keywords(interests_row_value: str) -> list:
    keywords_to_be_added = []

    if ',' in interests_row_value:
        keywords_to_be_added.extend([keyword.strip() for keyword in interests_row_value.split(',')])
    else:
        keywords_to_be_added(interests_row_value.strip())

    return keywords_to_be_added


def extract_unique_keywords(contact_df: pd.DataFrame):
    ignore_list = ["- None -", "N/A", "null"]

    all_keywords = []
    for interests in contact_df["Interest Areas"].dropna():
        keywords = [keyword.strip() for keyword in interests.split(',') if keyword.strip() not in ignore_list]
        all_keywords.extend(keywords)

    unique_keywords = list(set(all_keywords))

    unique_keywords_df = pd.DataFrame(unique_keywords, columns=["Keyword"])

    return unique_keywords_df


def prep_person_df(file_name: str, tag: str):
    person_df = pd.read_csv('data/' + file_name).drop_duplicates()
    person_df = harmonize_column_titles(person_df)
    person_df = add_uuid_column(person_df)
    person_df = add_ingestion_tag_column(person_df, tag)
    person_df.to_csv('data/prepped_' + file_name, index=False)


def prep_organization_df(file_name: str):
    person_df = pd.read_csv('data/' + file_name).drop_duplicates()
    # person_df['Organization'] = person_df['Organization'].str.strip().str.lower().str.title()
    unique_organizations_series = person_df['Organization'].dropna().drop_duplicates().reset_index(drop=True)
    unique_organizations_df = unique_organizations_series.to_frame()
    unique_organizations_df = harmonize_column_titles(unique_organizations_df)
    unique_organizations_df.to_csv('data/organization_list.csv', index=False)


def prep_keyword_df(file_name: str):
    person_df = pd.read_csv('data/' + file_name).drop_duplicates()
    person_df = harmonize_column_titles(person_df)
    unique_keywords_df = extract_unique_keywords(person_df)
    unique_keywords_df.to_csv('data/keyword_list.csv', index=False)


def prep_person_keyword_df(file_name: str):
    prepped_person_df = pd.read_csv('data/prepped_' + file_name)
    prepped_person_df = harmonize_column_titles(prepped_person_df)
    prepped_person_df['Interest Areas'].replace(["- None -", "N/A", "null"], np.nan, inplace=True)
    prepped_person_df = prepped_person_df.dropna(subset=['Interest Areas'])


# Function to standardize the organization names for matching
def standardize_organization(s):
    if pd.isnull(s):
        return s
    return ' '.join(word.capitalize() for word in s.lower().split())


def add_org_uuid_column(file_name: str):
    # Load prepped_person and organization_list DataFrames
    prepped_person_df = pd.read_csv('data/prepped_' + file_name)
    organization_df = pd.read_csv('data/organization_list.csv')

    # Create standardized columns for matching
    prepped_person_df['Organization_match'] = prepped_person_df['Organization'].apply(standardize_organization)
    organization_df['Organization_match'] = organization_df['Organization'].apply(standardize_organization)

    # Merge the DataFrames on the Organization column
    merged_df = pd.merge(prepped_person_df, organization_df, on="Organization", how="left", suffixes=('', '_org'))

    # Rename the UUID_org column to Organization UUID
    merged_df = merged_df.rename(columns={"UUID_org": "Organization UUID"})
    
    # Drop the temporary matching columns
    # merged_df = merged_df.drop(columns=['Organization_match', 'Organization_match_org'])

    # Reorder columns to place Organization UUID immediately after Organization
    org_idx = merged_df.columns.get_loc("Organization") + 1
    cols = merged_df.columns.tolist()
    cols.insert(org_idx, cols.pop(cols.index("Organization UUID")))
    merged_df = merged_df[cols]

    # Display the merged DataFrame
    print(merged_df)

    # Save the updated DataFrame to a new CSV file
    merged_df.to_csv('data/updated_person_df_with_org_uuid.csv', index=False)


# %%
add_org_uuid_column('2019-2023_Leads_List_Test_deduped.csv')

# %%
# prep_person_df('2019-2023_Leads_List_Test_deduped.csv', '2019-23')

# %%
# prep_organization_df('2019-2023_Leads_List_Test_deduped.csv')

# %%
# prep_keyword_df('2019-2023_Leads_List_Test_deduped.csv')

# %%
table1_path = 'data/qiagen_rneasy.csv'
table2_path = 'data/2019-2023_Leads_List_Test_deduped.csv'
new_table2_path = 'data/updated_2019-23_leads.csv'

remove_overlaps(table1_path, table2_path, new_table2_path)

# %%
qiagen_df = pd.read_csv('data/qiagen_rneasy.csv')
print('QIAGEN:', qiagen_df)

# %%
qiagen_df_deduped = qiagen_df.drop_duplicates()
print('DEDUPED QIAGEN:', qiagen_df_deduped)
qiagen_df_deduped.to_csv('data/deduped_qiagen_rneasy.csv', index=False)

# %%
leads1923_df = pd.read_csv('data/2019-2023_Leads_List_Test_deduped.csv')
leads1923_df['Ingestion Tag'] = "2019-23"

# %%
print(leads1923_df)
leads1923_df.to_csv('data/updated_2019-2023_Leads_List_Test_deduped.csv', index=False
                    )
# %%
