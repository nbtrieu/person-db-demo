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
        "Customer ID": "Customer ID",
        "customerId": "Customer ID",
        "customer_id": "Customer ID",
        "CustomerID": "Customer ID",
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
        "Previous Titles": "Previous Titles",
        "previous titles": "Previous Titles",
        "previous_titles": "Previous Titles",
        "previousTitles": "Previous Titles",
        "Organization": "Organization",
        "Organisation": "Organization",
        "organization": "Organization",
        "organisation": "Organization",
        "Previous Organizations": "Previous Organizations",
        "previous organizations": "Previous Organizations",
        "previous_organizations": "Previous Organizations",
        "previousOrganizations": "Previous Organizations",
        "Tentative Organizations": "Tentative Organizations",
        "tentative organizations": "Tentative Organizations",
        "tentative_organizations": "Tentative Organizations",
        "tentativeOrganizations": "Tentative Organizations",
        "Organization Standardized Name": "Organization Standardized Name",
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
        "Keywords": "Keywords",
        "Keyword": "Keywords",
        "Interest Areas": "Keywords",
        "Areas of Interest": "Keywords",
        "Area of Interests": "Keywords",
        "Zymo Research Keywords Found": "Keywords",
        "interest_areas": "Keywords",
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


def add_ingestion_tag_and_data_source_columns(df: pd.DataFrame, tag: str, source: str) -> pd.DataFrame:
    df['Ingestion Tag'] = tag
    df['Data Source'] = source
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
    for interests in contact_df["Keywords"].dropna():
        keywords = [keyword.strip() for keyword in interests.split(',') if keyword.strip() not in ignore_list]
        all_keywords.extend(keywords)

    unique_keywords = list(set(all_keywords))

    unique_keywords_df = pd.DataFrame(unique_keywords, columns=["Keywords"])

    return unique_keywords_df


def prep_person_df(file_name: str, tag: str, source: str):
    person_df = pd.read_csv('data/' + file_name).drop_duplicates()
    person_df = harmonize_column_titles(person_df)
    person_df = add_uuid_column(person_df)
    person_df = add_ingestion_tag_and_data_source_columns(person_df, tag, source)
    person_df.to_csv('data/prepped_' + file_name, index=False)


def prep_organization_df(file_name: str):
    person_df = pd.read_csv('data/prepped_' + file_name).drop_duplicates()
    person_df['Organization Standardized Name'] = person_df['Organization'].str.strip().str.lower()
    
    # Create a DataFrame with original and standardized organization names
    original_organizations_df = person_df[['Organization Standardized Name', 'Organization']].drop_duplicates()
    
    # Keep the original formatting for the display name
    original_organizations_df = original_organizations_df.rename(columns={"Organization": "Display Name"})
    
    # Get unique standardized organization names
    unique_organizations_series = person_df['Organization Standardized Name'].dropna().drop_duplicates().reset_index(drop=True)
    
    # Convert to DataFrame
    unique_organizations_df = unique_organizations_series.to_frame()
    
    # Harmonize column titles
    unique_organizations_df = harmonize_column_titles(unique_organizations_df)
    
    # Merge to add the Display Name column
    unique_organizations_df = pd.merge(unique_organizations_df, original_organizations_df, on='Organization Standardized Name', how='left')
    
    # Remove rows with duplicate values for the "Organization Standardized Name"
    unique_organizations_df = unique_organizations_df.drop_duplicates(subset='Organization Standardized Name')
    
    # Save to CSV
    unique_organizations_df.to_csv('data/organization_list_' + file_name, index=False)


def prep_keyword_df(file_name: str):
    person_df = pd.read_csv('data/' + file_name).drop_duplicates()
    person_df = harmonize_column_titles(person_df)
    unique_keywords_df = extract_unique_keywords(person_df)
    unique_keywords_df.to_csv('data/keyword_list_' + file_name, index=False)


def prep_edges_df(file_name: str, column_name: str, target_node_label: str):
    prepped_person_df = pd.read_csv('data/prepped_' + file_name)
    prepped_person_df = harmonize_column_titles(prepped_person_df)
    prepped_person_df[column_name].replace(["- None -", "N/A", "null"], np.nan, inplace=True)
    cleaned_df = prepped_person_df.dropna(subset=[column_name]).reset_index(drop=True)
    cleaned_df.to_csv('data/cleaned_' + target_node_label + '_' + file_name, index=False)


# %%
file_name = '2019-2023_Leads_List_Test_deduped.csv'

# %%
prep_person_df(file_name=file_name, tag="2019-23", source="tradeshows")

# %%
prep_organization_df(file_name=file_name)

# %%
prep_keyword_df(file_name=file_name)

# %%
prep_edges_df(file_name=file_name, column_name="Organization", target_node_label="organization")

# %%
prep_edges_df(file_name=file_name, column_name="Keywords", target_node_label="keyword")

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
