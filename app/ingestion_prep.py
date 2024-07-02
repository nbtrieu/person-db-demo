# %%
import pandas as pd
import uuid


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
        "organization": "Organization",
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
        "ingestion_tag": "Ingestion Tag"
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


def prep_df(file_name: str, tag: str):
    contact_df = pd.read_csv('data/' + file_name)
    contact_df = harmonize_column_titles(contact_df)
    contact_df = add_uuid_column(contact_df)
    contact_df = add_ingestion_tag_column(contact_df, tag)
    contact_df.to_csv('data/prepped_' + file_name, index=False)


# %%
prep_df('2019-2023_Leads_List_Test_deduped.csv', '2019-23')

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
