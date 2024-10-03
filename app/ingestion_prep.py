# %%
import pandas as pd
import uuid
import numpy as np
import json
from tqdm import tqdm

# %%
def remove_overlaps(table1_path: str, table2_path: str, new_table2_path: str, duplicates_path: str):
    csv1 = pd.read_csv(table1_path)
    csv2 = pd.read_csv(table2_path)
    duplicate_emails = csv2[csv2['Email'].isin(csv1['Email'])]
    print("Duplicate Emails:")
    print(duplicate_emails)
    duplicate_emails.to_csv(duplicates_path, index=False)
    csv2_cleaned = csv2[~csv2['Email'].isin(csv1['Email'])]
    csv2_deduped = csv2_cleaned.drop_duplicates()
    csv2_deduped.to_csv(new_table2_path, index=False)
    print("Successfully deduped table 2 based on emails")


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
        "Company": "Organization",
        "company": "Organization",
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
        "keyword": "Keyword",
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
        "last_updated_at": "Last Updated At",
        "#Cases": "Number of Cases",
        "Score_Case": "Score Case",
        "Qty.": "Quantity",
        "Sales($)": "Sales",
        "#Orders": "Number of Orders",
        "Score_Sales": "Score Sales",
        "Score_Total": "Score Total",
        "Rank": "Rank",
        "Cases Tier": "Cases Tier",
        "Sales Tier": "Sales Tier",
        "Message Tier": "Message Tier",
        "Orders Tier": "Orders Tier",
        "Department/Contact": "Full Name",
        "Job Title": "Title",
        "Opens": "Opens",
        "opens": "Opens",
        "Clicks": "Clicks",
        "clicks": "Clicks",
        "Conversions": "Conversions",
        "conversions": "Conversions"
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


def add_ingestion_tag_and_data_source_columns(
    df: pd.DataFrame, 
    node_tag: str, 
    node_source: str,
    edge_tag: str,
    edge_source: str
) -> pd.DataFrame:
    df['Node Ingestion Tag'] = node_tag
    df['Node Data Source'] = node_source
    df['Edge Ingestion Tag'] = edge_tag
    df['Edge Data Source'] = edge_source
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


def prep_person_df(
    file_path: str, 
    file_name: str, 
    node_tag: str, 
    node_source: str,
    edge_tag: str,
    edge_source: str
):
    person_df = pd.read_csv(f'{file_path}/{file_name}').drop_duplicates()
    person_df = harmonize_column_titles(person_df)
    person_df = add_uuid_column(person_df)
    person_df = add_ingestion_tag_and_data_source_columns(person_df, node_tag, node_source, edge_tag, edge_source)
    if "Full Name" not in person_df.columns:
        person_df["Full Name"] = person_df["First Name"] + " " + person_df["Last Name"]
    person_df.to_csv(f'{file_path}/prepped_{file_name}', index=False)


def prep_organization_df(file_name: str):
    person_df = pd.read_csv(f'{file_path}/prepped_{file_name}').drop_duplicates()
    person_df['Organization Standardized Name'] = person_df['Organization'].str.strip().str.lower()
    original_organizations_df = person_df[['Organization Standardized Name', 'Organization']].drop_duplicates()
    original_organizations_df = original_organizations_df.rename(columns={"Organization": "Display Name"})
    unique_organizations_series = person_df['Organization Standardized Name'].dropna().drop_duplicates().reset_index(drop=True)
    unique_organizations_df = unique_organizations_series.to_frame()
    unique_organizations_df = harmonize_column_titles(unique_organizations_df)
    unique_organizations_df = pd.merge(unique_organizations_df, original_organizations_df, on='Organization Standardized Name', how='left')
    unique_organizations_df = unique_organizations_df.drop_duplicates(subset='Organization Standardized Name')
    unique_organizations_df.to_csv(f'{file_path}/organization_list_{file_name}', index=False)


def prep_keyword_df(file_path: str, file_name: str):
    person_df = pd.read_csv({file_path}/{file_name}).drop_duplicates()
    person_df = harmonize_column_titles(person_df)
    unique_keywords_df = extract_unique_keywords(person_df)
    unique_keywords_df.to_csv(f'{file_path}/keyword_list_{file_name}', index=False)


def prep_edges_df(file_path: str, file_name: str, column_name: str, source_node_label: str, target_node_label: str):
    prepped_source_df = pd.read_csv(f'{file_path}/prepped_{file_name}')
    prepped_source_df = harmonize_column_titles(prepped_source_df)
    prepped_source_df[column_name].replace(["- None -", "N/A", "null"], np.nan, inplace=True)
    cleaned_df = prepped_source_df.dropna(subset=[column_name]).reset_index(drop=True)
    cleaned_df.to_csv(f'{file_path}/edge_{source_node_label}_{target_node_label}_{file_name}', index=False)


def prep_edges_same_keyword_df(file_path: str, original_file_name: str, keywords: list, target_node_label: str):
    prepped_person_df = pd.read_csv(f'{file_path}/prepped_{original_file_name}')
    keyword_str = ', '.join(keywords)
    prepped_person_df["Keywords"] = keyword_str
    prepped_person_df.to_csv(f'{file_path}/keyword_added_{target_node_label}_{original_file_name}', index=False)

def prep_edges_same_campaign_df(file_path: str, original_file_name: str, campaign_id: str, target_node_label: str): 
    marketing_recipients_df = pd.read_csv(f'{file_path}/keyword_added_{target_node_label}_{original_file_name}')
    marketing_recipients_df["Campaign ID"] = campaign_id
    marketing_recipients_df.to_csv(f'{file_path}/campaign_added_{target_node_label}_{original_file_name}', index=False)

# %%
current_file_name = 'recipients_directzol_RNA_NGS_US'
current_file_directory = 'directzol_RNA_NGS_US'
previous_table_path = 'data/klaviyo/directzol/directzol_miRNA/recipients_directzol_miRNA.csv'
current_table_path = f'data/klaviyo/directzol/{current_file_directory}/{current_file_name}.csv'
new_current_table_path = f'data/klaviyo/directzol/{current_file_directory}/deduped_{current_file_name}.csv'
duplicates_path = f'data/klaviyo/directzol/{current_file_directory}/duplicates_{current_file_name}.csv'

remove_overlaps(previous_table_path, current_table_path, new_current_table_path, duplicates_path)

# %%
file_path = 'data/klaviyo/directzol/directzol_RNA_NGS_INTL/'
file_name = 'recipients_directzol_RNA_NGS_INTL.csv'

# %%
prep_person_df(
    file_path=file_path, 
    file_name=file_name, 
    node_tag="klaviyo", 
    node_source="Klaviyo Analytics",
    edge_tag="klaviyo_directzol_RNA_NGS_INTL",
    edge_source="Klaviyo Direct-zol RNA - NGS- INTL"
)

# %%
klaviyo_keywords = ["Klaviyo", "Emails", "Marketing", "Marketing Campaigns", "Email Marketing Data", "Direct-zol", "RNA", "NGS", "International"]
prep_edges_same_keyword_df(file_path=file_path, original_file_name=file_name, keywords=klaviyo_keywords, target_node_label="person_node")

# %%
prep_edges_same_campaign_df(file_path=file_path, original_file_name=file_name, campaign_id='JBEpX8', target_node_label="person_node")

# %% PREP DIRECTZOL CAMPAIGNS WITH COMMON STRING VALUES:
directzol_campaigns_df = pd.read_csv('data/klaviyo/directzol/directzol_campaigns.csv')
directzol_campaigns_df["Directory Name"] = "directzol_"
directzol_campaigns_df["Keywords"] = "Klaviyo, Emails, Marketing, Marketing Campaigns, Email Marketing Data, Direct-zol, "
directzol_campaigns_df.to_csv('data/klaviyo/directzol/prepped_directzol_campaigns.csv')

# %% TO DO: modify this to loop over the prepped directzol campaign DF:
def prep_each_directzol_campaign_df(dir_name: str, campaign_name: str, keywords: list, campaign_id: str):
    file_path=f'data/klaviyo/directzol/{dir_name}'
    file_name=f'recipients_{dir_name}.csv' 
    
    prep_person_df(
        file_path=file_path, 
        file_name=file_name, 
        node_tag="klaviyo", 
        node_source="Klaviyo Analytics",
        edge_tag=f"klaviyo_{dir_name}",
        edge_source=f"Klaviyo {campaign_name}"
    )
    prep_edges_same_keyword_df(file_path=file_path, original_file_name=file_name, keywords=keywords, target_node_label="person_node")
    prep_edges_same_campaign_df(file_path=file_path, original_file_name=file_name, campaign_id=campaign_id, target_node_label="person_node")

# %%
prepped_directzol_campaigns = pd.read_csv('data/klaviyo/directzol/prepped_directzol_campaigns.csv')
for _, row in tqdm(
    prepped_directzol_campaigns.iterrows(),
    total=prepped_directzol_campaigns.shape[0],
    desc="Preparing each directzol campaign"
):
    prep_each_directzol_campaign_df(
        dir_name=row.get("Directory Name"),
        campaign_name=row.get("Campaign Name"),
        keywords=row.get("Keywords"),
        campaign_id=row.get("Campaign ID")
    )

# %%
klaviyo_keywords = ["Klaviyo", "Emails", "Marketing", "Marketing Campaigns", "Email Marketing Data", "Direct-zol", "RNA", "NGS", "International"]
prep_each_directzol_campaign_df(
    dir_name="directzol_RNA_NGS_INTL",
    campaign_name="Direct-zol RNA - NGS- INTL",
    keywords=klaviyo_keywords,
    campaign_id="JBEpX8"
)

# %%
prep_organization_df(file_path=file_path, file_name=file_name)

# %%
prep_keyword_df(file_path=file_path, file_name=file_name)

# %%
prep_edges_df(file_path=file_path, file_name=file_name, column_name="Organization", target_node_label="organization")

# %%
prep_edges_df(file_path=file_path, file_name=file_name, column_name="Keywords", target_node_label="keyword")

# %% Filter for campaigns related to Direct-zol
marketing_campaigns_df = pd.read_csv('data/klaviyo/all_sent_campaigns.csv')
filtered_marketing_campaigns_df = marketing_campaigns_df[marketing_campaigns_df["Campaign Name"].str.contains("Direct-zol", case=False, na=False)]
filtered_marketing_campaigns_df.to_csv('data/klaviyo/directzol_campaigns.csv')

# %%
original_df = pd.read_csv('data/2019-23_scileads/2019-2023_Leads_List_Test_deduped.csv')
filtered_df = original_df[
    original_df["Email"].str.contains("USDA", case=False, na=False) |
    original_df["Organization"].str.contains("USDA", case=False, na=False)
]
filtered_df.to_csv('data/2019-23_scileads/2019-23_usda_leads.csv', index=False)

# # %%
# qiagen_df = pd.read_csv('data/qiagen_rneasy.csv')
# print('QIAGEN:', qiagen_df)

# # %%
# qiagen_df_deduped = qiagen_df.drop_duplicates()
# print('DEDUPED QIAGEN:', qiagen_df_deduped)
# qiagen_df_deduped.to_csv('data/deduped_qiagen_rneasy.csv', index=False)

# # %%
# leads1923_df = pd.read_csv('data/2019-2023_Leads_List_Test_deduped.csv')
# leads1923_df['Ingestion Tag'] = "2019-23"

# # %%
# print(leads1923_df)
# leads1923_df.to_csv('data/updated_2019-2023_Leads_List_Test_deduped.csv', index=False
#                     )

# %%
microbiomics_products_df = pd.read_csv('data/netsuite_products/microbiomics_products.csv')
skus_list = microbiomics_products_df['Item name/SKU#'].tolist()
skus_string = ', '.join(skus_list)
print(skus_string)

# %% ZYMO PRODUCT INGESTION
# Load the CSV files into DataFrames
table1 = pd.read_csv('data/netsuite_products.csv')
table2 = pd.read_csv('data/product_enrichment.csv')

# Select specific columns from netsuite_products table
table1 = table1[['Item name/SKU#', 'Product Category', 'Class (no hierarchy)', 'Type', 'Base Price', 'Inactive', 'Shelf Life (Months)', 'Storage Temperature', 'Shipping Temperature', 'Length', 'Width', 'Height', 'Weight', 'Available']]

# Merge the "Highlight A", "Highlight B", and "Highlight C" columns into one "Features" column
table2['Features'] = table2[['Highlight A', 'Highlight B', 'Highlight C']].apply(lambda x: ' '.join(x.dropna()), axis=1)

# Select specific columns from product_enrichment table
table2 = table2[['Catalog Number', 'Description', 'Short Description', 'Features', 'Safety Data Sheet/ MSDS URL', 'Product URL', 'Link for Main image of item']]

# Merge the DataFrames on the matching columns
merged_table = pd.merge(table1, table2, left_on='Item name/SKU#', right_on='Catalog Number', how='left')

# Drop the redundant 'Catalog Number' column from the merged DataFrame
merged_table = merged_table.drop(columns=['Catalog Number'])

# Save the merged table to a new CSV file
merged_table.to_csv('data/merged_netsuite_products.csv', index=False)

# %% PUBLICATION PRODUCT INGESTION
def generate_uuid():
    return str(uuid.uuid4())

def add_uuids_to_products(publication_products):
    for publication_product in publication_products:
        for product in publication_product["products"]:
            product["uuid"] = generate_uuid()

def save_json_file(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=2)

def load_json_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# %%
input_file_path = 'data/publication_product_data.json'
output_file_path = 'data/uuid_publication_product_data.json'

publication_products = load_json_file(input_file_path)

# Add UUIDs to publication_products and save to new file
add_uuids_to_products(publication_products)
save_json_file(publication_products, output_file_path)

# %%
article_metadata = load_json_file('data/article_metadata.json')
article_keywords = []
for article in article_metadata:
    keywords = article["search_term"]
    if keywords not in article_keywords:
        article_keywords.append(keywords)
print(article_keywords)

# %% MICROBIOMICS CUSTOMERS
microbiomics_customers_df = pd.read_csv('/Users/nicoletrieu/Documents/zymo/business-intelligence/shopify/microbiomics_orders.csv')
columns_to_keep = ['ID', 'Name', 'Number', 'Phone', 'Email', 'Created At', 'Updated At', 'Processed At', 'Closed At', 
                   'Source', 'Customer: ID', 'Customer: Email', 'Customer: Phone', 'Customer: First Name', 
                   'Customer: Last Name', 'Customer: Tags', 'Customer: Email Marketing Status', 
                   'Customer: SMS Marketing Status', 'Customer: Tax Exempt', 'Customer: Orders Count', 
                   'Customer: Total Spent', 'Billing: First Name', 'Billing: Last Name', 'Billing: Name', 
                   'Billing: Company', 'Billing: Phone', 'Billing: Address 1', 'Billing: Address 2', 
                   'Billing: Zip', 'Billing: City', 'Billing: Province', 'Billing: Province Code', 
                   'Billing: Country', 'Billing: Country Code', 'Shipping: First Name', 'Shipping: Last Name', 
                   'Shipping: Name', 'Shipping: Company', 'Shipping: Phone', 'Shipping: Address 1', 
                   'Shipping: Address 2', 'Shipping: Zip', 'Shipping: City', 'Shipping: Province', 
                   'Shipping: Province Code', 'Shipping: Country', 'Shipping: Country Code', 'Line: Title', 
                   'Line: Name', 'Line: Variant Title', 'Line: SKU', 'Line: Quantity', 'Line: Price', 
                   'Line: Discount', 'Line: Discount Allocation', 'Line: Discount per Item', 'Line: Total', 
                   'Line: Vendor']
df_filtered = microbiomics_customers_df[columns_to_keep]
df_sorted = df_filtered.sort_values(by='Customer: Orders Count', ascending=False)
df_deduped = df_sorted.drop_duplicates(subset=['Customer: First Name', 'Customer: Last Name'], keep='first')
print(df_deduped)
df_deduped.to_csv('/Users/nicoletrieu/Documents/zymo/business-intelligence/shopify/microbiomics_customers.csv')

# %%
