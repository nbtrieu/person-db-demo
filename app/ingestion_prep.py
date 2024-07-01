# %%
import pandas as pd


def remove_duplicates(table1_path: str, table2_path: str, new_table2_path: str):
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


# %%
table1_path = 'data/qiagen_rneasy.csv'
table2_path = 'data/2019-2023_Leads_List_Test_deduped.csv'
new_table2_path = 'data/updated_2019-23_leads.csv'

remove_duplicates(table1_path, table2_path, new_table2_path)

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
