# %%
import requests
import logging
import json
import pandas as pd
import os
from time import sleep
from tqdm.auto import tqdm
from database.bulk_api_caller import BulkApiCaller

# %%
# print(os.getcwd())

# %%
with open('config.json', 'r') as file:
    config = json.load(file)
google_maps_places_api_key = config['apiKeys']['googleMapsPlaces']

# Ensure the 'results' directory exists
results_dir = 'results'
if not os.path.exists(results_dir):
    os.makedirs(results_dir)

# Specify the path including the 'results' folder
file_path = os.path.join(results_dir, '2019-2023_addresses.csv')


# %%
def search_place(place, api_key):
    url = 'https://places.googleapis.com/v1/places:searchText'
    payload = {"textQuery": f"address for {place}"}
    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': api_key,
        'X-Goog-FieldMask': 'places.displayName,places.formattedAddress,places.priceLevel'
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # This will raise an HTTPError for bad responses
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err} - {response.text}')
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f'Connection error occurred: {conn_err}')
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f'Timeout error occurred: {timeout_err}')
    except requests.exceptions.RequestException as req_err:
        logging.error(f'Unexpected error occurred: {req_err}')
    except Exception as e:
        logging.error(f'An unexpected error occurred: {e}')

    # Return a detailed error message or a structured error object for further processing
    return {"error": True, "status_code": response.status_code if response else 'N/A', "message": response.text if response else 'No response'}


def filter_best_match(result_list: list, place_name: str):
    key_words = set(word.lower() for word in place_name.replace(',', '').split())
    best_match = None

    for result in result_list:
        display_name_field = result["displayName"]["text"]
        address_field = result["formattedAddress"]
        matched_words = sum(word.lower() in display_name_field.lower() for word in key_words)
        match_percentage = matched_words / len(key_words)

        if match_percentage == 1:
            best_match = address_field

        elif match_percentage >= 0.5:
            best_match = address_field

        elif match_percentage < 0.5:
            best_match = result_list[0]["formattedAddress"]

    return {
        "best_match_address": best_match,
        "display_name": display_name_field
    }


def get_organization_address(organizations_df: pd.DataFrame, api_key: str, file_path: str):
    all_results = []

    # Iterate over the DataFrame rows as (index, Series) pairs
    for index, row in tqdm(organizations_df.iterrows(), desc="Getting addresses", total=organizations_df.shape[0]):
        organization_name = row["Organization"]

        search_result = search_place(organization_name, api_key)

        if search_result == {} or search_result.get("error"):
            print(search_result.get("message", "Unknown error"))  # Log the error message if available
            # Even in case of error, append a dict to maintain one-to-one row correspondence
            all_results.append({"display_name": None, "address": None})
            continue

        result_list = search_result.get("places", [])
        if not result_list:
            all_results.append({"display_name": None, "address": None})
            continue

        best_match_info = filter_best_match(result_list, organization_name)
        # Append results with original organization name for merging
        all_results.append({
            "Organization": organization_name,  # Ensure this matches the column name in organizations_df exactly
            "Display Name": best_match_info.get("display_name"),
            "Address": best_match_info.get("best_match_address")
        })

    # Convert all results into a DataFrame
    results_df = pd.DataFrame(all_results)

    # Merge the results with the original DataFrame to keep all columns
    # Ensure both DataFrames have a common column ('Organization' in this case) to merge on
    final_df = organizations_df.merge(results_df, on="Organization", how="left")

    # Save the merged DataFrame to CSV
    final_df.to_csv(
        file_path,
        sep=',',
        index=False,
        encoding='utf-8'
    )

    return final_df


def get_organization_address_bulk(organizations_df: pd.DataFrame, api_key: str, file_path: str):
    bulk_api_caller = BulkApiCaller(api_key=api_key, batch_size=5)
    all_results = []

    # Define your batch size and delay for throttling
    mini_batch_size = 5
    delay_between_batches = 12  # seconds, adjust based on rate limits

    # Iterate over DataFrame in mini-batches
    for i in tqdm(range(0, len(organizations_df), mini_batch_size), desc="Processing organizations"):
        mini_batch_df = organizations_df.iloc[i:i+mini_batch_size]
        mini_batch_list = mini_batch_df["Organization"].tolist()
        batch_results = bulk_api_caller.search_places_batch(mini_batch_list)

        for index, result in enumerate(batch_results):
            organization_name = result["organization_name"]
            search_results = result["result"]

            if search_results.get("error"):
                print(search_results.get("message"))
                # Append None values for missing or errored results
                all_results.append((organization_name, None, None))
            else:
                result_list = search_results.get("places", [])
                if result_list:
                    best_match_info = filter_best_match(result_list, organization_name)
                    all_results.append((organization_name, best_match_info.get("display_name"), best_match_info.get("best_match_address")))
                else:
                    all_results.append((organization_name, None, None))

        # Throttle requests by sleeping between mini-batches
        if i + mini_batch_size < len(organizations_df):  # Avoid sleeping on the last batch
            sleep(delay_between_batches)

    # Create a DataFrame from the results
    results_df = pd.DataFrame(all_results, columns=["Organization", "Display Name", "Address"])

    # Merge the original DataFrame with the results DataFrame
    final_df = organizations_df.merge(results_df, on="Organization", how="left")

    # Save the merged DataFrame to CSV
    final_df.to_csv(
        file_path,
        sep=',',
        index=False,
        encoding='utf-8'
    )

    return final_df


# %%
# Load the dataframe
contact_df = pd.read_csv("data/2019-2023_Leads_List_Test_deduped.csv")

# Clean the 'Organization' column
contact_df['Organization'] = contact_df['Organization'].str.strip().str.lower().str.title()

# Remove rows with missing 'Organization' values
contact_df = contact_df.dropna(subset=['Organization'])

# Remove duplicates based on 'Organization' while keeping all original columns
unique_organizations_df = contact_df.drop_duplicates(subset=['Organization'])

# Now, contact_df is your unique_organizations_df with all original columns retained
print("unique_organizations_df:\n", unique_organizations_df)

# %%
small_sample_df = unique_organizations_df.sample(n=10, random_state=1)
print("small_sample_df:\n", small_sample_df)

# %%
sample_list = small_sample_df["Organization"].tolist()
print(sample_list)

# %%
sample_addresses = get_organization_address(small_sample_df, google_maps_places_api_key, file_path)
print(sample_addresses)

# %%
sample_bulk_addresses = get_organization_address_bulk(small_sample_df, google_maps_places_api_key, file_path)
print(sample_bulk_addresses)

# %%
sample_search = search_place("Raksan Investors", google_maps_places_api_key)
print(sample_search)

# %%
test_addresses = get_organization_address(unique_organizations_df, google_maps_places_api_key, file_path)
print(test_addresses)

# %%
bulk_api_caller = BulkApiCaller(api_key=google_maps_places_api_key, batch_size=5)
results = bulk_api_caller.search_places_batch(sample_list)
print(results)

# %%
test_bulk_addresses = get_organization_address_bulk(unique_organizations_df, google_maps_places_api_key, file_path)
print(test_bulk_addresses)

# %%
