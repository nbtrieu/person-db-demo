# %%
import requests
import logging
import json
import pandas as pd
import os
import time
from tqdm import tqdm
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
        "best_match_addresss": best_match,
        "display_name": display_name_field
    }


def get_organization_address(
    organizations_df: pd.DataFrame, api_key: str, file_path: str
):
    all_results = []
    organization_names_list = organizations_df["Organization"].tolist()

    for organization_name in tqdm(
        organization_names_list,
        desc="Getting addresses"
    ):
        organization_results = []  # Storing results for each organization separately

        search_result = search_place(organization_name, api_key)

        if search_result == {}:
            continue  # Skip empty search result
        if search_result.get("error"):
            print(search_result.get("message"))  # Log the error message
            continue

        result_list = search_result.get("places", [])
        if not result_list:
            # If no results found, append the organization with address as None
            result_dict = {
                "organization_name": organization_name,
                "address": None  # Use None for the address
            }
            organization_results.append(result_dict)
        else:
            # Find the best match:
            best_match_address = filter_best_match(result_list, organization_name)
            if best_match_address:
                result_dict = {
                    "organization_name": organization_name,
                    "address": best_match_address
                }
                organization_results.append(result_dict)

        # Combine all organization results into the main results list
        all_results.extend(organization_results)

    results_df = pd.DataFrame(all_results)
    results_df.to_csv(
        file_path,
        sep=',',
        columns=['organization_name', 'address'],
        header=True,
        index=False,
        encoding='utf-8'
    )

    return results_df


def get_organization_address_bulk(
    organizations_df: pd.DataFrame, api_key: str, file_path: str
):
    bulk_api_caller = BulkApiCaller(api_key=api_key, batch_size=5)

    organization_names_list = organizations_df["Organization"].tolist()
    batch_results = bulk_api_caller.search_places_batch(organization_names_list)

    all_results = []
    for result in tqdm(batch_results, desc="Getting addresses in bulk"):
        organization_name = result["organization_name"]
        search_results = result["result"]

        # Initialize default values
        result_dict = {
            "organization_name": organization_name,
            "display_name": None,
            "address": None,
        }

        if search_results.get("error"):
            print(search_results.get("message"))
        else:
            result_list = search_results.get("places", [])
            if result_list:
                # Assuming filter_best_match returns the best match from result_list
                best_match_info = filter_best_match(result_list, organization_name)
                print(best_match_info)
                if best_match_info:
                    # If a best match is found, update address and display_name in result_dict
                    result_dict["display_name"] = best_match_info["display_name"]
                    result_dict["address"] = best_match_info["best_match_addresss"]

        all_results.append(result_dict)

    # Convert all results into a DataFrame
    results_df = pd.DataFrame(all_results)
    # Specify the columns to include in the CSV
    results_df.to_csv(
        file_path,
        sep=',',
        columns=['organization_name', 'display_name', 'address'],
        header=True,
        index=False,
        encoding='utf-8'
    )

    return results_df


# %%
contact_df = pd.read_csv("data/2019-2023_Leads_List_Test_deduped.csv")

contact_df['Organization'] = contact_df['Organization'].str.strip().str.lower().str.title()

# Remove duplicates and drop rows with missing 'Organization' values
unique_organizations_series = contact_df['Organization'].dropna().drop_duplicates().reset_index(drop=True)
# Convert Pandas Series to DataFrame
unique_organizations_df = unique_organizations_series.to_frame()
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
