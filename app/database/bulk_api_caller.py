import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


class BulkApiCaller:
    def __init__(self, api_key: str, batch_size: int = 10):
        self.api_key = api_key
        self.batch_size = batch_size

    def search_place(self, place):
        response = None  # Initialize response as None
        try:
            # Your request setup and execution code
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()  # This will raise an HTTPError for bad responses
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err} - {response.text if response else "No response"}')
        except requests.exceptions.ConnectionError as conn_err:
            logging.error(f'Connection error occurred: {conn_err}')
        except requests.exceptions.Timeout as timeout_err:
            logging.error(f'Timeout error occurred: {timeout_err}')
        except requests.exceptions.RequestException as req_err:
            logging.error(f'Unexpected error occurred: {req_err}')
        except Exception as e:
            logging.error(f'An unexpected error occurred: {e}')

        # Make sure to return a structured response even in case of failure
        return {"error": True, "status_code": response.status_code if response else 'N/A', "message": response.text if response else 'No response'}

    def search_places_batch(self, places):
        all_results = []

        def make_api_call(place):
            result = self.search_place(place)
            return {"organization_name": place, "result": result}

        with ThreadPoolExecutor(max_workers=self.batch_size) as executor:
            futures = [executor.submit(make_api_call, place) for place in places]
            for future in as_completed(futures):
                all_results.append(future.result())

        return all_results
