# TikTok to BigQuery Integration

This repository contains a set of scripts designed to extract campaign data from TikTok's Business API and insert it into Google BigQuery. The core logic fetches campaign metrics for a specified date range, processes the data, and stores it in BigQuery for further analysis.

## Requirements

- Python 3.x
- Google Cloud SDK
- Google Cloud BigQuery
- TikTok Business API Access Token
- Required Python Libraries:
  - `pandas`
  - `requests`
  - `google-cloud-bigquery`
  - `six`
  - `numpy`

You can install the required libraries using:

```bash
pip install -r requirements.txt
```

## Project Structure

```
.
├── tiktok_to_bq.py          # Functions for BigQuery interactions
├── main.py                  # Main script for processing TikTok data
├── tiktok_cred.json         # TikTok API credentials file (should be added to .gitignore)
└── requirements.txt         # Python dependencies
```

## Functions

### `read_json(file_json)`
Reads a JSON file and returns its content.

**Parameters:**
- `file_json` (str): Path to the JSON file.

**Returns:**
- Dictionary containing the JSON data.

### `interval_day(choice)`
Returns a date based on the input choice. Options include 'yesterday' or 'past_30' for the last 30 days.

**Parameters:**
- `choice` (str): Specifies the time range.

**Returns:**
- Date in `YYYY-MM-DD` format.

### `build_url(path, query="")`
Builds the URL for making requests to TikTok's Business API.

**Parameters:**
- `path` (str): API endpoint path.
- `query` (str): Optional query parameters.

**Returns:**
- Full URL string.

### `get_campaign_dateails(json_str, access_token)`
Fetches campaign details from TikTok's Business API.

**Parameters:**
- `json_str` (str): JSON-formatted string with the request parameters.
- `access_token` (str): TikTok Business API access token.

**Returns:**
- Pandas DataFrame with campaign metrics and dimensions.

### `main_tiktok(event, context)`
Main entry point for triggering the data extraction process. This function is typically used with Google Cloud Functions.

**Parameters:**
- `event` (dict): Event data.
- `context` (object): Metadata about the event.

**Returns:**
- None.

This function fetches the campaign data, processes it, and stores it in BigQuery.

## Setup

1. **Credentials:**  
   Make sure to provide the TikTok API access token in the `tiktok_cred.json` file. This file should be stored securely and **not** included in version control.

2. **Google Cloud:**  
   You must set up a Google Cloud Project with BigQuery enabled. The script uses `google-cloud-bigquery` to interact with BigQuery.

3. **Environment Variables:**  
   Set up environment variables for the following values:
   - `GOOGLE_APPLICATION_CREDENTIALS`: Path to your Google Cloud credentials JSON file.

4. **Running the Script:**  
   To run the script locally, use the following command:
   ```bash
   python main.py
   ```

## BigQuery Integration

The script checks if the dataset and table exist in BigQuery and inserts the campaign data if necessary. Ensure that the dataset and table names are correctly specified in the event trigger.

## Security

- **API Tokens:** Keep your TikTok API access token safe and ensure it is not exposed.
- **BigQuery Access:** Ensure that the Google Cloud account has the appropriate permissions to access BigQuery and insert data into the specified tables.
