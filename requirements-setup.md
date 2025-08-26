# YouTube Data Collector - Setup Guide

## Requirements File (requirements.txt)

```txt
streamlit>=1.28.0
pandas>=2.0.0
google-api-python-client>=2.100.0
youtube-transcript-api>=0.6.1
gspread>=5.12.0
google-auth>=2.23.0
google-auth-oauthlib>=1.1.0
isodate>=0.6.1
```

## Installation & Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. YouTube API Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing
3. Enable YouTube Data API v3
4. Create credentials (API Key)
5. Copy the API key for use in the app

### 3. Google Sheets API Setup

1. In Google Cloud Console:
   - Enable Google Sheets API
   - Enable Google Drive API
   
2. Create Service Account:
   - Go to "Credentials" → "Create Credentials" → "Service Account"
   - Name your service account
   - Grant it "Editor" role
   - Click "Done"
   
3. Generate JSON Key:
   - Click on your service account
   - Go to "Keys" tab
   - Click "Add Key" → "Create new key"
   - Choose JSON format
   - Download the JSON file
   
4. Setup Google Sheet Access:
   - Copy the service account email from the JSON (client_email field)
   - Share your Google Sheet with this email address (give Editor access)
   - Or let the app create a new sheet automatically

### 4. Run the Application

```bash
streamlit run youtube_collector.py
```

## n8n Workflow Integration

### Option 1: Direct Google Sheets Integration

1. **In n8n:**
   - Add a "Google Sheets" node
   - Configure authentication with your Google account
   - Set it to read from the spreadsheet created by the app
   - Set trigger: "On Row Added" or poll periodically

2. **Workflow Example:**
   ```
   Google Sheets Trigger → Process Data → Your Actions
   ```

### Option 2: Webhook Integration (Alternative)

If you want real-time updates, modify the Streamlit app to send webhooks:

```python
import requests

def send_to_n8n(video_data):
    webhook_url = "https://your-n8n-instance.com/webhook/youtube-collector"
    response = requests.post(webhook_url, json=video_data)
    return response.status_code == 200
```

### Option 3: Database Integration

For production use, you might want to add database support:

```python
# Add to requirements.txt:
# sqlalchemy>=2.0.0
# psycopg2-binary>=2.9.0  # for PostgreSQL

from sqlalchemy import create_engine

def save_to_database(videos, connection_string):
    engine = create_engine(connection_string)
    df = pd.DataFrame(videos)
    df.to_sql('raw_links', engine, if_exists='append', index=False)
```

## Environment Variables (Optional)

Create a `.env` file for sensitive data:

```env
YOUTUBE_API_KEY=your_youtube_api_key_here
GOOGLE_SHEETS_CREDS={"type":"service_account","project_id":"..."}
SPREADSHEET_NAME=YouTube_Collection_Data
```

Then modify the app to use environment variables:

```python
import os
from dotenv import load_dotenv

load_dotenv()

youtube_api_key = os.getenv('YOUTUBE_API_KEY', '')
sheets_creds = os.getenv('GOOGLE_SHEETS_CREDS', '')
```

## Data Structure

The app exports data with the following fields:

| Field | Type | Description |
|-------|------|-------------|
| video_id | string | YouTube video ID |
| title | string | Video title |
| url | string | Full YouTube URL |
| category | string | Content category (heartwarming/funny/traumatic) |
| search_query | string | Query used to find the video |
| duration_seconds | integer | Video length in seconds |
| view_count | integer | Number of views |
| like_count | integer | Number of likes |
| comment_count | integer | Number of comments |
| published_at | string | ISO timestamp of publication |
| channel_title | string | Channel name |
| tags | string | Comma-separated tags |
| collected_at | string | ISO timestamp of collection |

## n8n Workflow Examples

### Basic Processing Workflow
1. **Google Sheets Node**: Read new rows
2. **Filter Node**: Additional filtering if needed
3. **HTTP Request Node**: Fetch additional data
4. **Database Node**: Store processed data

### Advanced Workflow with Analysis
1. **Google Sheets Trigger**: On new row
2. **Code Node**: Calculate engagement rate
3. **IF Node**: Check thresholds
4. **Slack Node**: Notify if high-performing video
5. **Database Node**: Store for analysis

## Troubleshooting

### Common Issues:

1. **"Quota exceeded" error**
   - YouTube API has daily quotas
   - Reduce batch sizes or wait 24 hours

2. **"Permission denied" for Google Sheets**
   - Ensure service account email has edit access to sheet
   - Check if APIs are enabled in Google Cloud

3. **"No transcript available"**
   - Many videos don't have public transcripts
   - This is normal and videos are filtered out

4. **Rate limiting**
   - The app includes delays between API calls
   - Adjust sleep times if needed

## Performance Tips

1. **Optimize API Calls:**
   - Batch video ID checks when possible
   - Cache results to avoid duplicate API calls

2. **Google Sheets Limits:**
   - Sheets have a 10 million cell limit
   - Create new sheets periodically for large datasets

3. **n8n Processing:**
   - Process in batches to avoid memory issues
   - Use n8n's built-in error handling and retry logic

## Security Notes

- Never commit API keys to version control
- Use environment variables or secrets management
- Rotate service account keys periodically
- Limit API key permissions to only required APIs
- Use read-only access where possible

## Support & Updates

For issues or feature requests, consider:
1. Checking API quotas and limits
2. Verifying all credentials are correct
3. Ensuring all required APIs are enabled
4. Checking network connectivity

The app is designed to be resilient with built-in error handling and logging for debugging.