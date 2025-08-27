def load_existing_sheet_ids(self, spreadsheet_id: str) -> set:
        """Load existing video IDs from Google Sheet for duplicate checking"""
        try:
            if self.sheets_exporter:
                spreadsheet = self.sheets_exporter.get_spreadsheet_by_id(spreadsheet_id)
                worksheet = spreadsheet.worksheet("raw_links")
                
                # Get all values from the sheet
                all_values = worksheet.get_all_values()
                
                if len(all_values) > 1:  # Has header and data
                    # Find video_id column index
                    headers = all_values[0]
                    video_id_index = headers.index('video_id') if 'video_id' in headers else 0
                    
                    # Extract all video IDs (skip header row)
                    existing_ids = {row[video_id_index] for row in all_values[1:] if row[video_id_index]}
                    
                    self.add_log(f"Loaded {len(existing_ids)} existing video IDs from sheet", "INFO")
                    return existing_ids
                    
            return set()
            
        except Exception as e:
            self.add_log(f"Could not load existing sheet IDs: {str(e)}", "WARNING")
            return set()"""
YouTube Data Collector - Streamlit App
Collects filtered YouTube videos and exports to Google Sheets for n8n workflows
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import time
import random
from typing import Dict, List, Optional, Tuple
import re

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    st.error("Please install google-api-python-client: pip install google-api-python-client")
    st.stop()

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    st.error("Please install gspread and google-auth: pip install gspread google-auth")
    st.stop()

try:
    import isodate
except ImportError:
    st.error("Please install isodate: pip install isodate")
    st.stop()

# Page config
st.set_page_config(
    page_title="YouTube Data Collector",
    page_icon="🎬",
    layout="wide"
)

# Initialize session state
if 'collected_videos' not in st.session_state:
    st.session_state.collected_videos = []
if 'is_collecting' not in st.session_state:
    st.session_state.is_collecting = False
if 'stats' not in st.session_state:
    st.session_state.stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0}
if 'logs' not in st.session_state:
    st.session_state.logs = []
if 'used_queries' not in st.session_state:
    st.session_state.used_queries = set()

class YouTubeCollector:
    """Main collector class for YouTube videos"""
    
    def __init__(self, api_key: str, sheets_exporter=None):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.sheets_exporter = sheets_exporter  # For duplicate checking
        self.existing_sheet_ids = set()  # Cache for sheet video IDs
        self.existing_queries = set()  # Cache for used queries
        
        # Enhanced search queries - 30 per category, no years, shorter terms
        self.search_queries = {
            'heartwarming': [
                'soldier surprise homecoming',
                'dog reunion owner',
                'random acts kindness',
                'baby first time hearing',
                'proposal reaction emotional',
                'surprise gift reaction',
                'homeless man helped',
                'teacher surprised students',
                'reunion after years',
                'saving animal rescue',
                'kid helps stranger',
                'emotional wedding moment',
                'surprise visit family',
                'grateful reaction wholesome',
                'community helps neighbor',
                'dad meets baby',
                'emotional support moment',
                'stranger pays bill',
                'found lost pet',
                'surprise donation reaction',
                'elderly couple sweet',
                'child generous sharing',
                'unexpected hero saves',
                'touching tribute video',
                'surprise reunion compilation',
                'faith humanity restored',
                'emotional thank you',
                'surprise birthday elderly',
                'veteran honored ceremony',
                'wholesome interaction strangers'
            ],
            'funny': [
                'funny fails compilation',
                'unexpected moments caught',
                'comedy sketches viral',
                'hilarious reactions',
                'funny animals doing',
                'epic fail video',
                'instant karma funny',
                'comedy gold moments',
                'prank goes wrong',
                'funny kids saying',
                'dad jokes reaction',
                'wedding fails funny',
                'sports bloopers hilarious',
                'funny news bloopers',
                'pet fails compilation',
                'funny work moments',
                'hilarious misunderstanding',
                'comedy timing perfect',
                'funny voice over',
                'unexpected plot twist',
                'funny security camera',
                'hilarious interview moments',
                'comedy accident harmless',
                'funny dancing fails',
                'laughing contagious video',
                'funny sleep talking',
                'comedy scare pranks',
                'funny workout fails',
                'hilarious costume fails',
                'funny zoom fails'
            ],
            'traumatic': [
                'shocking moments caught',
                'dramatic rescue operation',
                'natural disaster footage',
                'intense police chase',
                'survival story real',
                'near death experience',
                'unbelievable close call',
                'extreme weather footage',
                'emergency response dramatic',
                'accident caught camera',
                'dangerous situation survived',
                'storm chaser footage',
                'rescue mission dramatic',
                'wildfire evacuation footage',
                'flood rescue dramatic',
                'earthquake footage real',
                'tornado close encounter',
                'avalanche survival story',
                'lightning strike caught',
                'road rage incident',
                'building collapse footage',
                'helicopter rescue dramatic',
                'cliff rescue operation',
                'shark encounter real',
                'volcano eruption footage',
                'mudslide caught camera',
                'train near miss',
                'bridge collapse footage',
                'explosion caught camera',
                'emergency landing footage'
            ]
        }
        
        # Exclusion keywords for filtering
        self.music_keywords = [
            'music video', 'official video', 'official music',
            'lyrics', 'lyric video', 'audio', 'soundtrack',
            'ost', 'mv', 'song', 'album', 'single release'
        ]
        
        self.compilation_keywords = [
            'best of', 'top 10', 'top 20',
            'montage', 'every time', 'all moments', 'mega compilation'
        ]
    
    def add_log(self, message: str, log_type: str = "INFO"):
        """Add a log entry"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        # Keep only last 50 logs
        st.session_state.logs = st.session_state.logs[:50]
    
    def search_videos(self, query: str, max_results: int = 50) -> List[Dict]:
        """Search for videos using YouTube API"""
        try:
            # Track API call
            st.session_state.stats['api_calls'] += 1
            
            # Calculate date 6 months ago
            six_months_ago = (datetime.now() - timedelta(days=180)).isoformat() + 'Z'
            
            request = self.youtube.search().list(
                part='id,snippet',
                q=query,
                type='video',
                maxResults=max_results,
                order='relevance',
                publishedAfter=six_months_ago,
                videoDuration='medium',  # > 4 minutes
                relevanceLanguage='en'
            )
            
            response = request.execute()
            return response.get('items', [])
            
        except HttpError as e:
            self.add_log(f"API Error during search: {str(e)}", "ERROR")
            return []
    
    def is_youtube_short(self, video_id: str, details: Dict) -> bool:
        """Check if video is a YouTube Short - URL pattern first, then duration"""
        try:
            # Method 1: Check URL pattern (most reliable if available)
            # Note: We don't have the actual URL in search results, but we can check indicators
            
            # Method 2: Check title and description for Shorts indicators
            title = details['snippet'].get('title', '').lower()
            description = details['snippet'].get('description', '').lower()
            
            # Common indicators in titles/descriptions
            shorts_indicators = ['#shorts', '#short', '#youtubeshorts', '#ytshorts']
            for indicator in shorts_indicators:
                if indicator in title or indicator in description:
                    return True
            
            # Method 3: Duration check as fallback (Shorts are max 60 seconds)
            duration = isodate.parse_duration(details['contentDetails']['duration'])
            duration_seconds = duration.total_seconds()
            
            # Only use duration as definitive indicator if ≤ 60 seconds
            if duration_seconds <= 60:
                return True
            
            return False
            
        except Exception as e:
            self.add_log(f"Error checking if video is Short: {str(e)}", "WARNING")
            return False
    
    def load_used_queries(self, spreadsheet_id: str) -> set:
        """Load previously used queries from Google Sheet"""
        try:
            if self.sheets_exporter:
                spreadsheet = self.sheets_exporter.get_spreadsheet_by_id(spreadsheet_id)
                
                # Try to get used_queries worksheet
                try:
                    worksheet = spreadsheet.worksheet("used_queries")
                    all_values = worksheet.get_all_values()
                    
                    if len(all_values) > 1:  # Has header and data
                        # Extract all queries (skip header row)
                        used_queries = {row[0] for row in all_values[1:] if row[0]}
                        self.add_log(f"Loaded {len(used_queries)} previously used queries", "INFO")
                        return used_queries
                except gspread.exceptions.WorksheetNotFound:
                    # Create the worksheet if it doesn't exist
                    worksheet = spreadsheet.add_worksheet(title="used_queries", rows=1000, cols=5)
                    worksheet.append_row(['query', 'category', 'timestamp', 'videos_found', 'session_id'])
                    self.add_log("Created new used_queries worksheet", "INFO")
                    
            return set()
            
        except Exception as e:
            self.add_log(f"Could not load used queries: {str(e)}", "WARNING")
            return set()
    
    def save_used_query(self, spreadsheet_id: str, query: str, category: str, videos_found: int):
        """Save used query to Google Sheet"""
        try:
            if self.sheets_exporter:
                spreadsheet = self.sheets_exporter.get_spreadsheet_by_id(spreadsheet_id)
                worksheet = spreadsheet.worksheet("used_queries")
                
                # Add query record
                worksheet.append_row([
                    query,
                    category,
                    datetime.now().isoformat(),
                    videos_found,
                    st.session_state.get('session_id', 'manual')
                ])
                
        except Exception as e:
            self.add_log(f"Could not save used query: {str(e)}", "WARNING")
    
    def check_caption_availability(self, details: Dict) -> bool:
        """Check if video has captions (auto-generated or manual)"""
        try:
            # Get caption value - could be boolean or string
            caption_value = details.get('contentDetails', {}).get('caption', False)
            
            # Handle both boolean and string responses
            if isinstance(caption_value, bool):
                return caption_value
            elif isinstance(caption_value, str):
                return caption_value.lower() == 'true'
            else:
                return False
        except:
            return False
    
    def get_video_details(self, video_id: str) -> Optional[Dict]:
        """Get detailed information about a video including caption availability"""
        try:
            # Track API call
            st.session_state.stats['api_calls'] += 1
            
            request = self.youtube.videos().list(
                part='snippet,contentDetails,statistics',  # Get all in one call
                id=video_id
            )
            response = request.execute()
            
            if response['items']:
                return response['items'][0]
            return None
            
        except HttpError as e:
            self.add_log(f"API Error getting video details: {str(e)}", "ERROR")
            return None
    
    def validate_video(self, search_item: Dict) -> Tuple[bool, str]:
        """Validate video against all criteria. Returns: (passed, reason_if_failed_or_details)"""
        video_id = search_item['id']['videoId']
        
        # Get ALL video details in one API call (including caption info)
        self.add_log(f"Checking video: {search_item['snippet']['title'][:50]}...")
        details = self.get_video_details(video_id)
        if not details:
            return False, "Could not fetch video details"
        
        # Check 1: Caption availability (replaces transcript check)
        if not self.check_caption_availability(details):
            return False, "No captions available"
        
        # Check 2: Age confirmation
        published_at = datetime.fromisoformat(details['snippet']['publishedAt'].replace('Z', '+00:00'))
        six_months_ago = datetime.now(published_at.tzinfo) - timedelta(days=180)
        if published_at < six_months_ago:
            return False, "Video older than 6 months"
        
        # Check 3: Check if it's a YouTube Short (replaces duration check)
        if self.is_youtube_short(video_id, details):
            return False, "YouTube Short detected"
        
        # Also check minimum duration for safety (at least 90 seconds for non-Shorts)
        duration = isodate.parse_duration(details['contentDetails']['duration'])
        duration_seconds = duration.total_seconds()
        if duration_seconds < 90:
            return False, f"Video too short ({duration_seconds}s < 90s)"
        
        # Check 4: Content type exclusion
        title = details['snippet']['title'].lower()
        tags = [tag.lower() for tag in details['snippet'].get('tags', [])]
        
        # Check for music video indicators
        for keyword in self.music_keywords:
            if keyword in title or any(keyword in tag for tag in tags):
                return False, f"Music video detected (keyword: {keyword})"
        
        # Check for compilation indicators  
        for keyword in self.compilation_keywords:
            if keyword in title or any(keyword in tag for tag in tags):
                return False, f"Compilation detected (keyword: {keyword})"
        
        # Check 5: View count
        view_count = int(details['statistics'].get('viewCount', 0))
        if view_count < 10000:
            return False, f"View count too low ({view_count} < 10,000)"
        
        # Check 6: Duplicate check (local + Google Sheet)
        existing_ids = [v['video_id'] for v in st.session_state.collected_videos]
        if video_id in existing_ids:
            return False, "Duplicate video (already collected)"
        
        # Also check against existing sheet IDs
        if video_id in self.existing_sheet_ids:
            return False, "Duplicate video (already in sheet)"
        
        # All checks passed - return details to avoid second API call
        return True, details
    
    def collect_videos(self, target_count: int, category: str, spreadsheet_id: str = None, progress_callback=None):
        """Main collection logic"""
        collected = []
        
        # Load existing video IDs and used queries from sheet if connected
        if spreadsheet_id and self.sheets_exporter:
            self.existing_sheet_ids = self.load_existing_sheet_ids(spreadsheet_id)
            self.existing_queries = self.load_used_queries(spreadsheet_id)
            st.session_state.used_queries.update(self.existing_queries)
        
        # Determine categories to use
        if category == 'mixed':
            categories = ['heartwarming', 'funny', 'traumatic']
        else:
            categories = [category]
        
        category_index = 0
        attempts = 0
        max_attempts = 30  # Increased for more query variety
        videos_checked_ids = set()  # Track checked videos to avoid rechecking
        
        while len(collected) < target_count and attempts < max_attempts:
            current_category = categories[category_index % len(categories)]
            
            # Get available queries and shuffle for variety
            available_queries = self.search_queries[current_category].copy()
            random.shuffle(available_queries)  # Randomize order
            
            # Find an unused query
            query = None
            for potential_query in available_queries:
                if potential_query not in st.session_state.used_queries:
                    query = potential_query
                    break
            
            # If all queries used, pick a random one
            if not query:
                query = random.choice(available_queries)
                self.add_log(f"All queries used for {current_category}, recycling: {query}", "INFO")
            
            # Mark query as used
            st.session_state.used_queries.add(query)
            
            self.add_log(f"Searching category '{current_category}': {query}", "INFO")
            
            # Search for videos
            search_results = self.search_videos(query, max_results=25)  # Reduced for efficiency
            
            if not search_results:
                self.add_log("No results found for query, trying another...", "WARNING")
                attempts += 1
                category_index += 1  # Try next category
                continue
            
            # Process each video
            videos_found_this_query = 0
            for item in search_results:
                if len(collected) >= target_count:
                    break
                
                video_id = item['id']['videoId']
                
                # Skip if already checked
                if video_id in videos_checked_ids:
                    continue
                    
                videos_checked_ids.add(video_id)
                st.session_state.stats['checked'] += 1
                
                # Validate video
                result = self.validate_video(item)
                
                if result[0]:  # Passed validation
                    # We already have details from validation
                    details = result[1]
                    
                    # Create video record
                    video_record = {
                        'video_id': video_id,
                        'title': details['snippet']['title'],
                        'url': f"https://youtube.com/watch?v={video_id}",
                        'category': current_category,
                        'search_query': query,
                        'duration_seconds': int(isodate.parse_duration(
                            details['contentDetails']['duration']
                        ).total_seconds()),
                        'view_count': int(details['statistics'].get('viewCount', 0)),
                        'like_count': int(details['statistics'].get('likeCount', 0)),
                        'comment_count': int(details['statistics'].get('commentCount', 0)),
                        'published_at': details['snippet']['publishedAt'],
                        'channel_title': details['snippet']['channelTitle'],
                        'tags': ','.join(details['snippet'].get('tags', [])),
                        'collected_at': datetime.now().isoformat()
                    }
                    
                    collected.append(video_record)
                    st.session_state.collected_videos.append(video_record)
                    st.session_state.stats['found'] += 1
                    videos_found_this_query += 1
                    
                    self.add_log(f"✓ Added: {video_record['title'][:50]}...", "SUCCESS")
                    
                    if progress_callback:
                        progress_callback(len(collected), target_count)
                else:
                    # Failed validation
                    reason = result[1]
                    st.session_state.stats['rejected'] += 1
                    self.add_log(f"✗ Rejected: {item['snippet']['title'][:50]}... - {reason}", "WARNING")
                
                # Small delay to avoid rate limiting
                time.sleep(0.3)
            
            # Save used query to sheet
            if spreadsheet_id and self.sheets_exporter:
                self.save_used_query(spreadsheet_id, query, current_category, videos_found_this_query)
            
            # If no videos found with this query, try next category quickly
            if videos_found_this_query == 0:
                self.add_log(f"No valid videos found with this query, switching category...", "INFO")
                category_index += 1
            else:
                # Stay with successful category for a bit
                if videos_found_this_query >= 2:
                    category_index += 1  # Only switch after finding some videos
            
            attempts += 1
            
            # Delay between searches
            time.sleep(1.5)
        
        if len(collected) > 0:
            self.add_log(f"Collection complete! Found {len(collected)} videos.", "SUCCESS")
        else:
            self.add_log(f"No valid videos found after {attempts} attempts. Try different settings.", "WARNING")
        
        return collected

class GoogleSheetsExporter:
    """Handle Google Sheets export"""
    
    def __init__(self, credentials_dict: Dict):
        """Initialize with service account credentials"""
        self.creds = Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets',
                   'https://www.googleapis.com/auth/drive']
        )
        self.client = gspread.authorize(self.creds)
    
    def get_spreadsheet_by_id(self, spreadsheet_id: str):
        """Get spreadsheet by ID"""
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            st.success(f"✅ Connected to existing spreadsheet")
            return spreadsheet
        except Exception as e:
            st.error(f"Could not access spreadsheet: {str(e)}")
            raise e
    
    def create_or_get_spreadsheet(self, spreadsheet_name: str):
        """Create a new spreadsheet or get existing one"""
        try:
            # Try to open existing spreadsheet
            spreadsheet = self.client.open(spreadsheet_name)
            st.success(f"✅ Found existing spreadsheet: {spreadsheet_name}")
        except gspread.exceptions.SpreadsheetNotFound:
            # Create new spreadsheet
            spreadsheet = self.client.create(spreadsheet_name)
            st.success(f"✅ Created new spreadsheet: {spreadsheet_name}")
            st.warning(f"⚠️ IMPORTANT: Share this spreadsheet with your main Google account to view it!")
        
        return spreadsheet
    
    def export_to_sheets(self, videos: List[Dict], spreadsheet_id: str = None, spreadsheet_name: str = "YouTube_Collection_Data"):
        """Export videos to Google Sheets"""
        try:
            # Use existing spreadsheet by ID or create new one
            if spreadsheet_id:
                spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            else:
                spreadsheet = self.create_or_get_spreadsheet(spreadsheet_name)
            
            # Always use "raw_links" as worksheet name
            worksheet_name = "raw_links"
            
            # Get or create worksheet
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                st.info(f"Using existing worksheet: {worksheet_name}")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
                st.success(f"Created new worksheet: {worksheet_name}")
            
            # Prepare data for export
            if videos:
                df = pd.DataFrame(videos)
                
                # Get existing data count for appending
                existing_data = worksheet.get_all_values()
                if existing_data and len(existing_data) > 1:  # Has headers and data
                    st.info(f"Found {len(existing_data)-1} existing rows, appending new data...")
                    # Don't clear, just append
                    for _, row in df.iterrows():
                        values = [str(v) if pd.notna(v) else '' for v in row.tolist()]
                        worksheet.append_row(values)
                else:
                    # First time - add headers and data
                    worksheet.clear()
                    headers = list(df.columns)
                    worksheet.append_row(headers)
                    for _, row in df.iterrows():
                        values = [str(v) if pd.notna(v) else '' for v in row.tolist()]
                        worksheet.append_row(values)
                
                return spreadsheet.url
            
            return None
        except Exception as e:
            st.error(f"Error exporting to sheets: {str(e)}")
            raise e

def main():
    st.title("🎬 YouTube Data Collector")
    st.markdown("Collects filtered YouTube videos and exports to Google Sheets for n8n workflows")
    
    # Info box about service account
    with st.expander("ℹ️ Setup Instructions", expanded=False):
        st.markdown("""
        ### Required Setup:
        1. **YouTube API Key**: Get from Google Cloud Console → APIs & Services → Credentials
        2. **Service Account JSON**: Get from Google Cloud Console → IAM & Admin → Service Accounts
        3. **Important**: Share the Google Sheet with: `ytlink@testauto-470014.iam.gserviceaccount.com`
        
        ### Your Service Account Email:
        ```
        ytlink@testauto-470014.iam.gserviceaccount.com
        ```
        """)
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # API Keys
        st.subheader("1. YouTube API")
        youtube_api_key = st.text_input(
            "YouTube API Key", 
            type="password", 
            help="Your YouTube Data API v3 key from Google Cloud Console"
        )
        
        # Google Sheets credentials
        st.subheader("2. Google Sheets Setup")
        
        # Option to paste JSON or upload file
        creds_input_method = st.radio(
            "How to provide Service Account JSON?",
            ["Paste JSON", "Upload JSON file"]
        )
        
        sheets_creds = None
        if creds_input_method == "Paste JSON":
            sheets_creds_text = st.text_area(
                "Service Account JSON", 
                type="password",
                help="Paste your complete Google service account JSON",
                height=150,
                placeholder='{\n  "type": "service_account",\n  "project_id": "...",\n  ...\n}'
            )
            if sheets_creds_text:
                try:
                    sheets_creds = json.loads(sheets_creds_text)
                    st.success("✅ Valid JSON")
                except json.JSONDecodeError as e:
                    st.error(f"Invalid JSON: {str(e)}")
        else:
            uploaded_file = st.file_uploader(
                "Upload Service Account JSON",
                type=['json'],
                help="Upload your service account JSON file"
            )
            if uploaded_file:
                try:
                    sheets_creds = json.load(uploaded_file)
                    st.success("✅ JSON file loaded")
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
        
        # Options for existing sheet or new sheet
        use_existing = st.checkbox(
            "Use existing Google Sheet",
            value=True,
            help="Check if you have an existing sheet to use"
        )
        
        if use_existing:
            spreadsheet_url = st.text_input(
                "Google Sheet URL",
                value="https://docs.google.com/spreadsheets/d/1PHvW-LykIpIbwKJbiGHi6NcX7hd4EsIWK3zwr4Dmvrk/",
                help="Paste the URL of your existing Google Sheet"
            )
            # Extract spreadsheet ID from URL
            import re
            match = re.search(r'/d/([a-zA-Z0-9-_]+)', spreadsheet_url)
            spreadsheet_id = match.group(1) if match else None
            if spreadsheet_id:
                st.success(f"✅ Sheet ID: {spreadsheet_id[:20]}...")
        else:
            spreadsheet_name = st.text_input(
                "New Spreadsheet Name",
                value="YouTube_Collection_Data",
                help="Name for the new Google Sheet to create"
            )
        
        # Show service account email if credentials are loaded
        if sheets_creds and 'client_email' in sheets_creds:
            st.info(f"📧 Service Account: {sheets_creds['client_email']}")
        
        # Collection settings
        st.subheader("3. Collection Settings")
        category = st.selectbox(
            "Content Category",
            options=['heartwarming', 'funny', 'traumatic', 'mixed'],
            help="Select content type or 'mixed' to rotate"
        )
        
        target_count = st.number_input(
            "Target Video Count",
            min_value=1,
            max_value=500,
            value=10,
            help="Number of videos to collect"
        )
        
        auto_export = st.checkbox(
            "Auto-export to Google Sheets",
            value=True,
            help="Automatically export after collection"
        )
    
    # Main content area
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Videos Found", st.session_state.stats['found'])
    with col2:
        st.metric("Videos Checked", st.session_state.stats['checked'])
    with col3:
        st.metric("Videos Rejected", st.session_state.stats['rejected'])
    with col4:
        # Calculate API quota usage (approximate)
        api_calls = st.session_state.stats.get('api_calls', 0)
        # search.list costs 100 units, videos.list costs 1 unit
        estimated_units = api_calls * 50  # Average estimate
        st.metric("API Quota Used", f"{api_calls} calls", 
                 delta=f"~{estimated_units}/10,000 units",
                 delta_color="normal" if estimated_units < 8000 else "inverse")
    
    # Control buttons
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🚀 Start Collection", disabled=st.session_state.is_collecting, type="primary"):
            if not youtube_api_key:
                st.error("❌ Please enter your YouTube API key")
            else:
                st.session_state.is_collecting = True
                st.session_state.stats = {'checked': 0, 'found': 0, 'rejected': 0}
                st.session_state.logs = []
                
                try:
                    # Create collector with sheets exporter for duplicate checking
                    exporter = None
                    if sheets_creds:
                        try:
                            exporter = GoogleSheetsExporter(sheets_creds)
                        except Exception as e:
                            st.warning(f"Could not initialize sheets exporter: {str(e)}")
                    
                    collector = YouTubeCollector(youtube_api_key, sheets_exporter=exporter)
                    
                    # Progress bar
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    def update_progress(current, total):
                        progress = current / total
                        progress_bar.progress(progress)
                        status_text.text(f"Collecting: {current}/{total} videos")
                    
                    # Get spreadsheet ID if using existing sheet
                    sheet_id = None
                    if use_existing and spreadsheet_id:
                        sheet_id = spreadsheet_id
                    
                    # Run collection
                    with st.spinner(f"Collecting {target_count} videos..."):
                        videos = collector.collect_videos(
                            target_count=target_count,
                            category=category,
                            spreadsheet_id=sheet_id,
                            progress_callback=update_progress
                        )
                    
                    st.success(f"✅ Collection complete! Found {len(videos)} videos.")
                    
                    # Auto-export if enabled
                    if auto_export and sheets_creds and videos:
                        try:
                            exporter = GoogleSheetsExporter(sheets_creds)
                            # Check if using existing sheet
                            if 'use_existing' in locals() and use_existing and 'spreadsheet_id' in locals() and spreadsheet_id:
                                sheet_url = exporter.export_to_sheets(videos, spreadsheet_id=spreadsheet_id)
                            else:
                                sheet_url = exporter.export_to_sheets(videos, spreadsheet_name=spreadsheet_name if 'spreadsheet_name' in locals() else "YouTube_Collection_Data")
                            if sheet_url:
                                st.success(f"✅ Exported to Google Sheets!")
                                st.markdown(f"📊 [Open Spreadsheet]({sheet_url})")
                                collector.add_log(f"Exported to Google Sheets: {sheet_url}", "SUCCESS")
                        except Exception as e:
                            st.error(f"❌ Export failed: {str(e)}")
                            st.error("Make sure you've shared the sheet with the service account email!")
                            collector.add_log(f"Export error: {str(e)}", "ERROR")
                
                except Exception as e:
                    st.error(f"❌ Collection error: {str(e)}")
                    if "API key not valid" in str(e):
                        st.error("Your YouTube API key is invalid. Please check it in Google Cloud Console.")
                    elif "quota" in str(e).lower():
                        st.error("YouTube API quota exceeded. Wait 24 hours or use a different API key.")
                finally:
                    st.session_state.is_collecting = False
                    st.rerun()
    
    with col2:
        if st.button("🛑 Stop", disabled=not st.session_state.is_collecting):
            st.session_state.is_collecting = False
            st.rerun()
    
    with col3:
        if st.button("🔄 Reset"):
            st.session_state.collected_videos = []
            st.session_state.stats = {'checked': 0, 'found': 0, 'rejected': 0}
            st.session_state.logs = []
            st.rerun()
    
    with col4:
        if st.button("📤 Manual Export") and st.session_state.collected_videos:
            if not sheets_creds:
                st.error("❌ Please add Google Sheets credentials")
            else:
                try:
                    exporter = GoogleSheetsExporter(sheets_creds)
                    # Check if using existing sheet
                    if use_existing and spreadsheet_id:
                        sheet_url = exporter.export_to_sheets(
                            st.session_state.collected_videos, 
                            spreadsheet_id=spreadsheet_id
                        )
                    else:
                        sheet_url = exporter.export_to_sheets(
                            st.session_state.collected_videos, 
                            spreadsheet_name=spreadsheet_name if not use_existing else "YouTube_Collection_Data"
                        )
                    if sheet_url:
                        st.success(f"✅ Exported to Google Sheets!")
                        st.markdown(f"📊 [Open Spreadsheet]({sheet_url})")
                except Exception as e:
                    st.error(f"❌ Export failed: {str(e)}")
                    st.error("Tip: Make sure the sheet is shared with your service account email!")
    
    # Display collected videos
    if st.session_state.collected_videos:
        st.subheader("📊 Collected Videos")
        df = pd.DataFrame(st.session_state.collected_videos)
        
        # Display summary
        st.dataframe(
            df[['title', 'category', 'view_count', 'duration_seconds', 'url']],
            use_container_width=True,
            hide_index=True
        )
        
        # Download options
        col1, col2 = st.columns(2)
        with col1:
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"youtube_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            json_str = json.dumps(st.session_state.collected_videos, indent=2)
            st.download_button(
                label="📥 Download JSON",
                data=json_str,
                file_name=f"youtube_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
    
    # Activity log
    with st.expander("📜 Activity Log", expanded=False):
        if st.session_state.logs:
            for log in st.session_state.logs:
                if "SUCCESS" in log:
                    st.success(log)
                elif "ERROR" in log:
                    st.error(log)
                elif "WARNING" in log:
                    st.warning(log)
                else:
                    st.info(log)
        else:
            st.info("No activity yet")

if __name__ == "__main__":
    main()
