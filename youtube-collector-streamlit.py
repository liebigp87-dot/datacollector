"""
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
import uuid

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
    st.session_state.stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0, 'has_captions': 0, 'no_captions': 0}
if 'logs' not in st.session_state:
    st.session_state.logs = []
if 'used_queries' not in st.session_state:
    st.session_state.used_queries = set()

class YouTubeCollector:
    """Main collector class for YouTube videos"""
    
    def __init__(self, api_key: str, sheets_exporter=None):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.sheets_exporter = sheets_exporter
        self.existing_sheet_ids = set()
        self.existing_queries = set()
        
        # Enhanced search queries - 30 per category, no years
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
        
        # Exclusion keywords
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
        st.session_state.logs = st.session_state.logs[:50]
    
    def check_quota_available(self) -> Tuple[bool, str]:
        """Check if YouTube API quota is available"""
        try:
            self.add_log("Checking API quota availability...", "INFO")
            test_request = self.youtube.videos().list(
                part='id',
                id='YbJOTdZBX1g'
            )
            response = test_request.execute()
            st.session_state.stats['api_calls'] += 1
            self.add_log("✅ API quota check passed", "SUCCESS")
            return True, "Quota available"
        except HttpError as e:
            error_str = str(e)
            if 'quotaExceeded' in error_str:
                self.add_log("❌ YouTube API quota exceeded", "ERROR")
                return False, "Daily quota exceeded. Wait 24 hours or use different API key."
            elif 'forbidden' in error_str.lower():
                self.add_log("❌ API access forbidden", "ERROR")
                return False, "API key invalid or YouTube Data API not enabled"
            else:
                self.add_log(f"⚠️ API error: {error_str[:100]}", "WARNING")
                return True, "Warning: API error but attempting to continue"
        except Exception as e:
            self.add_log(f"⚠️ Non-API error: {str(e)[:100]}", "WARNING")
            return True, "Could not verify quota, proceeding anyway"
    
    def search_videos(self, query: str, max_results: int = 25) -> List[Dict]:
        """Search for videos using YouTube API"""
        try:
            st.session_state.stats['api_calls'] += 1
            six_months_ago = (datetime.now() - timedelta(days=180)).isoformat() + 'Z'
            
            request = self.youtube.search().list(
                part='id,snippet',
                q=query,
                type='video',
                maxResults=max_results,
                order='relevance',
                publishedAfter=six_months_ago,
                videoDuration='medium',
                relevanceLanguage='en'
            )
            
            response = request.execute()
            return response.get('items', [])
        except HttpError as e:
            self.add_log(f"API Error during search: {str(e)}", "ERROR")
            return []
    
    def get_video_details(self, video_id: str) -> Optional[Dict]:
        """Get detailed information about a video"""
        try:
            st.session_state.stats['api_calls'] += 1
            request = self.youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=video_id
            )
            response = request.execute()
            
            if response['items']:
                return response['items'][0]
            return None
        except HttpError as e:
            self.add_log(f"API Error getting video details: {str(e)}", "ERROR")
            return None
    
    def is_youtube_short(self, video_id: str, details: Dict) -> bool:
        """Check if video is a YouTube Short"""
        try:
            title = details['snippet'].get('title', '').lower()
            description = details['snippet'].get('description', '').lower()
            
            shorts_indicators = ['#shorts', '#short', '#youtubeshorts', '#ytshorts']
            for indicator in shorts_indicators:
                if indicator in title or indicator in description:
                    return True
            
            duration = isodate.parse_duration(details['contentDetails']['duration'])
            duration_seconds = duration.total_seconds()
            
            if duration_seconds <= 60:
                return True
            
            return False
        except Exception as e:
            self.add_log(f"Error checking if video is Short: {str(e)}", "WARNING")
            return False
    
    def check_caption_availability(self, details: Dict) -> bool:
        """Check if video has captions (auto-generated or manual)"""
        try:
            caption_value = details.get('contentDetails', {}).get('caption', False)
            
            has_caption = False
            if isinstance(caption_value, bool):
                has_caption = caption_value
            elif isinstance(caption_value, str):
                has_caption = caption_value.lower() == 'true'
            
            # Track caption statistics
            if has_caption:
                st.session_state.stats['has_captions'] += 1
                self.add_log(f"✓ Video has captions (auto-generated or manual)", "INFO")
            else:
                st.session_state.stats['no_captions'] += 1
                self.add_log(f"✗ Video has NO captions available", "INFO")
            
            # Calculate and log percentage
            total_caption_checks = st.session_state.stats['has_captions'] + st.session_state.stats['no_captions']
            if total_caption_checks > 0:
                caption_percentage = (st.session_state.stats['has_captions'] / total_caption_checks) * 100
                self.add_log(f"📊 Caption availability: {caption_percentage:.1f}% ({st.session_state.stats['has_captions']}/{total_caption_checks})", "INFO")
            
            return has_caption
        except Exception as e:
            self.add_log(f"Error checking captions: {str(e)}", "WARNING")
            return False
    
    def load_existing_sheet_ids(self, spreadsheet_id: str) -> set:
        """Load existing video IDs from Google Sheet"""
        try:
            if self.sheets_exporter:
                spreadsheet = self.sheets_exporter.get_spreadsheet_by_id(spreadsheet_id)
                worksheet = spreadsheet.worksheet("raw_links")
                all_values = worksheet.get_all_values()
                
                if len(all_values) > 1:
                    headers = all_values[0]
                    video_id_index = headers.index('video_id') if 'video_id' in headers else 0
                    existing_ids = {row[video_id_index] for row in all_values[1:] if row[video_id_index]}
                    self.add_log(f"Loaded {len(existing_ids)} existing video IDs from sheet", "INFO")
                    return existing_ids
            return set()
        except Exception as e:
            self.add_log(f"Could not load existing sheet IDs: {str(e)}", "WARNING")
            return set()
    
    def load_used_queries(self, spreadsheet_id: str) -> set:
        """Load previously used queries from Google Sheet"""
        try:
            if self.sheets_exporter:
                spreadsheet = self.sheets_exporter.get_spreadsheet_by_id(spreadsheet_id)
                try:
                    worksheet = spreadsheet.worksheet("used_queries")
                    all_values = worksheet.get_all_values()
                    
                    if len(all_values) > 1:
                        used_queries = {row[0] for row in all_values[1:] if row[0]}
                        self.add_log(f"Loaded {len(used_queries)} previously used queries", "INFO")
                        return used_queries
                except gspread.exceptions.WorksheetNotFound:
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
                worksheet.append_row([
                    query,
                    category,
                    datetime.now().isoformat(),
                    videos_found,
                    st.session_state.get('session_id', 'manual')
                ])
        except Exception as e:
            self.add_log(f"Could not save used query: {str(e)}", "WARNING")
    
    def validate_video(self, search_item: Dict, require_captions: bool = True) -> Tuple[bool, str]:
        """Validate video against all criteria"""
        video_id = search_item['id']['videoId']
        
        self.add_log(f"Checking video: {search_item['snippet']['title'][:50]}...")
        details = self.get_video_details(video_id)
        if not details:
            return False, "Could not fetch video details"
        
        # Check 1: Caption availability (optional based on setting)
        if require_captions:
            if not self.check_caption_availability(details):
                return False, "No captions available"
        else:
            # Still check captions for statistics, but don't reject
            self.check_caption_availability(details)
            self.add_log("Caption check disabled - accepting video regardless of captions", "INFO")
        
        # Check 2: Age confirmation
        published_at = datetime.fromisoformat(details['snippet']['publishedAt'].replace('Z', '+00:00'))
        six_months_ago = datetime.now(published_at.tzinfo) - timedelta(days=180)
        if published_at < six_months_ago:
            return False, "Video older than 6 months"
        
        # Check 3: YouTube Short detection
        if self.is_youtube_short(video_id, details):
            return False, "YouTube Short detected"
        
        duration = isodate.parse_duration(details['contentDetails']['duration'])
        duration_seconds = duration.total_seconds()
        if duration_seconds < 90:
            return False, f"Video too short ({duration_seconds}s < 90s)"
        
        # Check 4: Content type exclusion
        title = details['snippet']['title'].lower()
        tags = [tag.lower() for tag in details['snippet'].get('tags', [])]
        
        for keyword in self.music_keywords:
            if keyword in title or any(keyword in tag for tag in tags):
                return False, f"Music video detected (keyword: {keyword})"
        
        for keyword in self.compilation_keywords:
            if keyword in title or any(keyword in tag for tag in tags):
                return False, f"Compilation detected (keyword: {keyword})"
        
        # Check 5: View count
        view_count = int(details['statistics'].get('viewCount', 0))
        if view_count < 10000:
            return False, f"View count too low ({view_count} < 10,000)"
        
        # Check 6: Duplicate check
        existing_ids = [v['video_id'] for v in st.session_state.collected_videos]
        if video_id in existing_ids:
            return False, "Duplicate video (already collected)"
        
        if video_id in self.existing_sheet_ids:
            return False, "Duplicate video (already in sheet)"
        
        return True, details
    
    def collect_videos(self, target_count: int, category: str, spreadsheet_id: str = None, require_captions: bool = True, progress_callback=None):
        """Main collection logic"""
        collected = []
        
        self.add_log(f"Caption requirement: {'ENABLED - Only videos with captions' if require_captions else 'DISABLED - All videos accepted'}", "INFO")
        
        # Load existing data from sheet
        if spreadsheet_id and self.sheets_exporter:
            self.existing_sheet_ids = self.load_existing_sheet_ids(spreadsheet_id)
            self.existing_queries = self.load_used_queries(spreadsheet_id)
            st.session_state.used_queries.update(self.existing_queries)
        
        # Determine categories
        if category == 'mixed':
            categories = ['heartwarming', 'funny', 'traumatic']
        else:
            categories = [category]
        
        self.add_log(f"Starting collection for category: {category}, target: {target_count} videos", "INFO")
        
        category_index = 0
        attempts = 0
        max_attempts = 30
        videos_checked_ids = set()
        
        while len(collected) < target_count and attempts < max_attempts:
            current_category = categories[category_index % len(categories)]
            
            # Get available queries and shuffle
            available_queries = self.search_queries[current_category].copy()
            random.shuffle(available_queries)
            
            # Find unused query
            query = None
            for potential_query in available_queries:
                if potential_query not in st.session_state.used_queries:
                    query = potential_query
                    break
            
            if not query:
                query = random.choice(available_queries)
                self.add_log(f"All queries used for {current_category}, recycling: {query}", "INFO")
            
            st.session_state.used_queries.add(query)
            self.add_log(f"Searching category '{current_category}': {query}", "INFO")
            
            # Search for videos
            search_results = self.search_videos(query)
            
            if not search_results:
                self.add_log("No results found for query, trying another...", "WARNING")
                attempts += 1
                category_index += 1
                continue
            
            # Process each video
            videos_found_this_query = 0
            for item in search_results:
                if len(collected) >= target_count:
                    break
                
                video_id = item['id']['videoId']
                
                if video_id in videos_checked_ids:
                    continue
                
                videos_checked_ids.add(video_id)
                st.session_state.stats['checked'] += 1
                
                # Validate video
                result = self.validate_video(item, require_captions)
                
                if result[0]:
                    details = result[1]
                    
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
                    reason = result[1]
                    st.session_state.stats['rejected'] += 1
                    self.add_log(f"✗ Rejected: {item['snippet']['title'][:50]}... - {reason}", "WARNING")
                
                time.sleep(0.3)
            
            # Save used query
            if spreadsheet_id and self.sheets_exporter:
                self.save_used_query(spreadsheet_id, query, current_category, videos_found_this_query)
            
            if videos_found_this_query == 0:
                self.add_log(f"No valid videos found with this query, switching category...", "INFO")
                category_index += 1
            else:
                if videos_found_this_query >= 2:
                    category_index += 1
            
            attempts += 1
            time.sleep(1.5)
        
        if len(collected) > 0:
            self.add_log(f"Collection complete! Found {len(collected)} videos.", "SUCCESS")
        else:
            self.add_log(f"No valid videos found after {attempts} attempts.", "WARNING")
        
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
            spreadsheet = self.client.open(spreadsheet_name)
            st.success(f"✅ Found existing spreadsheet: {spreadsheet_name}")
        except gspread.exceptions.SpreadsheetNotFound:
            spreadsheet = self.client.create(spreadsheet_name)
            st.success(f"✅ Created new spreadsheet: {spreadsheet_name}")
            st.warning(f"⚠️ IMPORTANT: Share this spreadsheet with your main Google account!")
        
        return spreadsheet
    
    def export_to_sheets(self, videos: List[Dict], spreadsheet_id: str = None, spreadsheet_name: str = "YouTube_Collection_Data"):
        """Export videos to Google Sheets"""
        try:
            if spreadsheet_id:
                spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            else:
                spreadsheet = self.create_or_get_spreadsheet(spreadsheet_name)
            
            worksheet_name = "raw_links"
            
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                st.info(f"Using existing worksheet: {worksheet_name}")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
                st.success(f"Created new worksheet: {worksheet_name}")
            
            if videos:
                df = pd.DataFrame(videos)
                existing_data = worksheet.get_all_values()
                
                if existing_data and len(existing_data) > 1:
                    st.info(f"Found {len(existing_data)-1} existing rows, appending new data...")
                    for _, row in df.iterrows():
                        values = [str(v) if pd.notna(v) else '' for v in row.tolist()]
                        worksheet.append_row(values)
                else:
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
    
    # Info box
    with st.expander("ℹ️ Setup Instructions", expanded=False):
        st.markdown("""
        ### Required Setup:
        1. **YouTube API Key**: Get from Google Cloud Console
        2. **Service Account JSON**: Get from Google Cloud Console
        3. **Share Sheet with**: `ytlink@testauto-470014.iam.gserviceaccount.com`
        """)
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        st.subheader("1. YouTube API")
        youtube_api_key = st.text_input(
            "YouTube API Key", 
            type="password", 
            help="Your YouTube Data API v3 key"
        )
        
        st.subheader("2. Google Sheets Setup")
        creds_input_method = st.radio(
            "How to provide Service Account JSON?",
            ["Paste JSON", "Upload JSON file"]
        )
        
        sheets_creds = None
        if creds_input_method == "Paste JSON":
            sheets_creds_text = st.text_area(
                "Service Account JSON", 
                help="Paste your complete Google service account JSON",
                height=150
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
                type=['json']
            )
            if uploaded_file:
                try:
                    sheets_creds = json.load(uploaded_file)
                    st.success("✅ JSON file loaded")
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
        
        use_existing = st.checkbox(
            "Use existing Google Sheet",
            value=True
        )
        
        spreadsheet_id = None
        spreadsheet_name = "YouTube_Collection_Data"
        
        if use_existing:
            spreadsheet_url = st.text_input(
                "Google Sheet URL",
                value="https://docs.google.com/spreadsheets/d/1PHvW-LykIpIbwKJbiGHi6NcX7hd4EsIWK3zwr4Dmvrk/"
            )
            match = re.search(r'/d/([a-zA-Z0-9-_]+)', spreadsheet_url)
            spreadsheet_id = match.group(1) if match else None
            if spreadsheet_id:
                st.success(f"✅ Sheet ID: {spreadsheet_id[:20]}...")
        else:
            spreadsheet_name = st.text_input(
                "New Spreadsheet Name",
                value="YouTube_Collection_Data"
            )
        
        if sheets_creds and 'client_email' in sheets_creds:
            st.info(f"📧 Service Account: {sheets_creds['client_email']}")
        
        st.subheader("3. Collection Settings")
        category = st.selectbox(
            "Content Category",
            options=['heartwarming', 'funny', 'traumatic', 'mixed']
        )
        
        target_count = st.number_input(
            "Target Video Count",
            min_value=1,
            max_value=500,
            value=10
        )
        
        auto_export = st.checkbox(
            "Auto-export to Google Sheets",
            value=True
        )
        
        skip_quota_check = st.checkbox(
            "Skip quota check",
            value=False,
            help="Skip the API quota pre-flight check"
        )
        
        require_captions = st.checkbox(
            "Require captions",
            value=True,
            help="Only collect videos with captions (auto-generated or manual). Uncheck to accept all videos."
        )
        
        st.info(f"💡 Caption Info: When enabled, accepts videos with ANY type of captions including YouTube's auto-generated captions, which work perfectly for subtitle generation.")
    
    # Main content area - Added caption statistics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Videos Found", st.session_state.stats['found'])
    with col2:
        st.metric("Videos Checked", st.session_state.stats['checked'])
    with col3:
        st.metric("Videos Rejected", st.session_state.stats['rejected'])
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_caption_checks = st.session_state.stats.get('has_captions', 0) + st.session_state.stats.get('no_captions', 0)
        caption_rate = (st.session_state.stats.get('has_captions', 0) / total_caption_checks * 100) if total_caption_checks > 0 else 0
        st.metric("Videos with Captions", 
                 f"{st.session_state.stats.get('has_captions', 0)}/{total_caption_checks}",
                 delta=f"{caption_rate:.1f}%")
    with col2:
        api_calls = st.session_state.stats.get('api_calls', 0)
        estimated_units = api_calls * 50
        st.metric("API Quota Used", f"{api_calls} calls", 
                 delta=f"~{estimated_units}/10,000 units",
                 delta_color="normal" if estimated_units < 8000 else "inverse")
    with col3:
        st.metric("Session ID", st.session_state.get('session_id', 'Not started'))
    
    # Control buttons
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🚀 Start Collection", disabled=st.session_state.is_collecting, type="primary"):
            if not youtube_api_key:
                st.error("❌ Please enter your YouTube API key")
            else:
                st.session_state.is_collecting = True
                st.session_state.stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0, 'has_captions': 0, 'no_captions': 0}
                st.session_state.logs = []
                st.session_state['session_id'] = str(uuid.uuid4())[:8]
                
                try:
                    exporter = None
                    if sheets_creds:
                        try:
                            exporter = GoogleSheetsExporter(sheets_creds)
                        except Exception as e:
                            st.warning(f"Could not initialize sheets exporter: {str(e)}")
                    
                    collector = YouTubeCollector(youtube_api_key, sheets_exporter=exporter)
                    
                    quota_available = True
                    if not skip_quota_check:
                        quota_available, quota_message = collector.check_quota_available()
                        if not quota_available:
                            st.error(f"❌ Cannot start collection: {quota_message}")
                            st.session_state.is_collecting = False
                        else:
                            st.success(f"✅ {quota_message}")
                    else:
                        st.warning("⚠️ Skipping quota check")
                        collector.add_log("Quota check skipped by user", "INFO")
                    
                    if quota_available:
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        def update_progress(current, total):
                            progress = current / total
                            progress_bar.progress(progress)
                            status_text.text(f"Collecting: {current}/{total} videos")
                        
                        sheet_id = None
                        if use_existing and spreadsheet_id:
                            sheet_id = spreadsheet_id
                        
                        with st.spinner(f"Collecting {target_count} videos..."):
                            videos = collector.collect_videos(
                                target_count=target_count,
                                category=category,
                                spreadsheet_id=sheet_id,
                                require_captions=require_captions,
                                progress_callback=update_progress
                            )
                        
                        st.success(f"✅ Collection complete! Found {len(videos)} videos.")
                        
                        if auto_export and sheets_creds and videos:
                            try:
                                if not exporter:
                                    exporter = GoogleSheetsExporter(sheets_creds)
                                if use_existing and spreadsheet_id:
                                    sheet_url = exporter.export_to_sheets(videos, spreadsheet_id=spreadsheet_id)
                                else:
                                    sheet_url = exporter.export_to_sheets(videos, spreadsheet_name=spreadsheet_name)
                                if sheet_url:
                                    st.success(f"✅ Exported to Google Sheets!")
                                    st.markdown(f"📊 [Open Spreadsheet]({sheet_url})")
                                    collector.add_log(f"Exported to Google Sheets: {sheet_url}", "SUCCESS")
                            except Exception as e:
                                st.error(f"❌ Export failed: {str(e)}")
                                collector.add_log(f"Export error: {str(e)}", "ERROR")
                
                except Exception as e:
                    st.error(f"❌ Collection error: {str(e)}")
                    if "API key not valid" in str(e):
                        st.error("Your YouTube API key is invalid.")
                    elif "quota" in str(e).lower():
                        st.error("YouTube API quota exceeded.")
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
            st.session_state.stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0, 'has_captions': 0, 'no_captions': 0}
            st.session_state.logs = []
            st.session_state.used_queries = set()
            st.rerun()
    
    with col4:
        if st.button("📤 Manual Export") and st.session_state.collected_videos:
            if not sheets_creds:
                st.error("❌ Please add Google Sheets credentials")
            else:
                try:
                    exporter = GoogleSheetsExporter(sheets_creds)
                    if use_existing and spreadsheet_id:
                        sheet_url = exporter.export_to_sheets(
                            st.session_state.collected_videos, 
                            spreadsheet_id=spreadsheet_id
                        )
                    else:
                        sheet_url = exporter.export_to_sheets(
                            st.session_state.collected_videos, 
                            spreadsheet_name=spreadsheet_name
                        )
                    if sheet_url:
                        st.success(f"✅ Exported to Google Sheets!")
                        st.markdown(f"📊 [Open Spreadsheet]({sheet_url})")
                except Exception as e:
                    st.error(f"❌ Export failed: {str(e)}")
    
    # Display collected videos
    if st.session_state.collected_videos:
        st.subheader("📊 Collected Videos")
        df = pd.DataFrame(st.session_state.collected_videos)
        
        st.dataframe(
            df[['title', 'category', 'view_count', 'duration_seconds', 'url']],
            use_container_width=True,
            hide_index=True
        )
        
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
