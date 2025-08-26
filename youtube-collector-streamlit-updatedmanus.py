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

# Note: youtube-transcript-api is no longer needed for caption checking
# We now use the YouTube Data API's contentDetails.caption field

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
    st.session_state.stats = {'checked': 0, 'found': 0, 'rejected': 0}
if 'logs' not in st.session_state:
    st.session_state.logs = []

class YouTubeCollector:
    """Main collector class for YouTube videos"""
    
    def __init__(self, api_key: str):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.search_queries = {
            'heartwarming': [
                'heartwarming moments caught on camera 2024',
                'acts of kindness 2024',
                'wholesome content that will make you smile',
                'faith in humanity restored 2024',
                'emotional reunions caught on tape',
                'random acts of kindness viral',
                'heartwarming animal rescues 2024',
                'surprise homecoming soldier'
            ],
            'funny': [
                'funny fails 2024 new',
                'unexpected moments caught on camera 2024',
                'comedy sketches viral tiktok',
                'hilarious reactions 2024',
                'funny animals doing stupid things',
                'epic fail 2024 new videos',
                'instant karma funny moments',
                'comedy gold moments viral'
            ],
            'traumatic': [
                'shocking moments caught on camera 2024',
                'dramatic rescue operations real',
                'natural disaster footage 2024',
                'intense police chases dashcam',
                'survival stories real footage',
                'near death experiences caught on tape',
                'unbelievable close calls 2024',
                'extreme weather caught on camera'
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
    
    def get_video_details(self, video_id: str) -> Optional[Dict]:
        """Get detailed information about a video including caption availability"""
        try:
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
    
    def validate_video(self, video_data: Dict, search_item: Dict) -> Tuple[bool, str]:
        """
        Validate video against all criteria
        Returns: (passed, reason_if_failed)
        """
        video_id = search_item['id']['videoId']
        
        # Check 1: Get video details first (includes caption check)
        self.add_log(f"Checking video: {search_item['snippet']['title'][:50]}...")
        details = self.get_video_details(video_id)
        if not details:
            return False, "Could not fetch video details"
        
        # Check 2: Caption availability (NEW IMPROVED METHOD)
        has_captions = details['contentDetails'].get('caption', 'false') == 'true'
        if not has_captions:
            return False, "No captions available (auto-generated or manual)"
        
        # Check 3: Age confirmation (redundant but as specified)
        published_at = datetime.fromisoformat(details['snippet']['publishedAt'].replace('Z', '+00:00'))
        six_months_ago = datetime.now(published_at.tzinfo) - timedelta(days=180)
        if published_at < six_months_ago:
            return False, "Video older than 6 months"
        
        # Check 4: Video length (minimum 90 seconds)
        duration = isodate.parse_duration(details['contentDetails']['duration'])
        duration_seconds = duration.total_seconds()
        if duration_seconds < 90:
            return False, f"Video too short ({duration_seconds}s < 90s)"
        
        # Check 5: Content type exclusion
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
        
        # Check 6: View count
        view_count = int(details['statistics'].get('viewCount', 0))
        if view_count < 10000:
            return False, f"View count too low ({view_count} < 10,000)"
        
        # Check 7: Duplicate check
        existing_ids = [v['video_id'] for v in st.session_state.collected_videos]
        if video_id in existing_ids:
            return False, "Duplicate video"
        
        # All checks passed
        return True, "Passed all checks"
    
    def collect_videos(self, target_count: int, category: str, progress_callback=None):
        """Main collection logic"""
        collected = []
        queries_used = []
        
        # Determine categories to use
        if category == 'mixed':
            categories = ['heartwarming', 'funny', 'traumatic']
        else:
            categories = [category]
        
        category_index = 0
        attempts = 0
        max_attempts = 20  # Increased for better results
        videos_checked_ids = set()  # Track checked videos to avoid rechecking
        
        while len(collected) < target_count and attempts < max_attempts:
            current_category = categories[category_index % len(categories)]
            
            # Select random query from category
            available_queries = self.search_queries[current_category]
            query = random.choice(available_queries)
            
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
                
                # Validate video (now includes caption check)
                passed, reason = self.validate_video(item, item)
                
                if passed:
                    # Get full video details (already fetched in validate_video, but we need it again)
                    details = self.get_video_details(video_id)
                    
                    if details:
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
                            'has_captions': details['contentDetails'].get('caption', 'false') == 'true',
                            'collected_at': datetime.now().isoformat()
                        }
                        
                        collected.append(video_record)
                        st.session_state.collected_videos.append(video_record)
                        st.session_state.stats['found'] += 1
                        videos_found_this_query += 1
                        
                        self.add_log(f"✓ Added: {video_record['title'][:50]}... (Captions: ✓)", "SUCCESS")
                        
                        if progress_callback:
                            progress_callback(len(collected), target_count)
                else:
                    st.session_state.stats['rejected'] += 1
                    self.add_log(f"✗ Rejected: {item['snippet']['title'][:50]}... - {reason}", "WARNING")
                
                # Small delay to avoid rate limiting
                time.sleep(0.3)
            
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
                    start_row = len(existing_data) + 1
                    
                    # Convert DataFrame to list of lists for batch update
                    values = df.values.tolist()
                    
                    # Append data
                    worksheet.append_rows(values)
                    st.success(f"✅ Appended {len(videos)} new rows to existing data")
                    
                else:
                    # First time or empty sheet - add headers
                    st.info("Creating new sheet with headers...")
                    
                    # Clear sheet and add headers + data
                    worksheet.clear()
                    
                    # Prepare data with headers
                    headers = df.columns.tolist()
                    values = [headers] + df.values.tolist()
                    
                    # Update sheet
                    worksheet.update('A1', values)
                    st.success(f"✅ Created new sheet with {len(videos)} videos")
                
                # Return spreadsheet URL
                return spreadsheet.url
            
            else:
                st.warning("No videos to export")
                return None
                
        except Exception as e:
            st.error(f"Export error: {str(e)}")
            raise e

def main():
    """Main Streamlit app"""
    
    st.title("🎬 YouTube Data Collector")
    st.markdown("*Automated collection of filtered YouTube videos for n8n workflows*")
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # YouTube API Key
        st.subheader("1. YouTube API")
        youtube_api_key = st.text_input(
            "YouTube Data API Key",
            type="password",
            help="Get your API key from Google Cloud Console"
        )
        
        if youtube_api_key:
            st.success("✅ API Key provided")
        
        # Google Sheets credentials
        st.subheader("2. Google Sheets Export")
        sheets_creds = None
        
        credentials_input = st.text_area(
            "Service Account JSON",
            height=150,
            help="Paste your Google Service Account JSON credentials here"
        )
        
        if credentials_input:
            try:
                sheets_creds = json.loads(credentials_input)
                st.success("✅ Credentials loaded")
            except json.JSONDecodeError:
                st.error("❌ Invalid JSON format")
        
        # Spreadsheet options
        use_existing = st.checkbox("Use existing spreadsheet", help="Connect to an existing Google Sheet by ID")
        
        if use_existing:
            spreadsheet_id = st.text_input(
                "Spreadsheet ID",
                help="Get the ID from the spreadsheet URL"
            )
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
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Videos Found", st.session_state.stats['found'])
    with col2:
        st.metric("Videos Checked", st.session_state.stats['checked'])
    with col3:
        st.metric("Videos Rejected", st.session_state.stats['rejected'])
    
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
                    collector = YouTubeCollector(youtube_api_key)
                    
                    # Progress bar
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    def update_progress(current, total):
                        progress = current / total
                        progress_bar.progress(progress)
                        status_text.text(f"Collecting: {current}/{total} videos")
                    
                    # Run collection
                    with st.spinner(f"Collecting {target_count} videos..."):
                        videos = collector.collect_videos(
                            target_count=target_count,
                            category=category,
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
            df[['title', 'category', 'view_count', 'duration_seconds', 'has_captions', 'url']],
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

