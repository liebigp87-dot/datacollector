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
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread
from google.oauth2.service_account import Credentials
from youtube_transcript_api import YouTubeTranscriptApi
import isodate

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
                'heartwarming moments caught on camera',
                'acts of kindness compilation',
                'wholesome content that will make you smile',
                'faith in humanity restored',
                'emotional reunions caught on tape',
                'random acts of kindness',
                'heartwarming animal rescues',
                'surprise homecoming compilation'
            ],
            'funny': [
                'funny fails 2024',
                'unexpected moments caught on camera',
                'comedy sketches viral',
                'hilarious reactions compilation',
                'funny animals doing stupid things',
                'epic fail compilation new',
                'instant karma funny moments',
                'comedy gold moments'
            ],
            'traumatic': [
                'shocking moments caught on camera',
                'dramatic rescue operations',
                'natural disaster footage',
                'intense police chases',
                'survival stories real footage',
                'near death experiences caught on tape',
                'unbelievable close calls',
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
            'compilation', 'best of', 'top 10', 'top 20',
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
    
    def check_transcript_availability(self, video_id: str) -> bool:
        """Check if transcript is available for the video"""
        try:
            YouTubeTranscriptApi.list_transcripts(video_id)
            return True
        except:
            return False
    
    def get_video_details(self, video_id: str) -> Optional[Dict]:
        """Get detailed information about a video"""
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
        
        # Check 1: Transcript availability
        self.add_log(f"Checking transcript for: {search_item['snippet']['title'][:50]}...")
        if not self.check_transcript_availability(video_id):
            return False, "No transcript available"
        
        # Check 2-6: Get video details for remaining checks
        details = self.get_video_details(video_id)
        if not details:
            return False, "Could not fetch video details"
        
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
        max_attempts = 10  # Prevent infinite loops
        
        while len(collected) < target_count and attempts < max_attempts:
            current_category = categories[category_index % len(categories)]
            
            # Select random query from category
            available_queries = self.search_queries[current_category]
            query = random.choice(available_queries)
            
            self.add_log(f"Searching category '{current_category}': {query}", "INFO")
            
            # Search for videos
            search_results = self.search_videos(query)
            
            if not search_results:
                self.add_log("No results found for query, trying another...", "WARNING")
                attempts += 1
                continue
            
            # Process each video
            for item in search_results:
                if len(collected) >= target_count:
                    break
                
                st.session_state.stats['checked'] += 1
                
                # Validate video
                passed, reason = self.validate_video(item, item)
                
                if passed:
                    # Get full video details
                    video_id = item['id']['videoId']
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
                            'collected_at': datetime.now().isoformat()
                        }
                        
                        collected.append(video_record)
                        st.session_state.collected_videos.append(video_record)
                        st.session_state.stats['found'] += 1
                        
                        self.add_log(f"✓ Added: {video_record['title'][:50]}...", "SUCCESS")
                        
                        if progress_callback:
                            progress_callback(len(collected), target_count)
                else:
                    st.session_state.stats['rejected'] += 1
                    self.add_log(f"✗ Rejected: {item['snippet']['title'][:50]}... - {reason}", "WARNING")
                
                # Small delay to avoid rate limiting
                time.sleep(0.5)
            
            category_index += 1
            attempts += 1
            
            # Delay between searches
            time.sleep(2)
        
        self.add_log(f"Collection complete! Found {len(collected)} videos.", "SUCCESS")
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
    
    def create_or_get_spreadsheet(self, spreadsheet_name: str):
        """Create a new spreadsheet or get existing one"""
        try:
            # Try to open existing spreadsheet
            spreadsheet = self.client.open(spreadsheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            # Create new spreadsheet
            spreadsheet = self.client.create(spreadsheet_name)
        
        return spreadsheet
    
    def export_to_sheets(self, videos: List[Dict], spreadsheet_name: str = "YouTube_Collection_Data"):
        """Export videos to Google Sheets"""
        spreadsheet = self.create_or_get_spreadsheet(spreadsheet_name)
        
        # Get or create worksheet
        try:
            worksheet = spreadsheet.worksheet("raw_links")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title="raw_links", rows=1000, cols=20)
        
        # Prepare data for export
        if videos:
            df = pd.DataFrame(videos)
            
            # Clear existing content
            worksheet.clear()
            
            # Write headers
            headers = list(df.columns)
            worksheet.append_row(headers)
            
            # Write data
            for _, row in df.iterrows():
                worksheet.append_row(row.tolist())
            
            return spreadsheet.url
        
        return None

def main():
    st.title("🎬 YouTube Data Collector")
    st.markdown("Collects filtered YouTube videos and exports to Google Sheets for n8n workflows")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # API Keys
        st.subheader("API Credentials")
        youtube_api_key = st.text_input("YouTube API Key", type="password", help="Your YouTube Data API v3 key")
        
        # Google Sheets credentials
        st.subheader("Google Sheets Setup")
        sheets_creds = st.text_area(
            "Service Account JSON", 
            type="password",
            help="Paste your Google service account JSON credentials",
            height=150
        )
        
        spreadsheet_name = st.text_input(
            "Spreadsheet Name",
            value="YouTube_Collection_Data",
            help="Name for the Google Sheet (will be created if doesn't exist)"
        )
        
        # Collection settings
        st.subheader("Collection Settings")
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
        if st.button("🚀 Start Collection", disabled=st.session_state.is_collecting):
            if not youtube_api_key:
                st.error("Please enter your YouTube API key")
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
                    
                    st.success(f"Collection complete! Found {len(videos)} videos.")
                    
                    # Auto-export if enabled
                    if auto_export and sheets_creds and videos:
                        try:
                            creds_dict = json.loads(sheets_creds)
                            exporter = GoogleSheetsExporter(creds_dict)
                            sheet_url = exporter.export_to_sheets(videos, spreadsheet_name)
                            st.success(f"✅ Exported to Google Sheets: [Open Sheet]({sheet_url})")
                            collector.add_log(f"Exported to Google Sheets: {sheet_url}", "SUCCESS")
                        except Exception as e:
                            st.error(f"Export failed: {str(e)}")
                            collector.add_log(f"Export error: {str(e)}", "ERROR")
                
                except Exception as e:
                    st.error(f"Collection error: {str(e)}")
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
                st.error("Please add Google Sheets credentials")
            else:
                try:
                    creds_dict = json.loads(sheets_creds)
                    exporter = GoogleSheetsExporter(creds_dict)
                    sheet_url = exporter.export_to_sheets(
                        st.session_state.collected_videos, 
                        spreadsheet_name
                    )
                    st.success(f"✅ Exported to Google Sheets: [Open Sheet]({sheet_url})")
                except Exception as e:
                    st.error(f"Export failed: {str(e)}")
    
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