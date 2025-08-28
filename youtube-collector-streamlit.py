"""
Combined YouTube Data Collector & Video Rating Tool
Collects YouTube videos and rates them for content suitability
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
import requests
import numpy as np
from PIL import Image
import io
import xml.etree.ElementTree as ET
from urllib.parse import unquote

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
    page_title="YouTube Collection & Rating Tool",
    page_icon="🎬",
    layout="wide"
)

# CSS styling
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 2rem 0;
        margin-bottom: 2rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 15px;
    }
    .category-card {
        background: #262730;
        color: #fafafa;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 2px 10px rgba(0,0,0,0.3);
        border: 1px solid #404040;
    }
    .score-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        text-align: center;
        margin: 1rem 0;
    }
    .component-card {
        background: #262730;
        color: #fafafa;
        padding: 1.2rem;
        border-radius: 8px;
        border-left: 4px solid #667eea;
        margin: 0.5rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    }
    .timestamp-moment {
        background: #2d3748;
        color: #e2e8f0;
        padding: 0.8rem;
        border-radius: 6px;
        margin: 0.5rem 0;
        border-left: 3px solid #4299e1;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'collected_videos' not in st.session_state:
    st.session_state.collected_videos = []
if 'is_collecting' not in st.session_state:
    st.session_state.is_collecting = False
if 'is_rating' not in st.session_state:
    st.session_state.is_rating = False
if 'collector_stats' not in st.session_state:
    st.session_state.collector_stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0, 'has_captions': 0, 'no_captions': 0}
if 'rater_stats' not in st.session_state:
    st.session_state.rater_stats = {'rated': 0, 'moved_to_tobe': 0, 'rejected': 0, 'api_calls': 0}
if 'logs' not in st.session_state:
    st.session_state.logs = []
if 'used_queries' not in st.session_state:
    st.session_state.used_queries = set()
if 'analysis_history' not in st.session_state:
    st.session_state.analysis_history = []
if 'system_status' not in st.session_state:
    st.session_state.system_status = {'type': None, 'message': ''}

def show_status_alert():
    """Display system status alerts prominently"""
    if st.session_state.system_status['type']:
        if st.session_state.system_status['type'] == 'error':
            st.error(f"🚫 {st.session_state.system_status['message']}")
        elif st.session_state.system_status['type'] == 'warning':
            st.warning(f"⚠️ {st.session_state.system_status['message']}")
        elif st.session_state.system_status['type'] == 'info':
            st.info(f"ℹ️ {st.session_state.system_status['message']}")

def set_status(status_type: str, message: str):
    """Set system status message"""
    st.session_state.system_status = {'type': status_type, 'message': message}

def clear_status():
    """Clear system status message"""
    st.session_state.system_status = {'type': None, 'message': ''}

CATEGORIES = {
    'heartwarming': {
        'name': 'Heartwarming Content',
        'emoji': '❤️',
        'description': 'Genuine emotional moments that create positive feelings'
    },
    'funny': {
        'name': 'Funny Content', 
        'emoji': '😂',
        'description': 'Humorous content that entertains and amuses'
    },
    'traumatic': {
        'name': 'Traumatic Events',
        'emoji': '⚠️', 
        'description': 'Serious events with significant impact'
    }
}

class GoogleSheetsExporter:
    """Handle Google Sheets export and import"""
    
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
            return spreadsheet
        except Exception as e:
            raise e
    
    def get_next_raw_video(self, spreadsheet_id: str) -> Optional[Dict]:
        """Get next video from raw_links sheet"""
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            worksheet = spreadsheet.worksheet("raw_links")
            all_values = worksheet.get_all_values()
            
            if len(all_values) > 1:
                headers = all_values[0]
                first_row = all_values[1]
                
                # Convert to dict
                video_data = {headers[i]: first_row[i] for i in range(len(headers))}
                video_data['row_number'] = 2  # Row 2 in Google Sheets (1-indexed)
                return video_data
            return None
        except Exception as e:
            st.error(f"Error fetching next video: {str(e)}")
            return None
    
    def delete_raw_video(self, spreadsheet_id: str, row_number: int):
        """Delete video from raw_links sheet"""
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            worksheet = spreadsheet.worksheet("raw_links")
            worksheet.delete_rows(row_number)
        except Exception as e:
            st.error(f"Error deleting video: {str(e)}")
    
    def add_to_tobe_links(self, spreadsheet_id: str, video_data: Dict, analysis_data: Dict):
        """Add video to tobe_links sheet with analysis data"""
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet("tobe_links")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="tobe_links", rows=1000, cols=25)
                
                # Create headers combining raw_links + analysis data
                headers = [
                    'video_id', 'title', 'url', 'category', 'search_query', 
                    'duration_seconds', 'view_count', 'like_count', 'comment_count',
                    'published_at', 'channel_title', 'tags', 'collected_at',
                    'score', 'confidence', 'timestamped_moments', 'category_validation',
                    'analysis_timestamp'
                ]
                worksheet.append_row(headers)
            
            # Prepare row data
            row_data = [
                video_data.get('video_id', ''),
                video_data.get('title', ''),
                video_data.get('url', ''),
                video_data.get('category', ''),
                video_data.get('search_query', ''),
                video_data.get('duration_seconds', ''),
                video_data.get('view_count', ''),
                video_data.get('like_count', ''),
                video_data.get('comment_count', ''),
                video_data.get('published_at', ''),
                video_data.get('channel_title', ''),
                video_data.get('tags', ''),
                video_data.get('collected_at', ''),
                analysis_data.get('final_score', ''),
                analysis_data.get('confidence', ''),
                len(analysis_data.get('comments_analysis', {}).get('timestamped_moments', [])),
                analysis_data.get('comments_analysis', {}).get('category_validation', ''),
                datetime.now().isoformat()
            ]
            
            worksheet.append_row(row_data)
        except Exception as e:
            st.error(f"Error adding to tobe_links: {str(e)}")
    
    def add_to_discarded(self, spreadsheet_id: str, video_url: str):
        """Add video URL to discarded table"""
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet("discarded")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="discarded", rows=1000, cols=1)
                worksheet.append_row(['url'])  # Header
            
            # Add just the URL
            worksheet.append_row([video_url])
        except Exception as e:
            st.error(f"Error adding to discarded: {str(e)}")
    
    def load_discarded_urls(self, spreadsheet_id: str) -> set:
        """Load existing URLs from discarded sheet"""
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            try:
                worksheet = spreadsheet.worksheet("discarded")
                all_values = worksheet.get_all_values()
                
                if len(all_values) > 1:
                    # Skip header row, get URLs from first column
                    discarded_urls = {row[0] for row in all_values[1:] if row and row[0]}
                    return discarded_urls
            except gspread.exceptions.WorksheetNotFound:
                # Sheet doesn't exist yet, return empty set
                pass
            return set()
        except Exception as e:
            st.error(f"Error loading discarded URLs: {str(e)}")
            return set()
    
    def add_time_comments(self, spreadsheet_id: str, video_id: str, video_url: str, comments_analysis: Dict):
        """Add timestamped and category-matched comments to time_comments table"""
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet("time_comments")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="time_comments", rows=1000, cols=10)
                
                # Create headers
                headers = [
                    'video_id', 'video_url', 'comment_text', 'timestamp', 
                    'category_matched', 'relevance_score', 'sentiment'
                ]
                worksheet.append_row(headers)
            
            # Get timestamped moments from analysis
            moments = comments_analysis.get('timestamped_moments', [])
            
            for moment in moments:
                row_data = [
                    video_id,
                    video_url,
                    moment.get('comment', ''),
                    moment.get('timestamp', ''),
                    moment.get('category_matches', 0),
                    moment.get('relevance_score', 0),
                    moment.get('sentiment', '')
                ]
                worksheet.append_row(row_data)
                
        except Exception as e:
            st.error(f"Error adding to time_comments: {str(e)}")
    
    def export_to_sheets(self, videos: List[Dict], spreadsheet_id: str = None, spreadsheet_name: str = "YouTube_Collection_Data"):
        """Export videos to raw_links sheet"""
        try:
            if spreadsheet_id:
                spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            else:
                try:
                    spreadsheet = self.client.open(spreadsheet_name)
                except gspread.exceptions.SpreadsheetNotFound:
                    spreadsheet = self.client.create(spreadsheet_name)
            
            worksheet_name = "raw_links"
            
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
            
            if videos:
                df = pd.DataFrame(videos)
                existing_data = worksheet.get_all_values()
                
                if existing_data and len(existing_data) > 1:
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


class YouTubeCollector:
    """YouTube video collection functionality"""
    
    def __init__(self, api_key: str, sheets_exporter=None):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.sheets_exporter = sheets_exporter
        self.existing_sheet_ids = set()
        self.existing_queries = set()
        self.discarded_urls = set()
        
        # Search queries for unified categories
        self.search_queries = {
            'heartwarming': [
                'soldier surprise homecoming', 'dog reunion owner', 'random acts kindness',
                'baby first time hearing', 'proposal reaction emotional', 'surprise gift reaction',
                'homeless man helped', 'teacher surprised students', 'reunion after years',
                'saving animal rescue', 'kid helps stranger', 'emotional wedding moment',
                'surprise visit family', 'grateful reaction wholesome', 'community helps neighbor',
                'dad meets baby', 'emotional support moment', 'stranger pays bill',
                'found lost pet', 'surprise donation reaction', 'elderly couple sweet',
                'child generous sharing', 'unexpected hero saves', 'touching tribute video',
                'surprise reunion compilation', 'faith humanity restored', 'emotional thank you',
                'surprise birthday elderly', 'veteran honored ceremony', 'wholesome interaction strangers'
            ],
            'funny': [
                'funny fails compilation', 'unexpected moments caught', 'comedy sketches viral',
                'hilarious reactions', 'funny animals doing', 'epic fail video',
                'instant karma funny', 'comedy gold moments', 'prank goes wrong',
                'funny kids saying', 'dad jokes reaction', 'wedding fails funny',
                'sports bloopers hilarious', 'funny news bloopers', 'pet fails compilation',
                'funny work moments', 'hilarious misunderstanding', 'comedy timing perfect',
                'funny voice over', 'unexpected plot twist', 'funny security camera',
                'hilarious interview moments', 'comedy accident harmless', 'funny dancing fails',
                'laughing contagious video', 'funny sleep talking', 'comedy scare pranks',
                'funny workout fails', 'hilarious costume fails', 'funny zoom fails'
            ],
            'traumatic': [
                'shocking moments caught', 'dramatic rescue operation', 'natural disaster footage',
                'intense police chase', 'survival story real', 'near death experience',
                'unbelievable close call', 'extreme weather footage', 'emergency response dramatic',
                'accident caught camera', 'dangerous situation survived', 'storm chaser footage',
                'rescue mission dramatic', 'wildfire evacuation footage', 'flood rescue dramatic',
                'earthquake footage real', 'tornado close encounter', 'avalanche survival story',
                'lightning strike caught', 'road rage incident', 'building collapse footage',
                'helicopter rescue dramatic', 'cliff rescue operation', 'shark encounter real',
                'volcano eruption footage', 'mudslide caught camera', 'train near miss',
                'bridge collapse footage', 'explosion caught camera', 'emergency landing footage'
            ]
        }
        
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
        """Add a detailed log entry"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] COLLECTOR {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]  # Keep more logs for debugging
    
    def check_quota_available(self) -> Tuple[bool, str]:
        """Check if YouTube API quota is available"""
        try:
            self.add_log("Checking API quota availability...", "INFO")
            test_request = self.youtube.videos().list(
                part='id',
                id='YbJOTdZBX1g'
            )
            response = test_request.execute()
            st.session_state.collector_stats['api_calls'] += 1
            self.add_log("API quota check passed", "SUCCESS")
            return True, "Quota available"
        except HttpError as e:
            error_str = str(e)
            if 'quotaExceeded' in error_str:
                self.add_log("YouTube API quota exceeded", "ERROR")
                return False, "Daily quota exceeded. Wait 24 hours or use different API key."
            elif 'forbidden' in error_str.lower():
                self.add_log("API access forbidden", "ERROR")
                return False, "API key invalid or YouTube Data API not enabled"
            else:
                self.add_log(f"API error: {error_str[:100]}", "WARNING")
                return True, "Warning: API error but attempting to continue"
        except Exception as e:
            self.add_log(f"Non-API error: {str(e)[:100]}", "WARNING")
            return True, "Could not verify quota, proceeding anyway"
    
    def search_videos(self, query: str, max_results: int = 25) -> List[Dict]:
        """Search for videos using YouTube API"""
        try:
            st.session_state.collector_stats['api_calls'] += 1
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
            st.session_state.collector_stats['api_calls'] += 1
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
        """Check if video has captions"""
        try:
            caption_value = details.get('contentDetails', {}).get('caption', False)
            
            has_caption = False
            if isinstance(caption_value, bool):
                has_caption = caption_value
            elif isinstance(caption_value, str):
                has_caption = caption_value.lower() == 'true'
            
            if has_caption:
                st.session_state.collector_stats['has_captions'] += 1
            else:
                st.session_state.collector_stats['no_captions'] += 1
            
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
                    self.add_log(f"Loaded {len(existing_ids)} existing video IDs from raw_links", "INFO")
                    return existing_ids
            return set()
        except Exception as e:
            self.add_log(f"Could not load existing sheet IDs: {str(e)}", "WARNING")
            return set()
    
    def load_discarded_urls(self, spreadsheet_id: str) -> set:
        """Load discarded URLs to prevent reprocessing"""
        try:
            if self.sheets_exporter:
                discarded_urls = self.sheets_exporter.load_discarded_urls(spreadsheet_id)
                self.add_log(f"Loaded {len(discarded_urls)} discarded URLs for duplicate check", "INFO")
                return discarded_urls
            return set()
        except Exception as e:
            self.add_log(f"Could not load discarded URLs: {str(e)}", "WARNING")
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
    
    def validate_video_for_category(self, search_item: Dict, target_category: str, require_captions: bool = True) -> Tuple[bool, str]:
        """Validate video against collection criteria for specific category with detailed logging"""
        video_id = search_item['id']['videoId']
        video_url = f"https://youtube.com/watch?v={video_id}"
        title = search_item['snippet']['title']
        
        self.add_log(f"Starting validation for: {title[:50]}...")
        
        # Step 1: Get video details
        details = self.get_video_details(video_id)
        if not details:
            self.add_log(f"REJECTED: Could not fetch video details for {video_id}", "WARNING")
            return False, "Could not fetch video details"
        
        self.add_log(f"✓ Video details fetched successfully", "INFO")
        
        # Step 2: Duplicate check - Current session
        existing_ids = [v['video_id'] for v in st.session_state.collected_videos]
        if video_id in existing_ids:
            self.add_log(f"REJECTED: Video {video_id} already in current session", "WARNING")
            return False, "Duplicate video (already in current session)"
        
        self.add_log(f"✓ Not duplicate in current session", "INFO")
        
        # Step 3: Duplicate check - Already in raw_links
        if video_id in self.existing_sheet_ids:
            self.add_log(f"REJECTED: Video {video_id} already exists in raw_links sheet", "WARNING")
            return False, "Duplicate video (already in raw_links)"
        
        self.add_log(f"✓ Not duplicate in raw_links sheet", "INFO")
        
        # Step 4: Duplicate check - Already processed (in discarded)
        if video_url in self.discarded_urls:
            self.add_log(f"REJECTED: Video URL already processed (found in discarded table)", "WARNING")
            return False, "Video already processed (found in discarded)"
        
        self.add_log(f"✓ Not found in discarded table", "INFO")
        
        # Step 5: Caption check
        if require_captions:
            has_captions = self.check_caption_availability(details)
            if not has_captions:
                self.add_log(f"REJECTED: No captions available for video {video_id}", "WARNING")
                return False, "No captions available"
            self.add_log(f"✓ Captions available", "INFO")
        else:
            self.check_caption_availability(details)
            self.add_log(f"✓ Caption check skipped (not required)", "INFO")
        
        # Step 6: Age check
        published_at = datetime.fromisoformat(details['snippet']['publishedAt'].replace('Z', '+00:00'))
        six_months_ago = datetime.now(published_at.tzinfo) - timedelta(days=180)
        age_days = (datetime.now(published_at.tzinfo) - published_at).days
        
        if published_at < six_months_ago:
            self.add_log(f"REJECTED: Video too old ({age_days} days, limit: 180 days)", "WARNING")
            return False, "Video older than 6 months"
        
        self.add_log(f"✓ Age check passed ({age_days} days old)", "INFO")
        
        # Step 7: YouTube Short check
        if self.is_youtube_short(video_id, details):
            self.add_log(f"REJECTED: Video detected as YouTube Short", "WARNING")
            return False, "YouTube Short detected"
        
        self.add_log(f"✓ Not a YouTube Short", "INFO")
        
        # Step 8: Duration check
        duration = isodate.parse_duration(details['contentDetails']['duration'])
        duration_seconds = duration.total_seconds()
        
        if duration_seconds < 90:
            self.add_log(f"REJECTED: Video too short ({duration_seconds}s, minimum: 90s)", "WARNING")
            return False, f"Video too short ({duration_seconds}s < 90s)"
        
        self.add_log(f"✓ Duration check passed ({duration_seconds}s)", "INFO")
        
        # Step 9: Content type exclusion - Music
        title_lower = details['snippet']['title'].lower()
        tags = [tag.lower() for tag in details['snippet'].get('tags', [])]
        
        for keyword in self.music_keywords:
            if keyword in title_lower or any(keyword in tag for tag in tags):
                self.add_log(f"REJECTED: Music video detected (keyword: {keyword})", "WARNING")
                return False, f"Music video detected (keyword: {keyword})"
        
        self.add_log(f"✓ Music video check passed", "INFO")
        
        # Step 10: Content type exclusion - Compilation
        for keyword in self.compilation_keywords:
            if keyword in title_lower or any(keyword in tag for tag in tags):
                self.add_log(f"REJECTED: Compilation video detected (keyword: {keyword})", "WARNING")
                return False, f"Compilation detected (keyword: {keyword})"
        
        self.add_log(f"✓ Compilation check passed", "INFO")
        
        # Step 11: View count check
        view_count = int(details['statistics'].get('viewCount', 0))
        if view_count < 10000:
            self.add_log(f"REJECTED: View count too low ({view_count:,}, minimum: 10,000)", "WARNING")
            return False, f"View count too low ({view_count} < 10,000)"
        
        self.add_log(f"✓ View count check passed ({view_count:,} views)", "INFO")
        
        # Step 12: Category relevance check
        title_desc_text = (title_lower + ' ' + details['snippet'].get('description', '')).lower()
        
        category_keywords = {
            'heartwarming': ['heartwarming', 'touching', 'emotional', 'reunion', 'surprise', 'family', 'love', 
                           'soldier', 'homecoming', 'dog reunion', 'acts kindness', 'baby first time', 
                           'proposal reaction', 'homeless helped', 'teacher surprised', 'saving animal',
                           'grateful', 'wholesome', 'sweet', 'helping'],
            'funny': ['funny', 'comedy', 'humor', 'hilarious', 'joke', 'laugh', 'entertaining', 'fails', 
                     'epic fail', 'instant karma', 'prank', 'bloopers', 'comedy gold', 'dad jokes',
                     'silly', 'amusing', 'comical', 'laughing'],
            'traumatic': ['accident', 'tragedy', 'disaster', 'emergency', 'breaking news', 'shocking',
                        'dramatic rescue', 'natural disaster', 'police chase', 'survival story', 'near death',
                        'extreme weather', 'earthquake', 'tornado', 'avalanche', 'explosion',
                        'crash', 'incident', 'dangerous', 'intense']
        }
        
        keywords = category_keywords.get(target_category, [])
        matched_keywords = [kw for kw in keywords if kw in title_desc_text]
        
        if not matched_keywords:
            self.add_log(f"REJECTED: No {target_category} keywords found in title/description", "WARNING")
            return False, f"No {target_category} keywords found in title/description"
        
        self.add_log(f"✓ Category check passed - matched keywords: {', '.join(matched_keywords[:3])}", "SUCCESS")
        self.add_log(f"VALIDATION COMPLETE: Video {video_id} passed all checks!", "SUCCESS")
        
        return True, details
    
    def collect_videos(self, target_count: int, category: str, spreadsheet_id: str = None, require_captions: bool = True, progress_callback=None):
        """Main collection logic for specified category"""
        collected = []
        
        if category == 'mixed':
            categories = ['heartwarming', 'funny', 'traumatic']
        else:
            categories = [category]
        
        self.add_log(f"Starting collection for category: {category}, target: {target_count} videos", "INFO")
        
        # Load existing data from sheet
        if spreadsheet_id and self.sheets_exporter:
            self.existing_sheet_ids = self.load_existing_sheet_ids(spreadsheet_id)
            self.discarded_urls = self.load_discarded_urls(spreadsheet_id)
            self.existing_queries = self.load_used_queries(spreadsheet_id)
            st.session_state.used_queries.update(self.existing_queries)
        
        category_index = 0
        attempts = 0
        max_attempts = 30
        videos_checked_ids = set()
        
        while len(collected) < target_count and attempts < max_attempts:
            current_category = categories[category_index % len(categories)]
            
            available_queries = self.search_queries[current_category].copy()
            random.shuffle(available_queries)
            
            query = None
            for potential_query in available_queries:
                if potential_query not in st.session_state.used_queries:
                    query = potential_query
                    break
            
            if not query:
                query = random.choice(available_queries)
            
            st.session_state.used_queries.add(query)
            self.add_log(f"Searching category '{current_category}': {query}", "INFO")
            
            search_results = self.search_videos(query)
            
            if not search_results:
                attempts += 1
                category_index += 1
                continue
            
            videos_found_this_query = 0
            for item in search_results:
                if len(collected) >= target_count:
                    break
                
                video_id = item['id']['videoId']
                
                if video_id in videos_checked_ids:
                    continue
                
                videos_checked_ids.add(video_id)
                st.session_state.collector_stats['checked'] += 1
                
                result = self.validate_video_for_category(item, current_category, require_captions)
                
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
                    st.session_state.collector_stats['found'] += 1
                    videos_found_this_query += 1
                    
                    self.add_log(f"✅ ADDED TO COLLECTION: {video_record['title'][:50]}... (category: {current_category})", "SUCCESS")
                    self.add_log(f"Collection stats - Found: {st.session_state.collector_stats['found']}, Target: {target_count}", "INFO")
                    
                    if progress_callback:
                        progress_callback(len(collected), target_count)
                else:
                    reason = result[1]
                    st.session_state.collector_stats['rejected'] += 1
                
                time.sleep(0.3)
            
            # Save used query
            if spreadsheet_id and self.sheets_exporter:
                self.save_used_query(spreadsheet_id, query, current_category, videos_found_this_query)
            
            if videos_found_this_query == 0:
                category_index += 1
            else:
                if videos_found_this_query >= 2:
                    category_index += 1
            
            attempts += 1
            time.sleep(1.5)
        
        return collected


class VideoRater:
    """Video rating functionality with comment analysis"""
    
    def __init__(self, api_key: str):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
    
    def add_log(self, message: str, log_type: str = "INFO"):
        """Add a detailed log entry"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] RATER {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]  # Keep more logs for debugging
    
    def check_quota_available(self) -> Tuple[bool, str]:
        """Check if YouTube API quota is available"""
        try:
            self.add_log("Checking API quota availability...", "INFO")
            test_request = self.youtube.videos().list(
                part='id',
                id='YbJOTdZBX1g'
            )
            response = test_request.execute()
            st.session_state.rater_stats['api_calls'] += 1
            self.add_log("API quota check passed", "SUCCESS")
            return True, "Quota available"
        except HttpError as e:
            error_str = str(e)
            if 'quotaExceeded' in error_str:
                self.add_log("YouTube API quota exceeded", "ERROR")
                return False, "Daily quota exceeded"
            elif 'forbidden' in error_str.lower():
                self.add_log("API access forbidden", "ERROR")
                return False, "API key invalid"
            else:
                self.add_log(f"API error: {error_str[:100]}", "WARNING")
                return True, "Warning but continuing"
        except Exception as e:
            return True, "Could not verify quota"
    
    def extract_video_id(self, url):
        """Extract video ID from YouTube URL"""
        pattern = r'(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/)([^&\n?#]+)'
        match = re.search(pattern, url)
        return match.group(1) if match else None
    
    def parse_duration(self, duration_str):
        """Parse ISO 8601 duration to readable format"""
        pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
        match = re.match(pattern, duration_str)
        if not match:
            return "0:00"
        
        hours, minutes, seconds = match.groups()
        hours = int(hours) if hours else 0
        minutes = int(minutes) if minutes else 0
        seconds = int(seconds) if seconds else 0
        
        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes}:{seconds:02d}"
    
    def analyze_sentiment(self, text):
        """Basic sentiment analysis"""
        text_lower = text.lower()
        positive_words = ['amazing', 'incredible', 'beautiful', 'love', 'great', 'good', 'nice', 'happy']
        negative_words = ['terrible', 'awful', 'worst', 'hate', 'bad', 'fake', 'boring']
        
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        
        if pos_count > neg_count:
            return 'positive'
        elif neg_count > pos_count:
            return 'negative'
        else:
            return 'neutral'
    
    def extract_timestamped_moments(self, comments, category_key):
        """Extract timestamped comments for clippable moments"""
        timestamp_pattern = r'(?:at\s+)?(\d{1,2}):(\d{2})|(\d+:\d+)'
        moments = []
        
        category_keywords = {
            'heartwarming': ['crying', 'tears', 'emotional', 'touching', 'beautiful', 'best part', 'favorite moment'],
            'funny': ['laugh', 'hilarious', 'funny', 'lol', 'comedy', 'joke', 'humor'],
            'traumatic': ['shocking', 'unbelievable', 'devastating', 'terrible', 'awful', 'important', 'crucial moment']
        }
        
        clip_indicators = ['clip this', 'short', 'viral', 'best part', 'highlight', 'moment', 'scene', 'timestamp', 'here']
        
        for comment in comments:
            timestamps = re.findall(timestamp_pattern, comment)
            if timestamps:
                comment_lower = comment.lower()
                
                relevance_score = 0
                
                category_matches = sum(1 for kw in category_keywords.get(category_key, []) if kw in comment_lower)
                relevance_score += category_matches * 2
                
                clip_matches = sum(1 for ind in clip_indicators if ind in comment_lower)
                relevance_score += clip_matches * 1.5
                
                if len(comment) > 50:
                    relevance_score += 1
                
                strong_words = ['amazing', 'incredible', 'unbelievable', 'perfect', 'exactly', 'omg', 'wow']
                emotion_matches = sum(1 for word in strong_words if word in comment_lower)
                relevance_score += emotion_matches
                
                if relevance_score > 0:
                    for timestamp_match in timestamps:
                        timestamp = ':'.join(filter(None, timestamp_match))
                        
                        time_parts = timestamp.split(':')
                        seconds = int(time_parts[0]) * 60 + int(time_parts[1]) if len(time_parts) == 2 else 0
                        
                        moments.append({
                            'timestamp': timestamp,
                            'seconds': seconds,
                            'comment': comment,
                            'relevance_score': relevance_score,
                            'category_matches': category_matches,
                            'clip_potential': clip_matches > 0,
                            'sentiment': self.analyze_sentiment(comment)
                        })
        
        return sorted(moments, key=lambda x: (-x['relevance_score'], x['seconds']))
    
    def fetch_comments(self, video_id, max_results=500):
        """Fetch comments from YouTube video"""
        comments = []
        sentiment_data = {'positive': 0, 'negative': 0, 'neutral': 0, 'total': 0}
        
        try:
            url = f"https://www.googleapis.com/youtube/v3/commentThreads"
            
            for order in ['relevance', 'time']:
                params = {
                    'part': 'snippet',
                    'videoId': video_id,
                    'maxResults': 100,
                    'order': order,
                    'key': self.youtube._developerKey
                }
                
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    
                    for item in data.get('items', []):
                        comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                        
                        if comment_text not in comments and len(comment_text.strip()) > 5:
                            comments.append(comment_text)
                            
                            sentiment = self.analyze_sentiment(comment_text)
                            sentiment_data[sentiment] += 1
                            sentiment_data['total'] += 1
                            
                            if len(comments) >= max_results:
                                break
        except Exception as e:
            self.add_log(f"Limited comment access: {str(e)}", "WARNING")
        
        return {
            'comments': comments,
            'sentiment_analysis': sentiment_data,
            'total_fetched': len(comments)
        }
    
    def fetch_video_data(self, video_id):
        """Fetch complete video data including comments"""
        try:
            video_url = f"https://www.googleapis.com/youtube/v3/videos"
            params = {
                'part': 'snippet,statistics,contentDetails',
                'id': video_id,
                'key': self.youtube._developerKey
            }
            
            response = requests.get(video_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('items'):
                raise Exception("Video not found")
            
            video = data['items'][0]
            stats = video['statistics']
            snippet = video['snippet']
            content_details = video['contentDetails']
            
            comments_data = self.fetch_comments(video_id)
            
            return {
                'videoId': video_id,
                'title': snippet['title'],
                'description': snippet.get('description', ''),
                'viewCount': int(stats.get('viewCount', 0)),
                'likeCount': int(stats.get('likeCount', 0)),
                'commentCount': int(stats.get('commentCount', 0)),
                'duration': self.parse_duration(content_details['duration']),
                'publishedAt': snippet['publishedAt'],
                'channelTitle': snippet['channelTitle'],
                'comments': comments_data['comments'],
                'comment_sentiment': comments_data['sentiment_analysis'],
                'total_comments_fetched': comments_data['total_fetched']
            }
            
        except requests.exceptions.RequestException as e:
            if e.response and e.response.status_code == 403:
                raise Exception("API key error")
            elif e.response and e.response.status_code == 429:
                raise Exception("API quota exceeded")
            else:
                raise Exception(f"Error fetching data: {str(e)}")
    
    def analyze_comments_for_category(self, comments, category_key):
        """Analyze comments for category-specific patterns"""
        if not comments:
            return {
                'category_validation': 0.0,
                'emotional_alignment': 0.0,
                'authenticity_support': 0.0,
                'engagement_quality': 0.0,
                'breakdown': {},
                'timestamped_moments': []
            }
        
        all_text = ' '.join(comments).lower()
        timestamped_moments = self.extract_timestamped_moments(comments, category_key)
        
        if category_key == 'heartwarming':
            positive_emotions = ['crying', 'tears', 'emotional', 'beautiful', 'touching', 'moving', 'wholesome']
            authenticity_words = ['real', 'genuine', 'authentic', 'natural']
            fake_indicators = ['fake', 'staged', 'acting', 'scripted']
            
            positive_count = sum(1 for word in positive_emotions if word in all_text)
            auth_count = sum(1 for word in authenticity_words if word in all_text)
            fake_count = sum(1 for word in fake_indicators if word in all_text)
            
            validation = min(positive_count / max(len(comments) * 0.05, 1), 1.0)
            emotional = min(positive_count / max(len(comments) * 0.03, 1), 1.0)
            authenticity = max(0.1, min(auth_count / max(fake_count + 1, 1) * 0.5, 1.0))
            
            return {
                'category_validation': validation,
                'emotional_alignment': emotional,
                'authenticity_support': authenticity,
                'engagement_quality': min(positive_count / max(len(comments), 1), 1.0),
                'timestamped_moments': timestamped_moments,
                'breakdown': {
                    'positive_emotions': positive_count,
                    'authenticity_indicators': auth_count,
                    'fake_indicators': fake_count,
                    'timestamped_moments_found': len(timestamped_moments)
                }
            }
        
        elif category_key == 'funny':
            humor_words = ['laugh', 'funny', 'hilarious', 'lol', 'haha', 'comedy', 'joke']
            entertainment_words = ['entertaining', 'fun', 'enjoy', 'smile']
            boring_words = ['boring', 'not funny', 'stupid', 'lame']
            
            humor_count = sum(1 for word in humor_words if word in all_text)
            entertain_count = sum(1 for word in entertainment_words if word in all_text)
            boring_count = sum(1 for word in boring_words if word in all_text)
            
            validation = min(humor_count / max(len(comments) * 0.03, 1), 1.0)
            emotional = min(humor_count / max(len(comments) * 0.02, 1), 1.0)
            authenticity = 0.5 if humor_count == boring_count else min(0.8, max(0.2, humor_count / max(boring_count + 1, 1) * 0.4))
            
            return {
                'category_validation': validation,
                'emotional_alignment': emotional,
                'authenticity_support': authenticity,
                'engagement_quality': min(humor_count / max(len(comments), 1), 1.0),
                'timestamped_moments': timestamped_moments,
                'breakdown': {
                    'humor_reactions': humor_count,
                    'entertainment_validation': entertain_count,
                    'negative_reactions': boring_count,
                    'timestamped_moments_found': len(timestamped_moments)
                }
            }
        
        elif category_key == 'traumatic':
            empathy_words = ['prayers', 'sorry', 'sad', 'terrible', 'awful', 'devastating']
            concern_words = ['hope everyone ok', 'what happened', 'is everyone safe']
            inappropriate_words = ['lol', 'funny', 'cool', 'awesome']
            
            empathy_count = sum(1 for word in empathy_words if word in all_text)
            concern_count = sum(1 for phrase in concern_words if phrase in all_text)
            inappropriate_count = sum(1 for word in inappropriate_words if word in all_text)
            
            appropriate_total = empathy_count + concern_count
            
            validation = min(appropriate_total / max(len(comments) * 0.05, 1), 1.0)
            if inappropriate_count > appropriate_total:
                validation *= 0.3
                
            emotional = min(empathy_count / max(len(comments) * 0.03, 1), 1.0)
            authenticity = min(0.8, max(0.2, appropriate_total / max(inappropriate_count + 1, 1) * 0.3))
            
            return {
                'category_validation': validation,
                'emotional_alignment': emotional,
                'authenticity_support': authenticity,
                'engagement_quality': min(appropriate_total / max(len(comments), 1), 1.0),
                'timestamped_moments': timestamped_moments,
                'breakdown': {
                    'empathetic_responses': empathy_count,
                    'concern_responses': concern_count,
                    'inappropriate_responses': inappropriate_count,
                    'timestamped_moments_found': len(timestamped_moments)
                }
            }
        
        return {
            'category_validation': 0.5,
            'emotional_alignment': 0.5,
            'authenticity_support': 0.5,
            'engagement_quality': 0.5,
            'timestamped_moments': timestamped_moments,
            'breakdown': {}
        }
    
    def calculate_category_score(self, video_data, category_key):
        """Calculate final category score with original weighting system"""
        comments_analysis = self.analyze_comments_for_category(video_data['comments'], category_key)
        
        title_desc_text = (video_data['title'] + ' ' + video_data['description']).lower()
        
        category_keywords = {
            'heartwarming': ['heartwarming', 'touching', 'emotional', 'reunion', 'surprise', 'family', 'love'],
            'funny': ['funny', 'comedy', 'humor', 'hilarious', 'joke', 'laugh', 'entertaining'],
            'traumatic': ['accident', 'tragedy', 'disaster', 'emergency', 'breaking news', 'shocking']
        }
        
        keyword_matches = sum(1 for kw in category_keywords.get(category_key, []) if kw in title_desc_text)
        content_match = min(keyword_matches * 0.2, 1.0)
        
        engagement = 0.5
        if video_data['viewCount'] > 0:
            engagement = min((video_data['likeCount'] + video_data['commentCount']) / video_data['viewCount'] * 100, 1.0)
        
        if category_key == 'heartwarming':
            weights = {
                'comment_validation': 0.35,
                'comment_emotional': 0.25, 
                'comment_authenticity': 0.20,
                'content_match': 0.15,
                'engagement': 0.05
            }
            base_score = 3.0
        elif category_key == 'funny':
            weights = {
                'comment_validation': 0.40,
                'comment_authenticity': 0.25,
                'comment_emotional': 0.20,
                'content_match': 0.10,
                'engagement': 0.05
            }
            base_score = 2.5
        else:  # traumatic
            weights = {
                'comment_validation': 0.35,
                'comment_emotional': 0.30,
                'comment_authenticity': 0.20,
                'content_match': 0.10,
                'engagement': 0.05
            }
            base_score = 4.0
        
        component_scores = {
            'comment_validation': comments_analysis['category_validation'],
            'comment_emotional': comments_analysis['emotional_alignment'],
            'comment_authenticity': comments_analysis['authenticity_support'],
            'content_match': content_match,
            'engagement': engagement
        }
        
        weighted_score = sum(component_scores[key] * weights[key] for key in weights) * 7.0
        final_score = base_score + weighted_score
        
        if comments_analysis['category_validation'] > 0.8:
            final_score += 1.0
        if comments_analysis['authenticity_support'] < 0.2:
            final_score *= 0.6
        
        confidence = 0.3
        if len(video_data['comments']) > 100:
            confidence += 0.3
        if len(video_data['comments']) > 500:
            confidence += 0.2
        if comments_analysis['category_validation'] > 0.6:
            confidence += 0.2
        
        return {
            'final_score': min(final_score, 10.0),
            'confidence': min(confidence, 1.0),
            'component_scores': component_scores,
            'comments_analysis': comments_analysis
        }


def main():
    st.markdown("""
    <div class="main-header">
        <h1>YouTube Collection & Rating Tool</h1>
        <p><strong>Collect YouTube videos and rate them with AI analysis</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar configuration
    with st.sidebar:
        st.header("Configuration")
        
        # Mode selection
        mode = st.radio(
            "Select Mode:",
            ["Data Collector", "Video Rater"],
            horizontal=True
        )
        
        st.subheader("API Configuration")
        youtube_api_key = st.text_input(
            "YouTube API Key", 
            type="password", 
            help="Your YouTube Data API v3 key"
        )
        
        st.subheader("Google Sheets Configuration")
        creds_input_method = st.radio(
            "Service Account JSON:",
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
                    st.success("Valid JSON")
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
                    st.success("JSON file loaded")
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
        
        # Google Sheets URL/ID
        spreadsheet_url = st.text_input(
            "Google Sheet URL",
            value="https://docs.google.com/spreadsheets/d/1PHvW-LykIpIbwKJbiGHi6NcX7hd4EsIWK3zwr4Dmvrk/",
            help="URL or ID of your Google Sheets document"
        )
        
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', spreadsheet_url)
        spreadsheet_id = match.group(1) if match else spreadsheet_url
        
        if spreadsheet_id:
            st.success(f"Sheet ID: {spreadsheet_id[:20]}...")
        
        if sheets_creds and 'client_email' in sheets_creds:
            st.info(f"Service Account: {sheets_creds['client_email'][:30]}...")
    
    # Main content based on selected mode
    if mode == "Data Collector":
        st.subheader("Data Collector")
        
        # Show status alerts prominently
        show_status_alert()
        
        with st.sidebar:
            st.subheader("Collection Settings")
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
                value=False
            )
            
            require_captions = st.checkbox(
                "Require captions",
                value=True
            )
        
        # Statistics display
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Videos Found", st.session_state.collector_stats['found'])
        with col2:
            st.metric("Videos Checked", st.session_state.collector_stats['checked'])
        with col3:
            st.metric("Videos Rejected", st.session_state.collector_stats['rejected'])
        
        # Control buttons
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("Start Collection", disabled=st.session_state.is_collecting, type="primary"):
                clear_status()  # Clear any previous status
                
                if not youtube_api_key:
                    set_status('error', "COLLECTION ABORTED: YouTube API key required")
                    st.rerun()
                elif not sheets_creds and auto_export:
                    set_status('error', "COLLECTION ABORTED: Google Sheets credentials required for auto-export")
                    st.rerun()
                else:
                    st.session_state.is_collecting = True
                    st.session_state.collector_stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0, 'has_captions': 0, 'no_captions': 0}
                    
                    try:
                        exporter = None
                        if sheets_creds:
                            try:
                                exporter = GoogleSheetsExporter(sheets_creds)
                            except Exception as e:
                                set_status('error', f"COLLECTION ABORTED: Google Sheets connection failed - {str(e)}")
                                st.session_state.is_collecting = False
                                st.rerun()
                        
                        collector = YouTubeCollector(youtube_api_key, sheets_exporter=exporter)
                        
                        quota_available = True
                        if not skip_quota_check:
                            quota_available, quota_message = collector.check_quota_available()
                            if not quota_available:
                                set_status('error', f"COLLECTION ABORTED: {quota_message}")
                                st.session_state.is_collecting = False
                                st.rerun()
                            else:
                                set_status('info', f"COLLECTION STARTED: {quota_message}")
                        else:
                            set_status('info', "COLLECTION STARTED: API quota check skipped")
                        
                        if quota_available:
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            
                            def update_progress(current, total):
                                progress = current / total
                                progress_bar.progress(progress)
                                status_text.text(f"Collecting: {current}/{total} videos")
                            
                            with st.spinner(f"Collecting {target_count} videos for {category}..."):
                                videos = collector.collect_videos(
                                    target_count=target_count,
                                    category=category,
                                    spreadsheet_id=spreadsheet_id,
                                    require_captions=require_captions,
                                    progress_callback=update_progress
                                )
                            
                            if len(videos) > 0:
                                set_status('info', f"COLLECTION COMPLETED: Found {len(videos)} videos")
                            else:
                                set_status('warning', "COLLECTION COMPLETED: No videos found")
                            
                            if auto_export and sheets_creds and videos:
                                try:
                                    collector.add_log(f"Starting auto-export of {len(videos)} videos to Google Sheets", "INFO")
                                    if not exporter:
                                        exporter = GoogleSheetsExporter(sheets_creds)
                                        collector.add_log("Initialized Google Sheets exporter", "INFO")
                                    
                                    collector.add_log(f"Attempting to export to spreadsheet ID: {spreadsheet_id}", "INFO")
                                    sheet_url = exporter.export_to_sheets(videos, spreadsheet_id=spreadsheet_id)
                                    
                                    if sheet_url:
                                        st.success("Exported to Google Sheets!")
                                        st.markdown(f"[Open Spreadsheet]({sheet_url})")
                                        collector.add_log(f"EXPORT SUCCESS: {len(videos)} videos exported to raw_links sheet", "SUCCESS")
                                        collector.add_log(f"Spreadsheet URL: {sheet_url}", "INFO")
                                        set_status('info', f"EXPORT SUCCESS: {len(videos)} videos exported to raw_links")
                                    else:
                                        collector.add_log("EXPORT FAILED: No spreadsheet URL returned", "ERROR")
                                        set_status('error', "EXPORT FAILED: No spreadsheet URL returned")
                                        
                                except Exception as e:
                                    error_msg = str(e)
                                    collector.add_log(f"EXPORT ERROR: {error_msg}", "ERROR")
                                    set_status('error', f"EXPORT FAILED: {error_msg}")
                                    
                                    # Additional error details
                                    if "authentication" in error_msg.lower():
                                        collector.add_log("Check Google Sheets service account credentials", "ERROR")
                                    elif "permission" in error_msg.lower():
                                        collector.add_log("Check if service account has write access to spreadsheet", "ERROR")
                                    elif "spreadsheet" in error_msg.lower():
                                        collector.add_log("Check if spreadsheet ID is correct and accessible", "ERROR")
                            else:
                                if not auto_export:
                                    collector.add_log("Auto-export disabled - videos collected but not exported", "INFO")
                                elif not sheets_creds:
                                    collector.add_log("No Google Sheets credentials - cannot export", "WARNING")
                                elif not videos:
                                    collector.add_log("No videos collected - nothing to export", "INFO")
                    
                    except Exception as e:
                        set_status('error', f"COLLECTION FAILED: {str(e)}")
                    finally:
                        st.session_state.is_collecting = False
                        st.rerun()
        
        with col2:
            if st.button("Stop", disabled=not st.session_state.is_collecting):
                set_status('warning', "COLLECTION STOPPED: Process terminated by user")
                st.session_state.is_collecting = False
                st.rerun()
        
        with col3:
            if st.button("Reset"):
                st.session_state.collected_videos = []
                st.session_state.collector_stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0, 'has_captions': 0, 'no_captions': 0}
                st.rerun()
        
        with col4:
            if st.button("Manual Export") and st.session_state.collected_videos:
                if not sheets_creds:
                    st.error("Please add Google Sheets credentials")
                else:
                    try:
                        exporter = GoogleSheetsExporter(sheets_creds)
                        sheet_url = exporter.export_to_sheets(
                            st.session_state.collected_videos, 
                            spreadsheet_id=spreadsheet_id
                        )
                        if sheet_url:
                            st.success("Exported to Google Sheets!")
                            st.markdown(f"[Open Spreadsheet]({sheet_url})")
                    except Exception as e:
                        st.error(f"Export failed: {str(e)}")
        
        # Display collected videos
        if st.session_state.collected_videos:
            st.subheader("Collected Videos")
            df = pd.DataFrame(st.session_state.collected_videos)
            
            st.dataframe(
                df[['title', 'category', 'view_count', 'duration_seconds', 'url']],
                use_container_width=True,
                hide_index=True
            )
    
    elif mode == "Video Rater":
        st.subheader("Video Rater")
        
        # Show status alerts prominently
        show_status_alert()
        
        # Statistics display
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Videos Rated", st.session_state.rater_stats['rated'])
        with col2:
            st.metric("Moved to tobe_links", st.session_state.rater_stats['moved_to_tobe'])
        with col3:
            st.metric("API Calls", st.session_state.rater_stats['api_calls'])
        
        if not youtube_api_key or not sheets_creds or not spreadsheet_id:
            set_status('warning', "RATING UNAVAILABLE: Missing YouTube API key, Google Sheets credentials, or spreadsheet URL")
        else:
            # Rating control
            col1, col2 = st.columns([1, 1])
            
            with col1:
                if st.button("Start Rating", disabled=st.session_state.is_rating, type="primary"):
                    clear_status()
                    set_status('info', "RATING STARTED: Processing videos from raw_links")
                    st.session_state.is_rating = True
                    st.rerun()
            
            with col2:
                if st.button("Stop Rating", disabled=not st.session_state.is_rating):
                    set_status('warning', "RATING STOPPED: Process terminated by user")
                    st.session_state.is_rating = False
                    st.rerun()
            
            if st.session_state.is_rating:
                try:
                    rater = VideoRater(youtube_api_key)
                    exporter = GoogleSheetsExporter(sheets_creds)
                    
                    # Continuous rating loop
                    while st.session_state.is_rating:
                        # Check quota before each video
                        quota_available, quota_message = rater.check_quota_available()
                        
                        if not quota_available:
                            set_status('error', f"RATING ABORTED: {quota_message}")
                            st.session_state.is_rating = False
                            st.rerun()
                            break
                        
                        # Get next video from raw_links
                        next_video = exporter.get_next_raw_video(spreadsheet_id)
                        
                        if not next_video:
                            set_status('info', "RATING COMPLETED: All videos processed - no more videos in raw_links")
                            st.session_state.is_rating = False
                            st.rerun()
                            break
                        
                        # Display video info and use category from raw_links
                        video_category = next_video.get('category', 'heartwarming')
                        
                        # Create a container for the current video being processed
                        video_container = st.container()
                        
                        with video_container:
                            st.markdown(f"### Currently Processing:")
                            st.markdown(f"**Title:** {next_video.get('title', 'Unknown Title')}")
                            st.markdown(f"**Channel:** {next_video.get('channel_title', 'Unknown')}")
                            st.markdown(f"**Category:** {video_category} {CATEGORIES.get(video_category, {}).get('emoji', '')}")
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Views", f"{int(next_video.get('view_count', 0)):,}")
                            with col2:
                                st.metric("Likes", f"{int(next_video.get('like_count', 0)):,}")
                            with col3:
                                st.metric("Comments", f"{int(next_video.get('comment_count', 0)):,}")
                        
                        # Analyze video using category from raw_links
                        with st.spinner("Analyzing video..."):
                            video_id = next_video.get('video_id')
                            if video_id:
                                try:
                                    video_data = rater.fetch_video_data(video_id)
                                    analysis = rater.calculate_category_score(video_data, video_category)
                                    
                                    # Display score
                                    score = analysis['final_score']
                                    confidence = analysis['confidence']
                                    
                                    col1, col2 = st.columns([2, 1])
                                    
                                    with col2:
                                        st.markdown(f"""
                                        <div class="score-card">
                                            <h2>Score</h2>
                                            <h1 style="font-size: 3rem;">{score:.1f}/10</h1>
                                            <p>Confidence: {confidence:.0%}</p>
                                        </div>
                                        """, unsafe_allow_html=True)
                                    
                                    with col1:
                                        # Display timestamped moments
                                        moments = analysis['comments_analysis'].get('timestamped_moments', [])
                                        if moments:
                                            st.subheader("Timestamped Moments")
                                            for moment in moments[:5]:
                                                st.markdown(f"""
                                                <div class="timestamp-moment">
                                                    <strong>{moment['timestamp']}</strong><br>
                                                    <em>"{moment['comment'][:100]}{'...' if len(moment['comment']) > 100 else ''}"</em>
                                                </div>
                                                """, unsafe_allow_html=True)
                                        else:
                                            st.info("No timestamped moments found")
                                    
                                    # Process the video automatically
                                    video_url = next_video.get('url', '')
                                    
                                    # Always add to discarded first (before deletion)
                                    if video_url:
                                        exporter.add_to_discarded(spreadsheet_id, video_url)
                                    
                                    # Then delete from raw_links
                                    exporter.delete_raw_video(spreadsheet_id, next_video['row_number'])
                                    
                                    # If score >= 6.5, add to tobe_links AND time_comments
                                    if score >= 6.5:
                                        exporter.add_to_tobe_links(spreadsheet_id, next_video, analysis)
                                        
                                        # Also add time_comments for qualifying videos
                                        exporter.add_time_comments(
                                            spreadsheet_id, 
                                            video_id, 
                                            video_url, 
                                            analysis['comments_analysis']
                                        )
                                        
                                        st.session_state.rater_stats['moved_to_tobe'] += 1
                                        st.success(f"✅ Score: {score:.1f}/10 - Moved to tobe_links!")
                                        rater.add_log(f"Video {next_video.get('title', '')[:50]} scored {score:.1f} - moved to tobe_links and time_comments", "SUCCESS")
                                    else:
                                        st.info(f"ℹ️ Score: {score:.1f}/10 - Below threshold, removed from raw_links.")
                                        rater.add_log(f"Video {next_video.get('title', '')[:50]} scored {score:.1f} - removed", "INFO")
                                    
                                    # Log the discarded action
                                    rater.add_log(f"Added URL to discarded: {video_url}", "INFO")
                                    
                                    st.session_state.rater_stats['rated'] += 1
                                    
                                    # Brief pause before next video
                                    time.sleep(2)
                                    
                                    # Clear the container for next video
                                    video_container.empty()
                                
                                except Exception as e:
                                    set_status('error', f"RATING ERROR: Failed to analyze video - {str(e)}")
                                    rater.add_log(f"Error analyzing video: {str(e)}", "ERROR")
                                    
                                    # Still add to discarded and delete to avoid infinite loop
                                    video_url = next_video.get('url', '')
                                    if video_url:
                                        exporter.add_to_discarded(spreadsheet_id, video_url)
                                        rater.add_log(f"Added failed video URL to discarded: {video_url}", "INFO")
                                    
                                    exporter.delete_raw_video(spreadsheet_id, next_video['row_number'])
                                    time.sleep(1)
                            else:
                                set_status('error', "RATING ERROR: Video has no ID - skipping")
                                
                                # Still add to discarded and delete to avoid reprocessing
                                video_url = next_video.get('url', '')
                                if video_url:
                                    exporter.add_to_discarded(spreadsheet_id, video_url)
                                    rater.add_log(f"Added video with missing ID to discarded: {video_url}", "INFO")
                                
                                exporter.delete_raw_video(spreadsheet_id, next_video['row_number'])
                        
                        # Small delay and rerun to continue the loop
                        if st.session_state.is_rating:  # Check if still rating
                            time.sleep(0.5)
                            st.rerun()
                
                except Exception as e:
                    set_status('error', f"RATING SYSTEM FAILURE: {str(e)}")
                    st.session_state.is_rating = False
    
    # Activity log
    with st.expander("Activity Log", expanded=False):
        if st.session_state.logs:
            for log in st.session_state.logs[-20:]:  # Show last 20 entries
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
