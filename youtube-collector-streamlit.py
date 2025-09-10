"""
Optimized YouTube Data Collector & Video Rating Tool
With pre-filtered search, pagination, and reduced redundancy
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
    st.session_state.collector_stats = {'checked': 0, 'found': 0, 'rejected': 0, 'search_calls': 0, 'detail_calls': 0, 'has_captions': 0, 'no_captions': 0}
if 'rater_stats' not in st.session_state:
    st.session_state.rater_stats = {'rated': 0, 'moved_to_tobe': 0, 'rejected': 0, 'api_calls': 0}
if 'logs' not in st.session_state:
    st.session_state.logs = []
if 'used_queries' not in st.session_state:
    st.session_state.used_queries = set()
if 'analysis_history' not in st.session_state:
    st.session_state.analysis_history = []

# YouTube video categories (stable list)
YOUTUBE_CATEGORIES = {
    "0": "All Categories",
    "1": "Film & Animation",
    "2": "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "19": "Travel & Events",
    "20": "Gaming",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
}

REGION_CODES = {
    "": "All Regions",
    "US": "United States",
    "GB": "United Kingdom",
    "CA": "Canada",
    "AU": "Australia",
    "DE": "Germany",
    "FR": "France",
    "JP": "Japan",
    "KR": "South Korea",
    "BR": "Brazil",
    "IN": "India",
    "MX": "Mexico",
    "ES": "Spain",
    "IT": "Italy",
    "NL": "Netherlands",
    "PL": "Poland",
    "RU": "Russia",
    "SE": "Sweden",
    "TR": "Turkey",
    "AR": "Argentina",
    "ZA": "South Africa"
}

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

class GoogleSheetsRateLimiter:
    """Session-state based rate limiter for Google Sheets API calls"""
    
    def __init__(self, min_delay=1.5, max_requests_per_100s=80):
        self.min_delay = min_delay
        self.max_requests_per_100s = max_requests_per_100s
        
        if 'sheets_api_timestamps' not in st.session_state:
            st.session_state.sheets_api_timestamps = []
        if 'sheets_last_request' not in st.session_state:
            st.session_state.sheets_last_request = 0
        if 'sheets_api_call_count' not in st.session_state:
            st.session_state.sheets_api_call_count = 0
        
    def wait_if_needed(self, show_status=False):
        """Wait if necessary to avoid rate limits using session state"""
        current_time = time.time()
        
        st.session_state.sheets_api_timestamps = [
            t for t in st.session_state.sheets_api_timestamps 
            if current_time - t < 100
        ]
        
        delay_needed = 0
        reason = ""
        
        time_since_last = current_time - st.session_state.sheets_last_request
        if time_since_last < self.min_delay:
            delay_needed = self.min_delay - time_since_last
            reason = "minimum spacing"
        
        if len(st.session_state.sheets_api_timestamps) >= self.max_requests_per_100s:
            oldest_request = st.session_state.sheets_api_timestamps[0]
            wait_for_window = 100 - (current_time - oldest_request) + 1
            if wait_for_window > delay_needed:
                delay_needed = wait_for_window
                reason = f"rate limit ({len(st.session_state.sheets_api_timestamps)}/{self.max_requests_per_100s} requests in window)"
        
        if delay_needed > 0:
            if show_status:
                with st.spinner(f"Rate limiting: waiting {delay_needed:.1f}s ({reason})..."):
                    time.sleep(delay_needed)
            else:
                time.sleep(delay_needed)
            current_time = time.time()
        
        st.session_state.sheets_last_request = current_time
        st.session_state.sheets_api_timestamps.append(current_time)
        st.session_state.sheets_api_call_count += 1


class GoogleSheetsExporter:
    """Handle Google Sheets export and import with session-state based rate limiting"""
    
    def __init__(self, credentials_dict: Dict):
        self.creds = Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets',
                   'https://www.googleapis.com/auth/drive']
        )
        self.client = gspread.authorize(self.creds)
        self.rate_limiter = GoogleSheetsRateLimiter(min_delay=1.5)
        
        if 'sheets_api_stats' not in st.session_state:
            st.session_state.sheets_api_stats = {
                'total_calls': 0,
                'last_call_time': 0,
                'calls_in_last_100s': 0
            }
    
    def get_spreadsheet_by_id(self, spreadsheet_id: str):
        """Get spreadsheet by ID with rate limiting"""
        self.rate_limiter.wait_if_needed()
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            return spreadsheet
        except Exception as e:
            raise e
    
    def get_next_raw_video(self, spreadsheet_id: str) -> Optional[Dict]:
        """Get next video from raw_links sheet with rate limiting"""
        try:
            self.rate_limiter.wait_if_needed(show_status=True)
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            self.rate_limiter.wait_if_needed()
            worksheet = spreadsheet.worksheet("raw_links")
            
            self.rate_limiter.wait_if_needed()
            all_values = worksheet.get_all_values()
            
            if len(all_values) > 1:
                headers = all_values[0]
                first_row = all_values[1]
                
                video_data = {headers[i]: first_row[i] for i in range(len(headers))}
                video_data['row_number'] = 2
                return video_data
            return None
        except Exception as e:
            st.error(f"Error fetching next video: {str(e)}")
            if "quota" in str(e).lower() or "rate" in str(e).lower():
                st.warning("Rate limit hit - increasing delays...")
                self.rate_limiter.min_delay = min(self.rate_limiter.min_delay * 1.5, 5.0)
            return None
    
    def delete_raw_video(self, spreadsheet_id: str, row_number: int):
        """Delete video from raw_links sheet with rate limiting"""
        try:
            self.rate_limiter.wait_if_needed()
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            self.rate_limiter.wait_if_needed()
            worksheet = spreadsheet.worksheet("raw_links")
            
            self.rate_limiter.wait_if_needed()
            worksheet.delete_rows(row_number)
        except Exception as e:
            st.error(f"Error deleting video: {str(e)}")
    
    def add_to_tobe_links(self, spreadsheet_id: str, video_data: Dict, analysis_data: Dict):
        """Add video to tobe_links sheet with analysis data and rate limiting"""
        try:
            self.rate_limiter.wait_if_needed()
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            try:
                self.rate_limiter.wait_if_needed()
                worksheet = spreadsheet.worksheet("tobe_links")
            except gspread.exceptions.WorksheetNotFound:
                self.rate_limiter.wait_if_needed()
                worksheet = spreadsheet.add_worksheet(title="tobe_links", rows=1000, cols=25)
                
                headers = [
                    'video_id', 'title', 'url', 'category', 'search_query', 
                    'duration_seconds', 'view_count', 'like_count', 'comment_count',
                    'published_at', 'channel_title', 'tags', 'collected_at',
                    'score', 'confidence', 'timestamped_moments', 'category_validation',
                    'analysis_timestamp'
                ]
                self.rate_limiter.wait_if_needed()
                worksheet.append_row(headers)
            
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
            
            self.rate_limiter.wait_if_needed()
            worksheet.append_row(row_data)
        except Exception as e:
            st.error(f"Error adding to tobe_links: {str(e)}")
    
    def add_to_discarded(self, spreadsheet_id: str, video_url: str):
        """Add video URL to discarded table with rate limiting"""
        try:
            self.rate_limiter.wait_if_needed()
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            try:
                self.rate_limiter.wait_if_needed()
                worksheet = spreadsheet.worksheet("discarded")
            except gspread.exceptions.WorksheetNotFound:
                self.rate_limiter.wait_if_needed()
                worksheet = spreadsheet.add_worksheet(title="discarded", rows=1000, cols=1)
                self.rate_limiter.wait_if_needed()
                worksheet.append_row(['url'])
            
            self.rate_limiter.wait_if_needed()
            worksheet.append_row([video_url])
        except Exception as e:
            st.error(f"Error adding to discarded: {str(e)}")
    
    def load_discarded_urls(self, spreadsheet_id: str) -> set:
        """Load existing URLs from discarded sheet with rate limiting"""
        try:
            self.rate_limiter.wait_if_needed()
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            try:
                self.rate_limiter.wait_if_needed()
                worksheet = spreadsheet.worksheet("discarded")
                self.rate_limiter.wait_if_needed()
                all_values = worksheet.get_all_values()
                
                if len(all_values) > 1:
                    discarded_urls = {row[0] for row in all_values[1:] if row and row[0]}
                    return discarded_urls
            except gspread.exceptions.WorksheetNotFound:
                pass
            return set()
        except Exception as e:
            st.error(f"Error loading discarded URLs: {str(e)}")
            return set()
    
    def add_time_comments(self, spreadsheet_id: str, video_id: str, video_url: str, comments_analysis: Dict):
        """Add timestamped and category-matched comments to time_comments table with rate limiting"""
        try:
            self.rate_limiter.wait_if_needed()
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            try:
                self.rate_limiter.wait_if_needed()
                worksheet = spreadsheet.worksheet("time_comments")
            except gspread.exceptions.WorksheetNotFound:
                self.rate_limiter.wait_if_needed()
                worksheet = spreadsheet.add_worksheet(title="time_comments", rows=1000, cols=10)
                
                headers = [
                    'video_id', 'video_url', 'comment_text', 'timestamp', 
                    'category_matched', 'relevance_score', 'sentiment'
                ]
                self.rate_limiter.wait_if_needed()
                worksheet.append_row(headers)
            
            moments = comments_analysis.get('timestamped_moments', [])
            
            rows_to_add = []
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
                rows_to_add.append(row_data)
            
            if rows_to_add:
                for row in rows_to_add:
                    self.rate_limiter.wait_if_needed()
                    worksheet.append_row(row)
                
        except Exception as e:
            st.error(f"Error adding to time_comments: {str(e)}")
    
    def export_to_sheets(self, videos: List[Dict], spreadsheet_id: str = None, spreadsheet_name: str = "YouTube_Collection_Data"):
        """Export videos to raw_links sheet with rate limiting"""
        try:
            if spreadsheet_id:
                self.rate_limiter.wait_if_needed()
                spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            else:
                try:
                    self.rate_limiter.wait_if_needed()
                    spreadsheet = self.client.open(spreadsheet_name)
                except gspread.exceptions.SpreadsheetNotFound:
                    self.rate_limiter.wait_if_needed()
                    spreadsheet = self.client.create(spreadsheet_name)
            
            worksheet_name = "raw_links"
            
            try:
                self.rate_limiter.wait_if_needed()
                worksheet = spreadsheet.worksheet(worksheet_name)
            except gspread.exceptions.WorksheetNotFound:
                self.rate_limiter.wait_if_needed()
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
            
            if videos:
                df = pd.DataFrame(videos)
                self.rate_limiter.wait_if_needed()
                existing_data = worksheet.get_all_values()
                
                if existing_data and len(existing_data) > 1:
                    for _, row in df.iterrows():
                        values = [str(v) if pd.notna(v) else '' for v in row.tolist()]
                        self.rate_limiter.wait_if_needed()
                        worksheet.append_row(values)
                else:
                    self.rate_limiter.wait_if_needed()
                    worksheet.clear()
                    headers = list(df.columns)
                    self.rate_limiter.wait_if_needed()
                    worksheet.append_row(headers)
                    for _, row in df.iterrows():
                        values = [str(v) if pd.notna(v) else '' for v in row.tolist()]
                        self.rate_limiter.wait_if_needed()
                        worksheet.append_row(values)
                
                return spreadsheet.url
            
            return None
        except Exception as e:
            st.error(f"Error exporting to sheets: {str(e)}")
            raise e
    
    def load_existing_sheet_ids(self, spreadsheet_id: str) -> set:
        """Load existing video IDs from Google Sheet"""
        try:
            self.rate_limiter.wait_if_needed()
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            self.rate_limiter.wait_if_needed()
            worksheet = spreadsheet.worksheet("raw_links")
            self.rate_limiter.wait_if_needed()
            all_values = worksheet.get_all_values()
            
            if len(all_values) > 1:
                headers = all_values[0]
                video_id_index = headers.index('video_id') if 'video_id' in headers else 0
                existing_ids = {row[video_id_index] for row in all_values[1:] if len(row) > video_id_index and row[video_id_index]}
                return existing_ids
            return set()
        except Exception as e:
            return set()
    
    def load_used_queries(self, spreadsheet_id: str) -> set:
        """Load previously used queries from Google Sheet"""
        try:
            self.rate_limiter.wait_if_needed()
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            try:
                self.rate_limiter.wait_if_needed()
                worksheet = spreadsheet.worksheet("used_queries")
                self.rate_limiter.wait_if_needed()
                all_values = worksheet.get_all_values()
                
                if len(all_values) > 1:
                    used_queries = {row[0] for row in all_values[1:] if row and row[0]}
                    return used_queries
            except gspread.exceptions.WorksheetNotFound:
                self.rate_limiter.wait_if_needed()
                worksheet = spreadsheet.add_worksheet(title="used_queries", rows=1000, cols=5)
                self.rate_limiter.wait_if_needed()
                worksheet.append_row(['query', 'category', 'timestamp', 'videos_found', 'session_id'])
            return set()
        except Exception as e:
            return set()
    
    def save_used_query(self, spreadsheet_id: str, query: str, category: str, videos_found: int):
        """Save used query to Google Sheet"""
        try:
            self.rate_limiter.wait_if_needed()
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            self.rate_limiter.wait_if_needed()
            worksheet = spreadsheet.worksheet("used_queries")
            self.rate_limiter.wait_if_needed()
            worksheet.append_row([
                query,
                category,
                datetime.now().isoformat(),
                videos_found,
                st.session_state.get('session_id', 'manual')
            ])
        except Exception as e:
            pass


class YouTubeCollector:
    """Optimized YouTube video collection with pre-filtering and pagination"""
    
    def __init__(self, api_key: str, sheets_exporter=None):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.sheets_exporter = sheets_exporter
        self.existing_sheet_ids = set()
        self.existing_queries = set()
        self.discarded_urls = set()
        
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
    
    def add_log(self, message: str, log_type: str = "INFO"):
        """Add a detailed log entry"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] COLLECTOR {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]
    
    def check_quota_available(self) -> Tuple[bool, str]:
        """Check if YouTube API quota is available"""
        try:
            self.add_log("Checking API quota availability...", "INFO")
            test_request = self.youtube.videos().list(
                part='id',
                id='YbJOTdZBX1g'
            )
            response = test_request.execute()
            st.session_state.collector_stats['detail_calls'] += 1
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
    
    def search_videos(self, query: str, max_results: int = 50, page_token: str = None, 
                     region_code: str = None, category_id: str = None) -> Tuple[List[Dict], str]:
        """
        Search for videos with pre-filtering in the API query
        Returns: (items, nextPageToken)
        """
        try:
            st.session_state.collector_stats['search_calls'] += 1
            
            # Apply time filter - videos from last 6 months
            six_months_ago = (datetime.now() - timedelta(days=180)).isoformat() + 'Z'
            
            # Build the search query with exclusions
            excluded_terms = [
                '-shorts', '-#shorts', '-#short',
                '-"music video"', '-"official video"', '-"lyric video"', 
                '-"official audio"', '-compilation', '-"best of"',
                '-"top 10"', '-"top 20"', '-montage'
            ]
            
            filtered_query = f'{query} {" ".join(excluded_terms)}'
            
            # Build request parameters
            params = {
                'part': 'id,snippet',
                'q': filtered_query,
                'type': 'video',
                'maxResults': min(max_results, 50),  # API limit is 50 per page
                'order': 'relevance',
                'publishedAfter': six_months_ago,
                'videoDuration': 'medium',  # 4-20 minutes (excludes shorts)
                'videoEmbeddable': 'any',  # Changed from 'true' to get more results
                'relevanceLanguage': 'en',
                'safeSearch': 'none'
            }
            
            # Add optional parameters
            if page_token:
                params['pageToken'] = page_token
            if region_code and region_code != "":
                params['regionCode'] = region_code
            if category_id and category_id != "0":
                params['videoCategoryId'] = category_id
            
            request = self.youtube.search().list(**params)
            response = request.execute()
            
            items = response.get('items', [])
            next_page_token = response.get('nextPageToken', None)
            
            # Quick pre-filter based on snippet data (no extra API calls)
            filtered_items = []
            for item in items:
                title = item['snippet']['title'].lower()
                
                # Quick checks that don't require additional API calls
                skip = False
                
                unwanted = ['#shorts', 'compilation', 'top 10', 'top 20', 
                           'every time', 'all moments', 'best of', 'music video',
                           'official video', 'lyric', 'audio only']
                
                for word in unwanted:
                    if word in title:
                        skip = True
                        break
                
                if not skip:
                    filtered_items.append(item)
            
            self.add_log(f"Search returned {len(items)} items, {len(filtered_items)} after pre-filter", "INFO")
            return filtered_items, next_page_token
            
        except HttpError as e:
            self.add_log(f"API Error during search: {str(e)}", "ERROR")
            return [], None
    
    def get_video_details(self, video_id: str) -> Optional[Dict]:
        """Get detailed information about a video"""
        try:
            st.session_state.collector_stats['detail_calls'] += 1
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
    
    def validate_video_optimized(self, search_item: Dict, target_category: str, 
                                require_captions: bool = True) -> Tuple[bool, any]:
        """Optimized validation that leverages pre-filtering"""
        video_id = search_item['id']['videoId']
        video_url = f"https://youtube.com/watch?v={video_id}"
        title = search_item['snippet']['title']
        
        # Quick duplicate checks first (no API call)
        existing_ids = [v['video_id'] for v in st.session_state.collected_videos]
        if video_id in existing_ids or video_id in self.existing_sheet_ids:
            return False, "Duplicate video"
        
        if video_url in self.discarded_urls:
            return False, "Already processed"
        
        # Get details (API call) - only for non-duplicates
        details = self.get_video_details(video_id)
        if not details:
            return False, "Could not fetch details"
        
        # Caption check
        if require_captions:
            has_captions = self.check_caption_availability(details)
            if not has_captions:
                return False, "No captions available"
        else:
            self.check_caption_availability(details)
        
        # Duration check (already filtered by API but double-check)
        duration = isodate.parse_duration(details['contentDetails']['duration'])
        duration_seconds = duration.total_seconds()
        
        if duration_seconds < 90:
            return False, f"Video too short ({duration_seconds}s < 90s)"
        
        # View count check
        view_count = int(details['statistics'].get('viewCount', 0))
        if view_count < 10000:
            return False, f"View count too low ({view_count} < 10,000)"
        
        # Category relevance check
        title_desc_text = (title.lower() + ' ' + details['snippet'].get('description', '')).lower()
        
        category_keywords = {
            'heartwarming': ['heartwarming', 'touching', 'emotional', 'reunion', 'surprise', 'family', 'love', 
                           'soldier', 'homecoming', 'dog reunion', 'acts kindness', 'baby first time', 
                           'proposal reaction', 'homeless helped', 'teacher surprised', 'saving animal'],
            'funny': ['funny', 'comedy', 'humor', 'hilarious', 'joke', 'laugh', 'entertaining', 'fails', 
                     'epic fail', 'instant karma', 'prank', 'bloopers', 'comedy gold', 'dad jokes'],
            'traumatic': ['accident', 'tragedy', 'disaster', 'emergency', 'breaking news', 'shocking',
                        'dramatic rescue', 'natural disaster', 'police chase', 'survival story', 'near death',
                        'extreme weather', 'earthquake', 'tornado', 'avalanche', 'explosion']
        }
        
        keywords = category_keywords.get(target_category, [])
        matched_keywords = [kw for kw in keywords if kw in title_desc_text]
        
        if not matched_keywords:
            return False, f"No {target_category} keywords found"
        
        self.add_log(f"✓ Validated: {title[:50]}... - Keywords: {', '.join(matched_keywords[:3])}", "SUCCESS")
        
        return True, details
    
    def collect_videos_with_pagination(self, target_count: int, category: str, 
                                      spreadsheet_id: str = None, require_captions: bool = True,
                                      region_code: str = None, category_id: str = None,
                                      progress_callback=None):
        """Enhanced collection with pagination support"""
        collected = []
        
        if category == 'mixed':
            categories = ['heartwarming', 'funny', 'traumatic']
        else:
            categories = [category]
        
        self.add_log(f"Starting collection with pagination for: {category}", "INFO")
        
        # Load existing data
        if spreadsheet_id and self.sheets_exporter:
            self.existing_sheet_ids = self.sheets_exporter.load_existing_sheet_ids(spreadsheet_id)
            self.discarded_urls = self.sheets_exporter.load_discarded_urls(spreadsheet_id)
            self.existing_queries = self.sheets_exporter.load_used_queries(spreadsheet_id)
            st.session_state.used_queries.update(self.existing_queries)
            self.add_log(f"Loaded {len(self.existing_sheet_ids)} existing IDs, {len(self.discarded_urls)} discarded URLs", "INFO")
        
        category_index = 0
        attempts = 0
        max_attempts = 30
        videos_checked_ids = set()
        
        while len(collected) < target_count and attempts < max_attempts:
            current_category = categories[category_index % len(categories)]
            
            # Get available queries
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
            self.add_log(f"Searching '{current_category}': {query}", "INFO")
            
            # Pagination loop for current query
            page_token = None
            pages_fetched = 0
            max_pages = 3  # Fetch up to 3 pages (150 results) per query
            
            while pages_fetched < max_pages and len(collected) < target_count:
                # Get search results with pagination
                search_results, next_page_token = self.search_videos(
                    query, 
                    max_results=50,
                    page_token=page_token,
                    region_code=region_code,
                    category_id=category_id
                )
                
                if not search_results:
                    break
                
                pages_fetched += 1
                self.add_log(f"Processing page {pages_fetched} of results ({len(search_results)} items)", "INFO")
                
                videos_found_this_page = 0
                
                for item in search_results:
                    if len(collected) >= target_count:
                        break
                    
                    video_id = item['id']['videoId']
                    
                    if video_id in videos_checked_ids:
                        continue
                    
                    videos_checked_ids.add(video_id)
                    st.session_state.collector_stats['checked'] += 1
                    
                    # Validate video (optimized version)
                    result = self.validate_video_optimized(item, current_category, require_captions)
                    
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
                            'collected_at': datetime.now().isoformat(),
                            'page_number': pages_fetched,
                            'region_code': region_code or 'ALL',
                            'category_filter': category_id or '0'
                        }
                        
                        collected.append(video_record)
                        st.session_state.collected_videos.append(video_record)
                        st.session_state.collector_stats['found'] += 1
                        videos_found_this_page += 1
                        
                        self.add_log(f"✅ ADDED: {video_record['title'][:30]}... (page {pages_fetched})", "SUCCESS")
                        
                        if progress_callback:
                            progress_callback(len(collected), target_count)
                    else:
                        st.session_state.collector_stats['rejected'] += 1
                    
                    time.sleep(0.2)
                
                # Check if we should fetch next page
                if next_page_token and videos_found_this_page > 0:
                    page_token = next_page_token
                    self.add_log(f"Found {videos_found_this_page} videos on page {pages_fetched}, fetching next page...", "INFO")
                    time.sleep(1)
                else:
                    break
            
            # Save used query
            if spreadsheet_id and self.sheets_exporter:
                self.sheets_exporter.save_used_query(spreadsheet_id, query, current_category, 
                                                    sum(1 for v in collected if v.get('search_query') == query))
            
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
        st.session_state.logs = st.session_state.logs[:100]
    
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
                        timestamp_str = ':'.join(filter(None, timestamp_match))
                        if not timestamp_str:
                            continue
                            
                        time_parts = timestamp_str.split(':')
                        if len(time_parts) == 2:
                            try:
                                seconds = int(time_parts[0]) * 60 + int(time_parts[1])
                            except ValueError:
                                continue
                        else:
                            continue
                        
                        moments.append({
                            'timestamp': timestamp_str,
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
        <p><strong>Optimized collector with pre-filtered search and pagination</strong></p>
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
        
        # Add rate limit status display
        if 'sheets_api_call_count' in st.session_state:
            st.subheader("API Rate Limit Status")
            
            # Calculate current rate
            current_time = time.time()
            recent_calls = len([t for t in st.session_state.get('sheets_api_timestamps', []) 
                              if current_time - t < 100])
            
            # Display metrics
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total API Calls", st.session_state.sheets_api_call_count)
            with col2:
                color = "🟢" if recent_calls < 60 else "🟡" if recent_calls < 75 else "🔴"
                st.metric(f"{color} Calls (last 100s)", f"{recent_calls}/80")
            
            if recent_calls >= 75:
                st.warning("Approaching rate limit - automatic delays active")
    
    # Main content based on selected mode
    if mode == "Data Collector":
        st.subheader("Data Collector")
        
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
            
            # NEW: Region and Category filters
            st.subheader("Search Filters")
            
            region_code = st.selectbox(
                "Region Filter",
                options=list(REGION_CODES.keys()),
                format_func=lambda x: REGION_CODES[x],
                help="Filter results by country/region"
            )
            
            category_id = st.selectbox(
                "YouTube Category",
                options=list(YOUTUBE_CATEGORIES.keys()),
                format_func=lambda x: YOUTUBE_CATEGORIES[x],
                help="Filter by YouTube's content categories"
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
            
            # Display quota cost estimate
            st.subheader("Quota Usage Estimate")
            estimated_searches = min(target_count // 3, 10)  # Rough estimate
            estimated_details = target_count * 2  # Assuming 50% pass rate
            estimated_cost = (estimated_searches * 100) + (estimated_details * 1)
            st.info(f"Estimated quota cost: ~{estimated_cost} units\n"
                   f"(Search: {estimated_searches}×100 = {estimated_searches*100} units)\n"
                   f"(Details: ~{estimated_details}×1 = {estimated_details} units)")
        
        # Statistics display
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Videos Found", st.session_state.collector_stats['found'])
        with col2:
            st.metric("Videos Checked", st.session_state.collector_stats['checked'])
        with col3:
            st.metric("Search Calls", f"{st.session_state.collector_stats['search_calls']} ({st.session_state.collector_stats['search_calls']*100} units)")
        with col4:
            st.metric("Detail Calls", st.session_state.collector_stats['detail_calls'])
        
        # Control buttons
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("Start Collection", disabled=st.session_state.is_collecting, type="primary"):
                if not youtube_api_key:
                    st.error("Please enter your YouTube API key")
                else:
                    st.session_state.is_collecting = True
                    st.session_state.collector_stats = {'checked': 0, 'found': 0, 'rejected': 0, 'search_calls': 0, 'detail_calls': 0, 'has_captions': 0, 'no_captions': 0}
                    
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
                                st.error(f"Cannot start collection: {quota_message}")
                                st.session_state.is_collecting = False
                            else:
                                st.success(f"{quota_message}")
                        
                        if quota_available:
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            
                            def update_progress(current, total):
                                progress = current / total
                                progress_bar.progress(progress)
                                status_text.text(f"Collecting: {current}/{total} videos | Search calls: {st.session_state.collector_stats['search_calls']} | Detail calls: {st.session_state.collector_stats['detail_calls']}")
                            
                            with st.spinner(f"Collecting {target_count} videos for {category} with pagination..."):
                                videos = collector.collect_videos_with_pagination(
                                    target_count=target_count,
                                    category=category,
                                    spreadsheet_id=spreadsheet_id,
                                    require_captions=require_captions,
                                    region_code=region_code if region_code else None,
                                    category_id=category_id if category_id != "0" else None,
                                    progress_callback=update_progress
                                )
                            
                            st.success(f"Collection complete! Found {len(videos)} videos.")
                            st.info(f"Total API usage: {st.session_state.collector_stats['search_calls']*100 + st.session_state.collector_stats['detail_calls']} units")
                            
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
                                        collector.add_log(f"✅ EXPORT SUCCESS: {len(videos)} videos exported to raw_links sheet", "SUCCESS")
                                        collector.add_log(f"Spreadsheet URL: {sheet_url}", "INFO")
                                    else:
                                        collector.add_log("❌ EXPORT FAILED: No spreadsheet URL returned", "ERROR")
                                        st.error("Export completed but no URL returned")
                                        
                                except Exception as e:
                                    error_msg = str(e)
                                    collector.add_log(f"❌ EXPORT ERROR: {error_msg}", "ERROR")
                                    st.error(f"Export failed: {error_msg}")
                                    
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
                        st.error(f"Collection error: {str(e)}")
                    finally:
                        st.session_state.is_collecting = False
                        st.rerun()
        
        with col2:
            if st.button("Stop", disabled=not st.session_state.is_collecting):
                st.session_state.is_collecting = False
                st.rerun()
        
        with col3:
            if st.button("Reset"):
                st.session_state.collected_videos = []
                st.session_state.collector_stats = {'checked': 0, 'found': 0, 'rejected': 0, 'search_calls': 0, 'detail_calls': 0, 'has_captions': 0, 'no_captions': 0}
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
            
            # Show relevant columns including new filter data
            display_columns = ['title', 'category', 'view_count', 'duration_seconds', 'page_number', 'region_code', 'url']
            available_columns = [col for col in display_columns if col in df.columns]
            
            st.dataframe(
                df[available_columns],
                use_container_width=True,
                hide_index=True
            )
    
    elif mode == "Video Rater":
        st.subheader("Video Rater")
        
        # Statistics display
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Videos Rated", st.session_state.rater_stats['rated'])
        with col2:
            st.metric("Moved to tobe_links", st.session_state.rater_stats['moved_to_tobe'])
        with col3:
            st.metric("API Calls", st.session_state.rater_stats['api_calls'])
        
        if not youtube_api_key or not sheets_creds or not spreadsheet_id:
            st.warning("Please configure YouTube API key, Google Sheets credentials, and spreadsheet URL in the sidebar.")
        else:
            # Processing delay configuration
            processing_delay = 2.0
            if 'processing_delay' not in st.session_state:
                st.session_state.processing_delay = 2.0
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                if st.button("Start Rating", disabled=st.session_state.is_rating, type="primary"):
                    st.session_state.is_rating = True
                    st.rerun()
            
            with col2:
                if st.button("Stop Rating", disabled=not st.session_state.is_rating):
                    st.session_state.is_rating = False
                    st.rerun()
            
            if st.session_state.is_rating:
                try:
                    rater = VideoRater(youtube_api_key)
                    exporter = GoogleSheetsExporter(sheets_creds)
                    
                    while st.session_state.is_rating:
                        quota_available, quota_message = rater.check_quota_available()
                        
                        if not quota_available:
                            st.error(f"Stopping rating: {quota_message}")
                            st.session_state.is_rating = False
                            break
                        
                        next_video = exporter.get_next_raw_video(spreadsheet_id)
                        
                        if not next_video:
                            st.success("All videos have been processed! No more videos in raw_links.")
                            st.session_state.is_rating = False
                            break
                        
                        video_category = next_video.get('category', 'heartwarming')
                        
                        video_container = st.container()
                        
                        with video_container:
                            st.markdown(f"### Currently Processing:")
                            st.markdown(f"**Title:** {next_video.get('title', 'Unknown Title')}")
                            st.markdown(f"**Channel:** {next_video.get('channel_title', 'Unknown')}")
                            st.markdown(f"**Category:** {video_category} {CATEGORIES.get(video_category, {}).get('emoji', '')}")
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Views", f"{int(float(next_video.get('view_count', 0))):,}")
                            with col2:
                                st.metric("Likes", f"{int(float(next_video.get('like_count', 0))):,}")
                            with col3:
                                st.metric("Comments", f"{int(float(next_video.get('comment_count', 0))):,}")
                        
                        with st.spinner("Analyzing video..."):
                            video_id = next_video.get('video_id')
                            if video_id:
                                try:
                                    video_data = rater.fetch_video_data(video_id)
                                    analysis = rater.calculate_category_score(video_data, video_category)
                                    
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
                                    
                                    video_url = next_video.get('url', '')
                                    
                                    if video_url:
                                        exporter.add_to_discarded(spreadsheet_id, video_url)
                                    
                                    exporter.delete_raw_video(spreadsheet_id, next_video['row_number'])
                                    
                                    if score >= 6.5:
                                        exporter.add_to_tobe_links(spreadsheet_id, next_video, analysis)
                                        
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
                                    
                                    rater.add_log(f"Added URL to discarded: {video_url}", "INFO")
                                    
                                    st.session_state.rater_stats['rated'] += 1
                                    
                                    time.sleep(processing_delay)
                                    
                                    video_container.empty()
                                
                                except Exception as e:
                                    st.error(f"Error analyzing video: {str(e)}")
                                    rater.add_log(f"Error analyzing video: {str(e)}", "ERROR")
                                    
                                    video_url = next_video.get('url', '')
                                    if video_url:
                                        exporter.add_to_discarded(spreadsheet_id, video_url)
                                        rater.add_log(f"Added failed video URL to discarded: {video_url}", "INFO")
                                    
                                    exporter.delete_raw_video(spreadsheet_id, next_video['row_number'])
                                    time.sleep(processing_delay)
                            else:
                                st.error("No video ID found")
                                
                                video_url = next_video.get('url', '')
                                if video_url:
                                    exporter.add_to_discarded(spreadsheet_id, video_url)
                                    rater.add_log(f"Added video with missing ID to discarded: {video_url}", "INFO")
                                
                                exporter.delete_raw_video(spreadsheet_id, next_video['row_number'])
                        
                        if st.session_state.is_rating:
                            time.sleep(0.5)
                            st.rerun()
                
                except Exception as e:
                    st.error(f"Rating error: {str(e)}")
                    st.session_state.is_rating = False
    
    # Activity log
    with st.expander("Activity Log", expanded=False):
        if st.session_state.logs:
            for log in st.session_state.logs[-20:]:
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
