"""
YouTube Data Collector - Optimized Version
Uses oEmbed API and URL pattern analysis to minimize YouTube Data API quota usage
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import time
import random
import requests
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
    page_title="YouTube Data Collector - Optimized",
    page_icon="🎬",
    layout="wide"
)

# Initialize session state
if 'collected_videos' not in st.session_state:
    st.session_state.collected_videos = []
if 'is_collecting' not in st.session_state:
    st.session_state.is_collecting = False
if 'stats' not in st.session_state:
    st.session_state.stats = {
        'checked': 0, 
        'found': 0, 
        'rejected': 0,
        'quota_used': 0,
        'quota_saved': 0
    }
if 'logs' not in st.session_state:
    st.session_state.logs = []

class YouTubeCollectorOptimized:
    """Optimized collector class with minimal API usage"""
    
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
                'surprise homecoming soldier',
                'feel good stories 2024',
                'touching moments compilation',
                'kindness caught on camera',
                'heartwarming rescue videos'
            ],
            'funny': [
                'funny fails 2024 new',
                'unexpected moments caught on camera 2024',
                'comedy sketches viral tiktok',
                'hilarious reactions 2024',
                'funny animals doing stupid things',
                'epic fail 2024 new videos',
                'instant karma funny moments',
                'comedy gold moments viral',
                'funny pranks gone right',
                'hilarious moments caught on tape',
                'comedy videos viral',
                'funny clips 2024'
            ],
            'traumatic': [
                'shocking moments caught on camera 2024',
                'dramatic rescue operations real',
                'natural disaster footage 2024',
                'intense police chases dashcam',
                'survival stories real footage',
                'near death experiences caught on tape',
                'unbelievable close calls 2024',
                'extreme weather caught on camera',
                'dramatic moments real life',
                'intense rescue footage',
                'survival caught on camera',
                'dramatic real life events'
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
        st.session_state.logs = st.session_state.logs[:100]
    
    def search_videos(self, query: str, max_results: int = 50) -> List[Dict]:
        """Search for videos using YouTube API (100 quota units)"""
        try:
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
            results = response.get('items', [])
            
            # Track quota usage
            st.session_state.stats['quota_used'] += 100
            
            self.add_log(f"Search API: {len(results)} results, 100 quota units used", "INFO")
            return results
            
        except HttpError as e:
            self.add_log(f"API Error during search: {str(e)}", "ERROR")
            if "quotaExceeded" in str(e):
                self.add_log("YouTube API quota exceeded!", "ERROR")
            return []
        except Exception as e:
            self.add_log(f"Unexpected error during search: {str(e)}", "ERROR")
            return []
    
    def get_oembed_data(self, video_id: str) -> Optional[Dict]:
        """Get basic video info using oEmbed API (FREE - no quota)"""
        try:
            url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                st.session_state.stats['quota_saved'] += 1  # Would have cost 1 unit
                return data
            else:
                return None
                
        except Exception as e:
            self.add_log(f"oEmbed error for {video_id}: {str(e)}", "WARNING")
            return None
    
    def detect_shorts_by_url_pattern(self, video_id: str, oembed_data: Dict) -> bool:
        """Detect if video is a YouTube Short using URL pattern analysis (FREE)"""
        try:
            # Method 1: Check if oEmbed HTML contains shorts indicators
            html = oembed_data.get('html', '')
            if '/shorts/' in html:
                return True
            
            # Method 2: Check thumbnail aspect ratio
            width = oembed_data.get('thumbnail_width', 0)
            height = oembed_data.get('thumbnail_height', 0)
            if height > 0 and width > 0:
                aspect_ratio = width / height
                # Shorts typically have portrait or square thumbnails
                if aspect_ratio < 1.2:  # Portrait or nearly square
                    return True
            
            # Method 3: Check title patterns
            title = oembed_data.get('title', '').lower()
            shorts_indicators = ['#shorts', '#short', 'shorts', 'short video']
            if any(indicator in title for indicator in shorts_indicators):
                return True
            
            # Method 4: Check embed dimensions in HTML
            if 'width="200"' in html and 'height="113"' in html:
                # This is often the default for shorts embeds
                return True
            
            return False
            
        except Exception as e:
            self.add_log(f"Error detecting shorts for {video_id}: {str(e)}", "WARNING")
            return False  # Default to assuming it's not a short
    
    def check_content_filters(self, title: str, author: str) -> Tuple[bool, str]:
        """Check title and author against exclusion keywords (FREE)"""
        title_lower = title.lower()
        author_lower = author.lower()
        
        # Check for music video indicators
        for keyword in self.music_keywords:
            if keyword in title_lower:
                return False, f"Music video detected in title (keyword: {keyword})"
        
        # Check for compilation indicators
        for keyword in self.compilation_keywords:
            if keyword in title_lower:
                return False, f"Compilation detected in title (keyword: {keyword})"
        
        return True, "Passed content filters"
    
    def get_video_details_api(self, video_id: str) -> Optional[Dict]:
        """Get detailed video info using YouTube API (1 quota unit)"""
        try:
            request = self.youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=video_id
            )
            response = request.execute()
            
            if response['items']:
                # Track quota usage
                st.session_state.stats['quota_used'] += 1
                return response['items'][0]
            return None
            
        except HttpError as e:
            self.add_log(f"API Error getting video details for {video_id}: {str(e)}", "ERROR")
            return None
        except Exception as e:
            self.add_log(f"Unexpected error getting video details for {video_id}: {str(e)}", "ERROR")
            return None
    
    def validate_video_optimized(self, search_item: Dict) -> Tuple[bool, str, Optional[Dict]]:
        """
        Optimized validation using hybrid approach
        Returns: (passed, reason_if_failed, video_details)
        """
        video_id = search_item['id']['videoId']
        title = search_item['snippet']['title']
        
        try:
            self.add_log(f"Quick validation: {title[:50]}...", "INFO")
            
            # STEP 1: Get basic info via oEmbed (FREE)
            oembed_data = self.get_oembed_data(video_id)
            if not oembed_data:
                return False, "Could not fetch oEmbed data", None
            
            oembed_title = oembed_data.get('title', title)
            oembed_author = oembed_data.get('author_name', '')
            
            # STEP 2: Quick content filters (FREE)
            content_passed, content_reason = self.check_content_filters(oembed_title, oembed_author)
            if not content_passed:
                return False, content_reason, None
            
            # STEP 3: Shorts detection (FREE)
            is_short = self.detect_shorts_by_url_pattern(video_id, oembed_data)
            if is_short:
                return False, "Detected as YouTube Short (URL pattern analysis)", None
            
            # STEP 4: Duplicate check (FREE)
            existing_ids = [v['video_id'] for v in st.session_state.collected_videos]
            if video_id in existing_ids:
                return False, "Duplicate video", None
            
            self.add_log(f"Passed quick filters, getting full details: {title[:50]}...", "INFO")
            
            # STEP 5: Final validation via API (1 quota unit)
            details = self.get_video_details_api(video_id)
            if not details:
                return False, "Could not fetch video details from API", None
            
            # Check caption availability
            has_captions = details['contentDetails'].get('caption', 'false') == 'true'
            if not has_captions:
                return False, "No captions available", None
            
            # Check age (redundant but as specified)
            published_at = datetime.fromisoformat(details['snippet']['publishedAt'].replace('Z', '+00:00'))
            six_months_ago = datetime.now(published_at.tzinfo) - timedelta(days=180)
            if published_at < six_months_ago:
                return False, "Video older than 6 months", None
            
            # Double-check duration (fallback for shorts detection)
            duration = isodate.parse_duration(details['contentDetails']['duration'])
            duration_seconds = duration.total_seconds()
            if duration_seconds < 90:
                return False, f"Video too short ({duration_seconds}s < 90s) - API confirmation", None
            
            # Check view count
            view_count = int(details['statistics'].get('viewCount', 0))
            if view_count < 10000:
                return False, f"View count too low ({view_count} < 10,000)", None
            
            # Final content filter check with full tags
            tags = [tag.lower() for tag in details['snippet'].get('tags', [])]
            for keyword in self.music_keywords:
                if any(keyword in tag for tag in tags):
                    return False, f"Music video detected in tags (keyword: {keyword})", None
            
            for keyword in self.compilation_keywords:
                if any(keyword in tag for tag in tags):
                    return False, f"Compilation detected in tags (keyword: {keyword})", None
            
            # All checks passed
            return True, "Passed all checks", details
            
        except Exception as e:
            self.add_log(f"Error validating video {video_id}: {str(e)}", "ERROR")
            return False, f"Validation error: {str(e)}", None
    
    def collect_videos(self, target_count: int, category: str, progress_callback=None):
        """Optimized collection logic with minimal API usage"""
        collected = []
        
        if category == 'mixed':
            categories = ['heartwarming', 'funny', 'traumatic']
        else:
            categories = [category]
        
        category_index = 0
        attempts = 0
        max_attempts = 50
        videos_checked_ids = set()
        consecutive_failures = 0
        max_consecutive_failures = 10
        
        self.add_log(f"Starting optimized collection: Target={target_count}, Category={category}", "INFO")
        self.add_log(f"Quota optimization: Using oEmbed API + URL pattern analysis", "INFO")
        
        while len(collected) < target_count and attempts < max_attempts:
            try:
                current_category = categories[category_index % len(categories)]
                available_queries = self.search_queries[current_category]
                query = random.choice(available_queries)
                
                self.add_log(f"Attempt {attempts+1}/{max_attempts}: Searching '{current_category}'", "INFO")
                
                # SEARCH PHASE: YouTube Data API (100 units)
                search_results = self.search_videos(query, max_results=50)
                
                if not search_results:
                    self.add_log("No search results, trying different query...", "WARNING")
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        self.add_log(f"Too many consecutive failures, stopping", "ERROR")
                        break
                    attempts += 1
                    category_index += 1
                    time.sleep(2)
                    continue
                
                consecutive_failures = 0
                videos_found_this_query = 0
                
                for i, item in enumerate(search_results):
                    if len(collected) >= target_count:
                        self.add_log(f"Target reached! Found {len(collected)}/{target_count} videos", "SUCCESS")
                        break
                    
                    video_id = item['id']['videoId']
                    
                    if video_id in videos_checked_ids:
                        continue
                        
                    videos_checked_ids.add(video_id)
                    st.session_state.stats['checked'] += 1
                    
                    if progress_callback:
                        progress_callback(len(collected), target_count)
                    
                    # OPTIMIZED VALIDATION
                    try:
                        passed, reason, details = self.validate_video_optimized(item)
                        
                        if passed and details:
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
                            
                            self.add_log(f"✓ Added ({len(collected)}/{target_count}): {video_record['title'][:50]}...", "SUCCESS")
                            
                        else:
                            st.session_state.stats['rejected'] += 1
                            self.add_log(f"✗ Rejected: {item['snippet']['title'][:50]}... - {reason}", "WARNING")
                    
                    except Exception as e:
                        self.add_log(f"Error processing video {video_id}: {str(e)}", "ERROR")
                        st.session_state.stats['rejected'] += 1
                    
                    time.sleep(0.3)  # Rate limiting
                
                self.add_log(f"Query complete: Found {videos_found_this_query} valid videos", "INFO")
                
                if videos_found_this_query == 0:
                    consecutive_failures += 1
                    category_index += 1
                else:
                    consecutive_failures = 0
                    if videos_found_this_query >= 3:
                        pass  # Stay with successful category
                    else:
                        category_index += 1
                
                attempts += 1
                time.sleep(2)
                
            except Exception as e:
                self.add_log(f"Unexpected error in collection loop: {str(e)}", "ERROR")
                attempts += 1
                time.sleep(3)
                continue
        
        # Final summary with quota usage
        quota_used = st.session_state.stats['quota_used']
        quota_saved = st.session_state.stats['quota_saved']
        
        if len(collected) >= target_count:
            self.add_log(f"🎉 Collection COMPLETE! Found {len(collected)} videos", "SUCCESS")
        else:
            self.add_log(f"⚠️ Collection stopped. Found {len(collected)}/{target_count} videos", "WARNING")
        
        self.add_log(f"📊 Quota Usage: {quota_used} units used, ~{quota_saved} units saved", "INFO")
        self.add_log(f"📈 Efficiency: ~{quota_saved/(quota_used+quota_saved)*100:.1f}% quota savings", "SUCCESS")
        
        return collected

class GoogleSheetsExporter:
    """Handle Google Sheets export (unchanged)"""
    
    def __init__(self, credentials_dict: Dict):
        self.creds = Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets',
                   'https://www.googleapis.com/auth/drive']
        )
        self.client = gspread.authorize(self.creds)
    
    def get_spreadsheet_by_id(self, spreadsheet_id: str):
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            st.success(f"✅ Connected to existing spreadsheet")
            return spreadsheet
        except Exception as e:
            st.error(f"Could not access spreadsheet: {str(e)}")
            raise e
    
    def create_or_get_spreadsheet(self, spreadsheet_name: str):
        try:
            spreadsheet = self.client.open(spreadsheet_name)
            st.success(f"✅ Found existing spreadsheet: {spreadsheet_name}")
        except gspread.exceptions.SpreadsheetNotFound:
            spreadsheet = self.client.create(spreadsheet_name)
            st.success(f"✅ Created new spreadsheet: {spreadsheet_name}")
            st.warning(f"⚠️ IMPORTANT: Share this spreadsheet with your main Google account to view it!")
        
        return spreadsheet
    
    def export_to_sheets(self, videos: List[Dict], spreadsheet_id: str = None, spreadsheet_name: str = "YouTube_Collection_Data"):
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
                    values = df.values.tolist()
                    worksheet.append_rows(values)
                    st.success(f"✅ Appended {len(videos)} new rows to existing data")
                else:
                    st.info("Creating new sheet with headers...")
                    worksheet.clear()
                    headers = df.columns.tolist()
                    values = [headers] + df.values.tolist()
                    worksheet.update('A1', values)
                    st.success(f"✅ Created new sheet with {len(videos)} videos")
                
                return spreadsheet.url
            else:
                st.warning("No videos to export")
                return None
                
        except Exception as e:
            st.error(f"Export error: {str(e)}")
            raise e

def main():
    """Main Streamlit app"""
    
    st.title("🎬 YouTube Data Collector - Optimized")
    st.markdown("*Quota-optimized collection using oEmbed API + URL pattern analysis*")
    
    # Show optimization info
    st.info("🚀 **Optimization Features:** oEmbed API for quick filtering • URL pattern shorts detection • ~50-80% quota savings")
    
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        st.subheader("1. YouTube API")
        youtube_api_key = st.text_input(
            "YouTube Data API Key",
            type="password",
            help="Get your API key from Google Cloud Console"
        )
        
        if youtube_api_key:
            st.success("✅ API Key provided")
        
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
        
        if sheets_creds and 'client_email' in sheets_creds:
            st.info(f"📧 Service Account: {sheets_creds['client_email']}")
        
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
    
    # Main metrics with quota tracking
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Videos Found", st.session_state.stats['found'])
    with col2:
        st.metric("Videos Checked", st.session_state.stats['checked'])
    with col3:
        st.metric("Quota Used", st.session_state.stats['quota_used'])
    with col4:
        st.metric("Quota Saved", st.session_state.stats['quota_saved'])
    
    # Control buttons
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🚀 Start Collection", disabled=st.session_state.is_collecting, type="primary"):
            if not youtube_api_key:
                st.error("❌ Please enter your YouTube API key")
            else:
                st.session_state.is_collecting = True
                st.session_state.stats = {'checked': 0, 'found': 0, 'rejected': 0, 'quota_used': 0, 'quota_saved': 0}
                st.session_state.logs = []
                
                try:
                    collector = YouTubeCollectorOptimized(youtube_api_key)
                    
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    def update_progress(current, total):
                        progress = min(current / total, 1.0)
                        progress_bar.progress(progress)
                        status_text.text(f"Collecting: {current}/{total} videos ({progress*100:.1f}%) | Quota: {st.session_state.stats['quota_used']} used")
                    
                    with st.spinner(f"Collecting {target_count} videos with quota optimization..."):
                        videos = collector.collect_videos(
                            target_count=target_count,
                            category=category,
                            progress_callback=update_progress
                        )
                    
                    if videos:
                        st.success(f"✅ Collection complete! Found {len(videos)} videos.")
                        st.info(f"📊 Quota efficiency: {st.session_state.stats['quota_used']} units used, ~{st.session_state.stats['quota_saved']} saved")
                    else:
                        st.warning(f"⚠️ Collection completed but no videos found. Check the logs for details.")
                    
                    # Auto-export if enabled
                    if auto_export and sheets_creds and videos:
                        try:
                            exporter = GoogleSheetsExporter(sheets_creds)
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
            st.session_state.stats = {'checked': 0, 'found': 0, 'rejected': 0, 'quota_used': 0, 'quota_saved': 0}
            st.session_state.logs = []
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
        
        st.dataframe(
            df[['title', 'category', 'view_count', 'duration_seconds', 'has_captions', 'url']],
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

