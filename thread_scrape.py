from concurrent.futures import ThreadPoolExecutor, as_completed
from download_json import get_replay
import json
from requests import request
import os
from alive_progress import alive_bar
import re
from queue import Queue
from threading import Lock
import logging
from typing import Dict, List, Optional

class ReplayDownloader:
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.file_lock = Lock()
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Configure logging for the downloader"""
        logger = logging.getLogger('ReplayDownloader')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _safe_mkdir(self, directory: str) -> None:
        """Safely create directory if it doesn't exist"""
        if not os.path.exists(directory):
            os.makedirs(directory)

    def _parse_replay_data(self, log_content: str, data_dict: Dict) -> Dict:
        """Parse replay log content using regex patterns"""
        patterns = {
            'gen': r'\|gen\|(\d+)',
            'tier': r'\|tier\|([\w\s\[\]]+)',
            'winner': r'\|win\|([\w\s\*]+)'
        }
        
        parsed_data = {
            'id': data_dict.get('id'),
            'format': data_dict.get('format'),
            'players': data_dict.get('players'),
            'gen': None,
            'tier': None,
            'poke': {'p1': [], 'p2': []},
            'winner': None,
            'rules': [],
            'metadata': {},
            'full_log': log_content
        }
        
        # Extract basic patterns
        for key, pattern in patterns.items():
            match = re.search(pattern, log_content)
            if match:
                parsed_data[key] = int(match.group(1)) if key == 'gen' else match.group(1).strip()
        
        # Extract Pokemon for each player
        poke_pattern = r'\|poke\|(\w+)\|([\w\s,\-*]+)'
        for match in re.finditer(poke_pattern, log_content):
            player, pokemon = match.group(1), match.group(2).strip()
            if player in ['p1', 'p2']:
                parsed_data['poke'][player].append(pokemon)
        
        # Extract rules
        rule_pattern = r'\|rule\|([\w\s,:\-]+)'
        parsed_data['rules'] = [match.group(1).strip() 
                              for match in re.finditer(rule_pattern, log_content)]
        
        # Add metadata
        parsed_data['metadata'] = {
            'views': data_dict.get('views'),
            'uploadtime': data_dict.get('uploadtime'),
            'rating': data_dict.get('rating'),
            'private': data_dict.get('private'),
            'formatid': data_dict.get('formatid')
        }
        
        return parsed_data

    def _download_replay(self, url: str, output_path: str) -> Optional[Dict]:
        """Download and parse a single replay"""
        try:
            if not url.startswith("https://replay.pokemonshowdown.com/"):
                url = "https://replay.pokemonshowdown.com/" + url

            response = request("GET", url)
            if response.status_code != 200:
                self.logger.error(f"Failed to download {url}. Status code: {response.status_code}")
                return None

            data_dict = response.json()
            parsed_data = self._parse_replay_data(data_dict.get('log', ''), data_dict)

            with self.file_lock:
                with open(output_path, 'w', encoding='utf-8') as json_file:
                    json.dump(parsed_data, json_file, indent=4)

            return parsed_data

        except Exception as e:
            self.logger.error(f"Error processing {url}: {str(e)}")
            return None

    def _process_user_replays(self, user: str) -> List[Dict]:
        """Download all replays for a specific user"""
        try:
            user_url = f'https://replay.pokemonshowdown.com/search.json?user={user}'
            response = request("GET", user_url)
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch replays for user {user}")
                return []

            user_dict = response.json()
            replays = []
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_replay = {
                    executor.submit(
                        self._download_replay,
                        f"{game['id']}.json",
                        f'info/all/{game["id"]}.json'
                    ): game['id']
                    for game in user_dict
                }
                
                for future in as_completed(future_to_replay):
                    replay_id = future_to_replay[future]
                    try:
                        replay_data = future.result()
                        if replay_data:
                            replays.append(replay_data)
                    except Exception as e:
                        self.logger.error(f"Error processing replay {replay_id}: {str(e)}")

            return replays

        except Exception as e:
            self.logger.error(f"Error processing user {user}: {str(e)}")
            return []

    def download_ladder_replays(self, format_name: str = 'gen8ou') -> None:
        """Download replays for all players on a format's ladder"""
        try:
            # Create necessary directories
            self._safe_mkdir('info')
            self._safe_mkdir('info/all')
            
            # Download ladder data
            ladder_url = f'https://pokemonshowdown.com/ladder/{format_name}.json'
            response = request("GET", ladder_url)
            
            if response.status_code != 200:
                self.logger.error(f"Failed to download ladder data. Status code: {response.status_code}")
                return

            ladder_dict = response.json()
            
            # Process users with progress bar
            with alive_bar(len(ladder_dict['toplist'])) as bar:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_user = {
                        executor.submit(self._process_user_replays, entry['userid']): entry['userid']
                        for entry in ladder_dict['toplist']
                    }
                    
                    for future in as_completed(future_to_user):
                        user = future_to_user[future]
                        try:
                            replays = future.result()
                            self.logger.info(f"Downloaded {len(replays)} replays for user {user}")
                        except Exception as e:
                            self.logger.error(f"Error processing user {user}: {str(e)}")
                        bar()

        except Exception as e:
            self.logger.error(f"Error in download_ladder_replays: {str(e)}")

if __name__ == "__main__":
    # Initialize and run the downloader
    downloader = ReplayDownloader(max_workers=10)
    downloader.download_ladder_replays('gen3ou')