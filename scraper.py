
import os
import json
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import re

# Constant for base URL
espn_url = 'https://www.espn.com/'

def get_soup(url: str) -> BeautifulSoup:
    page = requests.get(url, headers={'User-agent': 'your bot 0.1'})
    soup = BeautifulSoup(page.text, 'html.parser')
    return soup

class DataESPN:
    """Class for fetching data from ESPN.com"""

    def __init__(self, data_dir: str = 'data'):
        """Initializes the class.

        Args:
            data_dir (str, optional): Directory to load/save data. Defaults to 'data'.
        """

        self.data_dir = data_dir
        print('Hello there user! I am DataESPN. I will help you fetch data from ESPN.com. Here are the list of data you currently have:')
        print(os.listdir(data_dir))

    ############# TEAMS #############
    #################################

    def get_teams_df(self) -> pd.DataFrame:
        """Fetches information about all NBA teams."""

        # Check if already saved
        if os.path.exists(f'{self.data_dir}/teams.parquet'):
            return pd.read_parquet(f'{self.data_dir}/teams.parquet')

        print('Saved teams not found. Fetching from ESPN.com...')
        soup = get_soup(f'{espn_url}nba/teams')

        # Process the HTML to get team data
        team_data = []
        for team in soup.find_all('section', class_='TeamLinks flex items-center'):
            team_data.append({
                'name': team.find('a').find('img')['alt'],
                'tag': team.find('a')['href'].split('/')[-1],
                'code': team.find('a')['href'].split('/')[-2],
                'url': team.find('a')['href']
            })

        # Convert to DataFrame and save to parquet
        teams_df = pd.DataFrame(team_data)
        teams_df.to_parquet(f'{self.data_dir}/teams.parquet')
        self.fix_teams_df()
        teams_df = pd.read_parquet(f'{self.data_dir}/teams.parquet')

        return teams_df
    
    def get_bref_teams_df(self, year: int = 2023) -> pd.DataFrame:
        """Gets the teams from basketball-reference.com"""

        # Check if already saved
        if os.path.exists(f'{self.data_dir}/bref_teams.parquet'):
            return pd.read_parquet(f'{self.data_dir}/bref_teams.parquet')

        print('Saved bref_teams not found. Fetching from basketball-reference.com...')

        soup = get_soup(f'https://www.basketball-reference.com/leagues/NBA_{year}_standings.html')
        all_tables = soup.find_all('table')
        print(len(all_tables))
        bref_teams = []
        for table in all_tables[:2]:
            for team in table.find_all('a'):
                bref_teams.append({
                    'name': team.text,
                    'bref_url': team['href'],
                    'bref_code': team['href'].split('/')[-2],
                })

        bref_teams_df = pd.DataFrame(bref_teams).sort_values('name').reset_index(drop=True)
        bref_teams_df.to_parquet(f'{self.data_dir}/bref_teams.parquet')
        return bref_teams_df

    def fix_teams_df(self):
        """This function fixes the inconsistency between ESPN and basketball-reference.com"""
        print('Running fix_teams_df()...')
        teams_df = self.get_teams_df()

        # Check if inconsistency exists
        if 'bref_code' in teams_df.columns:
            print('teams_df already has bref columns.')
        else:
            bref_teams_df = self.get_bref_teams_df()
            teams_df = teams_df.sort_values('name').reset_index(drop=True)
            teams_df.loc[teams_df['name'] == 'LA Clippers', 'name'] = 'Los Angeles Clippers'
            teams_df.loc[teams_df['name'] == 'LA Clippers', 'tag'] = 'los-angeles-clippers'
            teams_df = teams_df.merge(bref_teams_df, left_on='name', right_on='name', how='outer')
            teams_df.to_parquet(f'{self.data_dir}/teams.parquet')

    ############# SCHEDULE #############
    ####################################

    def get_schedule(self, team_code: str, team_tag: str, year: int = 2023) -> list:
        """Get schedule for a team in a given year

        Args:
            team_code (str): 3 letter team code. Example: 'atl'
            team_tag (str): team tag. Example: 'atlanta-hawks'
            year (int, optional): year. Example: 2023 means 2022-2023 season.
        Returns:
            list: list of dictionaries with schedule data
        """

        soup = get_soup(f'{espn_url}nba/team/schedule/_/name/{team_code}/season/{year}/seasontype/2')

        schedule_data = []
        for game in soup.find('table').find_all('tr')[1:]:
            tds = game.find_all('td')
            schedule_data.append({
                'date': tds[0].text,
                'datetime': datetime.strptime(tds[0].text, '%a, %b %d'),
                'is_home': tds[1].text.split(' ')[0] == 'vs',
                'result': tds[2].text[0],
                'scores': tds[2].text[1:].strip().split(' ')[0].split('-'),
                'OT': tds[2].text.strip()[-1] == 'T',
                'game_url': tds[2].find('a')['href'].split('.com/')[1] if tds[2].find('a') else None,
                'schedule_of': team_tag,
                'openent': tds[1].find('a')['href'].split('/')[-1],
            })
        return schedule_data
    
    def transform_schedule(self, schedule_data: list, year: int = 2023) -> pd.DataFrame:
        """Transforms schedule data into a DataFrame. Also adds year to datetime."""

        df = pd.DataFrame(schedule_data)
        df['datetime'] = df['datetime'].apply(lambda x: 
                                              x.replace(year=year-1) if x.month >= 10 else x.replace(year=year))
        return df
    
    def get_schedule_df(self, year: int = 2023) -> pd.DataFrame:
        """Gets the schedule for all teams in a given year."""

        # Check and return if already saved
        if os.path.exists(f'{self.data_dir}/schedule.parquet'):
            return pd.read_parquet(f'{data.data_dir}/schedule.parquet')
        
        print('Saved schedule not found. Fetching from ESPN.com...')

        # Get the schedule for each team
        schedule_data = []
        teams_df = self.get_teams_df()
        for _, row in tqdm(teams_df.iterrows(), total=teams_df.shape[0]):
            schedule_data.extend(self.get_schedule(row['code'], row['tag'], year))
        
        # Transform the schedule data
        schedule_df = self.transform_schedule(schedule_data, year)

        # Drop the away games (for duplicate games)
        schedule_df = schedule_df[schedule_df['is_home']]

        # Add new columns for home and away scores
        schedule_df['home_score'] = schedule_df.apply(lambda x: x['scores'][0]  if x['result'] == 'W' else x['scores'][-1], axis=1)
        schedule_df['away_score'] = schedule_df.apply(lambda x: x['scores'][-1] if x['result'] == 'W' else x['scores'][0],  axis=1)

        # Drop unnecessary columns
        schedule_df = schedule_df.drop(columns=['is_home', 'date', 'scores'])

        # Rename columns
        schedule_df.columns = ['date', 'home_win', 'OT', 'url', 'home_team', 'away_team', 'home_score', 'away_score']

        # Drop rows with duplicate urls (caused by neutral site games)
        schedule_df.drop_duplicates(subset=['url'], inplace=True)

        # Drop rows with nans
        schedule_df.dropna(inplace=True)

        # Check that there are 30 teams and 82 games per team
        assert schedule_df.shape[0] == 30 * 82 / 2
        
        # Save to parquet
        schedule_df.to_parquet(f'{self.data_dir}/schedule.parquet')

        return schedule_df
    
    ############# PLAYERS #############
    ###################################

    def get_players_df(self, year: int = 2023) -> pd.DataFrame:
        """Gets the players for all teams in a given year."""

        # Check if already saved
        if os.path.exists(f'{self.data_dir}/players.pkl'):
            return pd.read_pickle(f'{self.data_dir}/players.pkl')
        
        print('Saved players not found. Fetching from basketball-reference.com...')
        teams_df = self.get_teams_df()

        players_data = []
        for team_code in tqdm(teams_df['bref_code']): # Loop through all teams
            roster = pd.read_html(f'https://www.basketball-reference.com/teams/{team_code.upper()}/{year}.html')[0]
            roster['bref_code'] = team_code
            players_data.extend(roster.values.tolist())

        players_df = pd.DataFrame(players_data)
        players_df.columns = ['number', 'name', 'pos', 'height', 'weight', 'birth_date', 'origin', 'experience', 'college', 'bref_code']

        players_df.to_pickle(f'{self.data_dir}/players.pkl')
        return players_df

    ############# GAME DETAILS #############
    ########################################

    def gameclock2seconds(self, period: int, minutes: int, seconds: int) -> int:
        """Converts game clock to seconds"""
        return (period - 1) * 12 * 60 + (11 - minutes) * 60 + (60 - seconds)

    def secondsPassed(self, row):
        """Gets the number of seconds passed in the game"""
        if ':' in row['clock']:
            minutes, seconds = (int(x) for x in row['clock'].split(':'))
        elif '.' in row['clock']:
            minutes = 0
            seconds = int(row['clock'].split('.')[0])

        return self.gameclock2seconds(row['period'], minutes, seconds)
    
    def add_player_names(self, playbyplay_df: pd.DataFrame, team_tags: list) -> pd.DataFrame:
        """
        Adds a column with player names to the playbyplay_df.
        Args:
            playbyplay_df (pd.DataFrame): playbyplay dataframe
            team_tags (list): list of team tags. Example: ['atlanta-hawks', 'charlotte-hornets']
        """
        players_df = self.get_players_df()
        teams_df   = self.get_teams_df()

        # Get the set of players that played in the teams
        player_set = set(players_df.loc[players_df['bref_code'].isin(teams_df.loc[teams_df['tag'].isin(team_tags), 'bref_code']), 'name'])

        # Add a column with the player names
        playbyplay_df['playerNames'] = playbyplay_df['text'].apply(lambda x: [name for name in player_set if name in x])

        # Sort the names by their position in the text
        playbyplay_df['playerNames'] = playbyplay_df.apply(lambda x: [name for name, _ in 
                        sorted([(name, re.search(name, x['text']).start()) for name in x['playerNames']], key=lambda x: x[1])], axis=1)

        return playbyplay_df['playerNames']

    def get_playbyplay_df(self, game_url: str, team_tags: list) -> pd.DataFrame:
        """Gets play-by-play data for a given game url.
        Args:
            game_url (str): url of the game. Example: 'nba/game/_/gameId/401468020'
            team_tags (list): list of team tags. Example: ['atlanta-hawks', 'houston-rockets']
        """

        soup = get_soup(espn_url + game_url.replace('/game/', '/playbyplay/'))
        
        # A very weird ass script tag that has all the data
        text = soup.find_all('script')[-5].text
        text = text.split('playGrps')[1].split('}]],')[0] + '}]]'
        data = json.loads(text[2:])

        # flatten list
        df = pd.DataFrame([item for sublist in data for item in sublist]) 
        # columns are [id, period, text, homeAway, awayScore, homeScore, clock, scoringPlay]
        
        df['id']            = df['id'].astype(int)
        df['period']        = df['period'].apply(lambda x: x['number'])
        df['text']          = df['text'].fillna('').astype(str)
        df['homeAway']      = df['homeAway'].fillna('neutral').astype('category')
        df['clock']         = df['clock'].apply(lambda x: x['displayValue']).astype(str)
        df['scoringPlay']   = df['scoringPlay'].fillna(False)
        df['secondsPassed'] = df.apply(self.secondsPassed, axis=1)
        df['FT']            = df['text'].str.contains('free throw')
        df['playerNames']   = self.add_player_names(df, team_tags)

        return df

if __name__ == '__main__':
    data = DataESPN(data_dir='data_test')
    teams_df = data.get_teams_df()
    print(teams_df.head())

    schedule_df = data.get_schedule_df()
    print(schedule_df.head())

    playbyplay_df = data.get_playbyplay_df(schedule_df['url'].iloc[0], schedule_df[['home_team', 'away_team']].iloc[0].tolist())
    print(playbyplay_df.head())