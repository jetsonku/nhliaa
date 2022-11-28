import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np

def get_shifts():

    # Initializing some variables for later
    # headers will be the headers of the stats we are collecting
    # rows will be where all the data is stored
    base_url = 'https://statsapi.web.nhl.com'
    headers = ['gameId', 'id', 'detailCode', 'duration', 'endTime', 'eventDescription', 
                'eventDetails', 'eventNumber', 'firstName', 'gameId', 'hexValue', 
                'lastName', 'period', 'playerId', 'shiftNumber', 'startTime', 'teamAbbrev', 
                'teamId', 'teamName', 'typeCode']

    rows = {key: [] for key in headers}

    # Make an API request for all games between 2014 and now
    year_req = requests.get(base_url + '/api/v1/schedule?teamId=15&startDate=2022-09-05&endDate=2022-11-17')
    year_data = year_req.json()

    # For each game, get all the team stats
    for game in year_data['dates']:
        # Make sure it's regular season game
        if str(game['games'][0]['gamePk'])[4:6] == '02':
            gameID = game['games'][0]['gamePk']

            # Make an API request for each game to get team stats
            game_req = requests.get("https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId=" + str(gameID))
            game_data = game_req.json()
            for shift in game_data['data']:
                for stat in shift.keys():
                    rows[stat].append(shift[stat])
      
    shifts = pd.DataFrame(rows, columns=headers)

    return shifts

def get_plays():

    # Initializing some variables for later
    # headers will be the headers of the stats we are collecting
    # rows will be where all the data is stored
    base_url = 'https://statsapi.web.nhl.com'
    headers = ['event', 'eventCode', 'eventTypeId', 'description', 'secondaryType', 'penaltySeverity', 'penaltyMinutes', 'gameWinningGoal', 
    'emptyNet', 'eventIdx', 'eventId', 'period', 'periodType', 'ordinalNum', 'periodTime', 'periodTimeRemaining', 'dateTime', 'x', 'y', 
    'id', 'name', 'link', 'triCode', 'row.Names', 'Assist1', 'Assist2', 'Blocker', 'DrewBy', 'Goalie', 'Hittee', 'Hitter', 'Loser', 
    'PenaltyOn', 'PlayerID', 'Scorer', 'Shooter', 'Winner', 'gamePk', 'gameType', 'away_team', 'home_team', 'strength.code', 'strength.name', 
    'goals.away', 'goals.home', 'ServedBy']

    rows = {key: [] for key in headers}

    # Make an API request for all games between 2014 and now
    year_req = requests.get(base_url + '/api/v1/schedule?teamId=15&startDate=2022-09-05&endDate=2022-11-17')
    year_data = year_req.json()

 
    ## loop through each game and download play data
    for game in year_data['dates']:    
        
        gamelink = game['games'][0]['link']

        # Make an API request for each game to get team stats
        game_req = requests.get("https://statsapi.web.nhl.com" + str(gamelink))
        game_data = game_req.json()
        gamePk = game_data['gamePk']
        gameType = game_data['gameData']['game']['type']
        away_team = game_data['gameData']['teams']['away']['name']
        home_team = game_data['gameData']['teams']['home']['name']

        plays = game_data['liveData']['plays']['allPlays']
        for play in plays:
            for result in play['result'].keys():
                if result in headers:
                    rows[result].append(play['result'][result])
                else:
                    rows['strength.code'].append(play['result'][result]['code'])
                    rows['strength.name'].append(play['result'][result]['name'])
            for about in play['about'].keys():
                if about in headers:
                    rows[about].append(play['about'][about])
                else:
                    rows['goals.home'].append(play['about'][about]['home'])
                    rows['goals.away'].append(play['about'][about]['away'])
            for coordinate in play['coordinates'].keys():
                rows[coordinate].append(play['coordinates'][coordinate])
            if 'team' in play.keys():
                for team in play['team'].keys():
                    if team != 'link':
                        rows[team].append(play['team'][team])
            if 'players' in play.keys():
                i = 0                
                for player in play['players']:
                    if player['playerType'] == "Assist":
                        rows['Assist' + str(i)].append(player['player']['fullName'])
                    elif player['playerType'] != "Unknown":
                        rows[player['playerType']].append(player['player']['fullName'])

                    i += 1
            rows['home_team'].append(home_team)
            rows['away_team'].append(away_team)
            rows['gamePk'].append(gamePk)
            rows['gameType'].append(gameType)
            rows['link'].append(gamelink)
            for header in headers:
                if len(rows[header]) < len(rows['event']):
                    rows[header].append(None)
    plays = pd.DataFrame(rows, columns=headers)

    return plays

def to_team_shifts(shiftsdf):
    headers = ['CF60', 'length_shift', 'off_zonestart', 'def_zonestart', 'btb', 'score_down_3', 'score_down_2', 'score_down_1', 'score_up_1',
                'score_up_2', 'score_up_3', 'state_4v4', 'state_3v3', 'is_home', 'gameId', 'period', 'startSeconds', 'endSeconds', 'team']
    shifts = pd.read_csv(shiftsdf)
    shifts['colname'] = shifts['firstName'] + "." + shifts['lastName']

    columns = shifts.drop_duplicates(subset=['colname'], keep='first')
    rows = {key: [] for key in headers}
    

    
    shifts['endSeconds'] = [int(time.split(':')[0]) * 60 + int(time.split(':')[1]) for time in shifts['endTime']]
    shifts['startSeconds'] = [int(time.split(':')[0]) * 60 + int(time.split(':')[1]) for time in shifts['startTime']]

    shifts = shifts[shifts.typeCode == 517].sort_values(by=['gameId', 'period', 'endSeconds', 'startSeconds'])
    teamshifts_df = shifts.drop_duplicates(subset=['endTime'], keep='first')

    teamshifts = {'gameId':[0,] + teamshifts_df['gameId'].to_list(), 'period':[0,] +teamshifts_df['period'].to_list(), 
                'endtime':[0,] + teamshifts_df['endSeconds'].to_list(), 'team1':[], 'team2':[]}
    shifts = shifts.sort_values(by=['gameId', 'period', 'startSeconds'])
    for column in columns['colname'].to_list():
        headers.append(column + '.O')
        headers.append(column + '.D')
        rows.update({column + '.O':list(np.zeros(2*(len(teamshifts['gameId']) - 1)))})
        rows.update({column + '.D':list(np.zeros(2*(len(teamshifts['gameId']) - 1)))})
    for shift in range(1, len(teamshifts['gameId'])):
        rows['CF60'].append(0)
        rows['CF60'].append(0)
        rows['length_shift'].append(teamshifts['endtime'][shift] - teamshifts['endtime'][shift-1])
        rows['length_shift'].append(teamshifts['endtime'][shift] - teamshifts['endtime'][shift-1])
        rows['off_zonestart'].append(0)
        rows['off_zonestart'].append(0)
        rows['def_zonestart'].append(0)
        rows['def_zonestart'].append(0)
        rows['btb'].append(0)
        rows['btb'].append(0)
        rows['score_down_3'].append(0)
        rows['score_down_3'].append(0)  
        rows['score_down_2'].append(0)
        rows['score_down_2'].append(0)
        rows['score_down_1'].append(0)
        rows['score_down_1'].append(0)
        rows['score_up_1'].append(0)
        rows['score_up_1'].append(0)  
        rows['score_up_2'].append(0)
        rows['score_up_2'].append(0)  
        rows['score_up_3'].append(0)
        rows['score_up_3'].append(0)
        rows['state_4v4'].append(0)
        rows['state_4v4'].append(0)
        rows['state_3v3'].append(0)
        rows['state_3v3'].append(0)
        rows['is_home'].append(0)
        rows['is_home'].append(0)
        rows['gameId'].append(teamshifts['gameId'][shift])
        rows['gameId'].append(teamshifts['gameId'][shift])
        rows['period'].append(teamshifts['period'][shift])
        rows['period'].append(teamshifts['period'][shift])
        rows['startSeconds'].append(teamshifts['endtime'][shift-1])
        rows['startSeconds'].append(teamshifts['endtime'][shift-1])
        rows['endSeconds'].append(teamshifts['endtime'][shift])
        rows['endSeconds'].append(teamshifts['endtime'][shift])
        onice = shifts[(shifts.gameId == teamshifts['gameId'][shift]) & (shifts.period == teamshifts['period'][shift]) & 
                (shifts.endSeconds >= teamshifts['endtime'][shift]) & (shifts.startSeconds <= teamshifts['endtime'][shift-1])]
        teams = onice.drop_duplicates(subset=['teamAbbrev'], keep='first')['teamAbbrev'].to_list()
        print(teamshifts['gameId'][shift], teamshifts['period'][shift], teamshifts['endtime'][shift-1], teamshifts['endtime'][shift])
        print(onice)
        rows['team'].append(teams[0])
        rows['team'].append(teams[1])
        onice1 = onice[onice.teamAbbrev == teams[0]]
        onice2 = onice[onice.teamAbbrev == teams[1]]
        for player in onice1.iterrows():
            rows[player[1]['colname'] + '.O'][shift] = 1
            rows[player[1]['colname'] + '.D'][shift+1] = 1
        for player in onice2.iterrows():
            rows[player[1]['colname'] + '.D'][shift] = 1
            rows[player[1]['colname'] + '.O'][shift+1] = 1

    csv = pd.DataFrame(rows, columns=headers)
    csv.to_csv('teamshifts.csv')

def to_apm(shiftsdf, playsdf):
    headers = ['CF60', 'length_shift', 'off_zonestart', 'def_zonestart', 'btb', 'score_down_3', 'score_down_2', 'score_down_1', 'score_up_1',
                'score_up_2', 'score_up_3', 'state_4v4', 'state_3v3', 'is_home']
    shifts = pd.read_csv(shiftsdf)
    plays = pd.read_csv(playsdf)
    shifts = shifts[shifts.typeCode == 517]
    shifts['colname'] = shifts['firstName'] + "." + shifts['lastName'] + "." + "O"


to_team_shifts('shifts.csv')