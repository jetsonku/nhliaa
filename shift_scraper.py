import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np


teamids = {
    'NJD':'1',
    'NYI':'2',
    'NYR':'3',
    'PHI':'4',
    'PIT':'5',
    'BOS':'6',
    'BUF':'7',
    'MTL':'8',
    'OTT':'9',
    'TOR':'10',
    'CAR':'12',
    'FLA':'13',
    'TBL':'14',
    'WSH':'15',
    'CHI':'16',
    'DET':'17',
    'NSH':'18',
    'STL':'19',
    'CGY':'20',
    'COL':'21',
    'EDM':'22',
    'VAN':'23',
    'ANA':'24',
    'DAL':'25',
    'LAK':'26',
    'SJS':'28',
    'CBJ':'29',
    'MIN':'30',
    'WPG':'52',
    'ARI':'53',
    'VGK':'54',
    'SEA':'55'



}


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
    year_req = requests.get(base_url + '/api/v1/schedule?teamId=15&startDate=2022-09-05&endDate=2022-12-12')
    year_data = year_req.json()
    i = 0
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
    year_req = requests.get(base_url + '/api/v1/schedule?teamId=15&startDate=2022-09-05&endDate=2022-12-12')
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

def to_team_shifts(shiftsdf, playsdf):
    headers = ['CF60', 'length_shift', 'off_zonestart', 'def_zonestart', 'btb', 'score_down_3', 'score_down_2', 'score_down_1', 'score_up_1',
                'score_up_2', 'score_up_3', 'state_4v4', 'state_3v3', 'is_home', 'gameId', 'period', 'startSeconds', 'endSeconds', 'team']
    shifts = pd.read_csv(shiftsdf)
    plays = pd.read_csv(playsdf)
    shifts['colname'] = shifts['firstName'] + "." + shifts['lastName']
    columns = shifts.drop_duplicates(subset=['colname'], keep='first')
    rows = {key: [] for key in headers}
    
    plays['seconds'] = [int(time.split(':')[0]) * 60 + int(time.split(':')[1]) for time in plays['periodTime']]

    
    shifts['endSeconds'] = [int(time.split(':')[0]) * 60 + int(time.split(':')[1]) for time in shifts['endTime']]
    shifts['startSeconds'] = [int(time.split(':')[0]) * 60 + int(time.split(':')[1]) for time in shifts['startTime']]

    shifts = shifts[shifts.typeCode == 517].sort_values(by=['gameId', 'period', 'endSeconds', 'startSeconds'])
    teamshifts_df = shifts.drop_duplicates(subset=['gameId', 'period', 'endTime'], keep='first')

    teamshifts = {'gameId':[0,] + teamshifts_df['gameId'].to_list(), 'period':[0,] +teamshifts_df['period'].to_list(), 
                'endtime':[0,] + teamshifts_df['endSeconds'].to_list(), 'team1':[], 'team2':[]}
    shifts = shifts.sort_values(by=['gameId', 'period', 'startSeconds'])
    for column in columns['colname'].to_list():
        headers.append(column + '.O')
        headers.append(column + '.D')
        rows.update({column + '.O':list(np.zeros(2*(len(teamshifts['gameId']) - 1)))})
        rows.update({column + '.D':list(np.zeros(2*(len(teamshifts['gameId']) - 1)))})
    gameid = ''
    for shift in range(1, len(teamshifts['gameId'])):
        onice = shifts[(shifts.gameId == teamshifts['gameId'][shift]) & (shifts.period == teamshifts['period'][shift]) & 
                (shifts.endSeconds >= teamshifts['endtime'][shift]) & (shifts.startSeconds <= teamshifts['endtime'][shift-1])]
        teams = onice.drop_duplicates(subset=['teamAbbrev'], keep='first')['teamAbbrev'].to_list()

        if teamshifts['gameId'][shift] != gameid:
            gameid = teamshifts['gameId'][shift]
            base_url = 'https://statsapi.web.nhl.com'
            date = plays[(plays.gamePk == teamshifts['gameId'][shift])]['dateTime'].to_list()[0][:10]
            
            print(date)
            date = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)
            date = date.strftime('%Y-%m-%d')
            yester_req = requests.get(base_url + f'/api/v1/schedule?teamId={teamids[teams[0]]}&startDate={date}&endDate={date}')
            yester_data = yester_req.json()
            if yester_data['totalGames'] > 0:
                btb1 = 1
            else:
                btb1 = 0
            yester_req = requests.get(base_url + f'/api/v1/schedule?teamId={teamids[teams[1]]}&startDate={date}&endDate={date}')
            yester_data = yester_req.json()
            if yester_data['totalGames'] > 0:
                btb2 = 1
            else:
                btb2 = 0    
        rows['length_shift'].append(teamshifts['endtime'][shift] - teamshifts['endtime'][shift-1])
        rows['length_shift'].append(teamshifts['endtime'][shift] - teamshifts['endtime'][shift-1])
        rows['btb'].append(btb1)
        rows['btb'].append(btb2)
        rows['gameId'].append(teamshifts['gameId'][shift])
        rows['gameId'].append(teamshifts['gameId'][shift])
        rows['period'].append(teamshifts['period'][shift])
        rows['period'].append(teamshifts['period'][shift])
        rows['startSeconds'].append(teamshifts['endtime'][shift-1])
        rows['startSeconds'].append(teamshifts['endtime'][shift-1])
        rows['endSeconds'].append(teamshifts['endtime'][shift])
        rows['endSeconds'].append(teamshifts['endtime'][shift])

    #    print(shifts[(shifts.gameId == 2022020005) & (shifts.period == 3) & 
    #            (shifts.endSeconds >= 1180)])
        

        rows['team'].append(teams[0])
        rows['team'].append(teams[1])
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
        onice1 = onice[onice.teamAbbrev == teams[0]]
        onice2 = onice[onice.teamAbbrev == teams[1]]
        oniceplays = plays[(plays.gamePk == teamshifts['gameId'][shift]) & (plays.period == teamshifts['period'][shift]) & (plays.seconds <= teamshifts['endtime'][shift]) & (plays.seconds >= teamshifts['endtime'][shift-1])]
        lastplay = plays[(plays.gamePk == teamshifts['gameId'][shift]) & (plays.period == teamshifts['period'][shift]) & (plays.seconds <= teamshifts['endtime'][shift-1])]
        lastplay = lastplay.sort_values(by=['seconds'], ascending=False).drop_duplicates(subset=['gamePk'], keep='first')
        if teams[0] == lastplay['eventCode'].to_list()[0][:3]:
            rows['is_home'].append(1)
            rows['is_home'].append(0)
            if lastplay['goals.home'].to_list()[0] > lastplay['goals.away'].to_list()[0]:
                dif = lastplay['goals.home'].to_list()[0] - lastplay['goals.away'].to_list()[0]
                rows['score_up_' + str(min(3, dif))][-2] = 1
                rows['score_down_' + str(min(3, dif))][-1] = 1
            elif lastplay['goals.home'].to_list()[0] < lastplay['goals.away'].to_list()[0]:
                dif = lastplay['goals.away'].to_list()[0] - lastplay['goals.home'].to_list()[0]
                rows['score_up_' + str(min(3, dif))][-1] = 1
                rows['score_down_' + str(min(3, dif))][-2] = 1
        else:

            rows['is_home'].append(0)
            rows['is_home'].append(1)
            if lastplay['goals.home'].to_list()[0] < lastplay['goals.away'].to_list()[0]:
                dif = lastplay['goals.away'].to_list()[0] - lastplay['goals.home'].to_list()[0]
                rows['score_up_' + str(min(3, dif))][-2] = 1
                rows['score_down_' + str(min(3, dif))][-1] = 1
            elif lastplay['goals.home'].to_list()[0] > lastplay['goals.away'].to_list()[0]:
                dif = lastplay['goals.home'].to_list()[0] - lastplay['goals.away'].to_list()[0]
                rows['score_up_' + str(min(3, dif))][-1] = 1
                rows['score_down_' + str(min(3, dif))][-2] = 1

        rows['state_4v4'].append(0)
        rows['state_4v4'].append(0)
        rows['state_3v3'].append(0)
        rows['state_3v3'].append(0)

        if len(onice1) == 5 and len(onice2) == 5:
            rows['state_4v4'][-2] = 1
            rows['state_4v4'][-1] = 1
        elif len(onice1) == 4 and len(onice2) == 4:
            rows['state_3v3'][-2] = 1
            rows['state_3v3'][-1] = 1



        shots = oniceplays[(oniceplays.eventTypeId == "SHOT") | (oniceplays.eventTypeId == "MISSED_SHOT") | (oniceplays.eventTypeId == "BLOCKED_SHOT")]
        shots1 = shots[shots.triCode == teams[0]]
        shots2 = shots[shots.triCode == teams[1]]
        faceoff = oniceplays[oniceplays.eventTypeId == "FACEOFF"].sort_values(by=['gamePk', 'period', 'seconds']).drop_duplicates(subset=['gamePk'], keep='first')
        if len(faceoff) > 0 and faceoff.triCode.to_list()[0] == teams[0]:
            if faceoff.x.to_list()[0] < -20:
                rows['off_zonestart'].append(0)
                rows['off_zonestart'].append(1)
                rows['def_zonestart'].append(1)
                rows['def_zonestart'].append(0)
            elif faceoff.x.to_list()[0] > 20:
                rows['off_zonestart'].append(1)
                rows['off_zonestart'].append(0)
                rows['def_zonestart'].append(0)
                rows['def_zonestart'].append(1)
            else:
                rows['off_zonestart'].append(0)
                rows['off_zonestart'].append(0)
                rows['def_zonestart'].append(0)
                rows['def_zonestart'].append(0)   
        elif len(faceoff) > 0 :
            if faceoff.x.to_list()[0] < -20:
                rows['off_zonestart'].append(1)
                rows['off_zonestart'].append(0)
                rows['def_zonestart'].append(0)
                rows['def_zonestart'].append(1)
            elif faceoff.x.to_list()[0] > 20:
                rows['off_zonestart'].append(0)
                rows['off_zonestart'].append(1)
                rows['def_zonestart'].append(1)
                rows['def_zonestart'].append(0)
            else:
                rows['off_zonestart'].append(0)
                rows['off_zonestart'].append(0)
                rows['def_zonestart'].append(0)
                rows['def_zonestart'].append(0)                          
        else:
            rows['off_zonestart'].append(0)
            rows['off_zonestart'].append(0)
            rows['def_zonestart'].append(0)
            rows['def_zonestart'].append(0)             
        rows['CF60'].append(3600*len(shots1)/(teamshifts['endtime'][shift] - teamshifts['endtime'][shift-1]))
        rows['CF60'].append(3600*len(shots2)/(teamshifts['endtime'][shift] - teamshifts['endtime'][shift-1]))
        for player in onice1.iterrows():
            rows[player[1]['colname'] + '.O'][2*(shift-1)] = 1
            rows[player[1]['colname'] + '.D'][2*(shift-1)+1] = 1
            
        for player in onice2.iterrows():
            rows[player[1]['colname'] + '.D'][2*(shift-1)] = 1
            rows[player[1]['colname'] + '.O'][2*(shift-1)+1] = 1
    csv = pd.DataFrame(rows, columns=headers)
    csv.to_csv('teamshifts.csv')

def to_apm(shiftsdf, playsdf):
    headers = ['CF60', 'length_shift', 'off_zonestart', 'def_zonestart', 'btb', 'score_down_3', 'score_down_2', 'score_down_1', 'score_up_1',
                'score_up_2', 'score_up_3', 'state_4v4', 'state_3v3', 'is_home']
    shifts = pd.read_csv(shiftsdf)
    plays = pd.read_csv(playsdf)
    shifts = shifts[shifts.typeCode == 517]
    shifts['colname'] = shifts['firstName'] + "." + shifts['lastName'] + "." + "O"


get_shifts().to_csv('shifts.csv')
get_plays().to_csv('plays.csv')
to_team_shifts('shifts.csv', 'plays.csv')