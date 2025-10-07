# Fabric notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "{LAKEHOUSE_ID}",
# META       "default_lakehouse_name": "{LAKEHOUSE_NAME}",
# META       "default_lakehouse_workspace_id": "{WORKSPACE_ID}",
# META       "known_lakehouses": [
# META         {
# META           "id": "{LAKEHOUSE_ID}"
# META         }
# META       ]
# META     }
# META   }
# META }


# CELL ********************

import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
import time

from pyspark.sql.functions import col, desc, concat, lit, floor, datediff, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class NflTeam:
    """NFL Team."""

    def __init__(self, team, division, conference, city, team_roster):
        """Initialize NFL Team."""
        self.team = team
        self.division = division
        self.conference = conference
        self.city = city
        self.team_roster = team_roster


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nfl_teams = [
    NflTeam("Bills", "AFC-East", "AFC", "Buffalo, NY", "http://www.buffalobills.com/team/roster.html"),
    NflTeam("Dolphins", "AFC-East", "AFC", "Miami, FL", "http://www.miamidolphins.com/team/player-roster.html"),
    NflTeam("Jets", "AFC-East", "AFC", "East Rutherford, NJ", "http://www.newyorkjets.com/team/roster.html"),
    NflTeam("Patriots", "AFC-East", "AFC", "Foxboro, MA", "http://www.patriots.com/team/roster"),
    NflTeam("Bengals", "AFC-North", "AFC", "Cincinnati, OH", "http://www.bengals.com/team/roster.html"),
    NflTeam("Browns", "AFC-North", "AFC", "Cleveland, OH", "http://www.clevelandbrowns.com/team/roster.html"),
    NflTeam("Ravens", "AFC-North", "AFC", "Baltimore, MD", "http://www.baltimoreravens.com/team/roster.html"),
    NflTeam("Steelers", "AFC-North", "AFC", "Pittsburg, PA", "http://www.steelers.com/team/roster.html"),
    NflTeam("Colts", "AFC-South", "AFC", "Indianapolis, IN", "http://www.colts.com/team/roster.html"),
    NflTeam("Jaguars", "AFC-South", "AFC", "Jacksonville, FK", "http://www.jaguars.com/team/roster.html"),
    NflTeam("Texans", "AFC-South", "AFC", "Houston, TX", "http://www.houstontexans.com/team/roster.html"),
    NflTeam("Titans", "AFC-South", "AFC", "Nashville, TN", "http://www.titansonline.com/team/roster.html"),
    NflTeam("Broncos", "AFC-West", "AFC", "Denver, CO", "http://www.denverbroncos.com/team/roster.html"),
    NflTeam("Chargers", "AFC-West", "AFC", "San Diego, CA", "http://www.chargers.com/team/roster"),
    NflTeam("Chiefs", "AFC-West", "AFC", "Kansas City, MO", "http://www.chiefs.com/team/roster.html"),
    NflTeam("Raiders", "AFC-West", "AFC", "Oakland, CA", "http://www.raiders.com/team/roster.html"),
    NflTeam("Eagles", "NFC-East", "NFC", "Philadelphia, PA", "http://www.philadelphiaeagles.com/team/roster.html"),
    NflTeam("Giants", "NFC-East", "NFC", "East Rutherford, NJ", "http://www.giants.com/team/roster.html"),
    NflTeam("Redskins", "NFC-East", "NFC", "Washington, DC", "http://www.redskins.com/team/roster.html"),
    NflTeam("Cowboys", "NFC-East", "NFC", "Dallas, TX", "http://www.dallascowboys.com/team/roster"),
    NflTeam("Bears", "NFC-North", "NFC", "Chicago, IL", "http://www.chicagobears.com/team/roster.html"),
    NflTeam("Lions", "NFC-North", "NFC", "Detroit, MI", "http://www.detroitlions.com/team/roster.html"),
    NflTeam("Packers", "NFC-North", "NFC", "Green Bay, WI", "https://www.packers.com/team/players-roster"),
    NflTeam("Vikings", "NFC-North", "NFC", "Minneapolis , MN", "http://www.vikings.com/team/roster.html"),
    NflTeam("Buccaneers", "NFC-South", "NFC", "Tampa, FL", "http://www.buccaneers.com/team-and-stats/roster.html"),
    NflTeam("Falcons", "NFC-South", "NFC", "Atlanta, GA", "http://www.atlantafalcons.com/team/player-roster.html"),
    NflTeam("Panthers", "NFC-South", "NFC", "Charlotte, NC", "http://www.panthers.com/team/roster.html"),
    NflTeam("Saints", "NFC-South", "NFC", "New Orleans, LA", "http://www.neworleanssaints.com/team/roster.html"),
    NflTeam("49ers", "NFC-West", "NFC", "San Francisco, CA", "http://www.49ers.com/team/roster.html"),
    NflTeam("Cardinals", "NFC-West", "NFC", "Glendale, AZ", "http://www.azcardinals.com/roster/player-roster.html"),
    NflTeam("Rams", "NFC-West", "NFC", "St. Louis, MO", "http://www.therams.com/team/roster.html"),
    NflTeam("Seahawks", "NFC-West", "NFC", "Seattle, WA", "http://www.seahawks.com/team/roster")
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns = [ 'team', 'division', 'conference', 'city', 'team_roster' ]
df_nfl_teams = spark.createDataFrame(nfl_teams).select(columns)

df_nfl_teams.show()

df_nfl_teams.write \
       .mode("overwrite") \
       .option("overwriteSchema", "True") \
       .format("delta") \
       .saveAsTable(f"gold.nfl_teams")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

positions = {
    'FB': {'position':'Fullback', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'FB/DT': {'position':'Fullback', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'FB/LB': {'position':'Fullback', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'HB': {'position':'Halfback', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'H-B': {'position':'Halfback', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'QB': {'position':'Quarterback', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'RB': {'position':'Running Back', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'RB/RS': {'position':'Running Back', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'RB/WR': {'position':'Running Back', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'TE': {'position':'Tight End', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'TE/FB': {'position':'Tight End', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'TE/LS': {'position':'Tight End', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'TE/WR': {'position':'Tight End', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'WR': {'position':'Wide Receiver', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'WR/KR': {'position':'Wide Receiver', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'WR/RS': {'position':'Wide Receiver', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'C': {'position':'Center', 'position_category':'Offensive Line', 'side':'Offense' },
    'C/G': {'position':'Center', 'position_category':'Offensive Line', 'side':'Offense' },
    'C-G': {'position':'Center', 'position_category':'Offensive Line', 'side':'Offense' },
    'G': {'position':'Guard', 'position_category':'Offensive Line', 'side':'Offense' },
    'G / T': {'position':'Guard', 'position_category':'Offensive Line', 'side':'Offense' },
    'G/C': {'position':'Guard', 'position_category':'Offensive Line', 'side':'Offense' },
    'G/T': {'position':'Guard', 'position_category':'Offensive Line', 'side':'Offense' },
    'G-T': {'position':'Guard', 'position_category':'Offensive Line', 'side':'Offense' },
    'OG': {'position':'Guard', 'position_category':'Offensive Line', 'side':'Offense' },
    'OL': {'position':'Offensive Lineman', 'position_category':'Offensive Line', 'side':'Offense' },
    'OT': {'position':'Offensive Tackle', 'position_category':'Offensive Line', 'side':'Offense' },
    'T': {'position':'Offensive Tackle', 'position_category':'Offensive Line', 'side':'Offense' },
    'T/G': {'position':'Offensive Tackle', 'position_category':'Offensive Line', 'side':'Offense' },
    'H': {'position':'Holder', 'position_category':'Special Teams', 'side':'Offense' },
    'K': {'position':'Kicker', 'position_category':'Special Teams', 'side':'Offense' },
    'LS': {'position':'Long Snapper', 'position_category':'Special Teams', 'side':'Offense' },
    'P': {'position':'Punter', 'position_category':'Special Teams', 'side':'Offense' },
    'CB': {'position':'Cornerback', 'position_category':'Defensive Backs', 'side':'Defense' },
    'DB': {'position':'Defensive Back', 'position_category':'Defensive Backs', 'side':'Defense' },
    'DB/RS': {'position':'Defensive Back', 'position_category':'Defensive Backs', 'side':'Defense' },
    'FS': {'position':'Safety', 'position_category':'Defensive Backs', 'side':'Defense' },
    'S': {'position':'Safety', 'position_category':'Defensive Backs', 'side':'Defense' },
    'SS': {'position':'Safety', 'position_category':'Defensive Backs', 'side':'Defense' },
    'DE': {'position':'Defensive End', 'position_category':'Defensive Line', 'side':'Defense' },
    'DL': {'position':'Defensive Lineman', 'position_category':'Defensive Line', 'side':'Defense' },
    'DT': {'position':'Defensive Tackle', 'position_category':'Defensive Line', 'side':'Defense' },
    'NT': {'position':'Nose Tackle', 'position_category':'Defensive Line', 'side':'Defense' },
    'ILB': {'position':'Inside Lineback', 'position_category':'Linebackers', 'side':'Defense' },
    'LB': {'position':'Linebacker', 'position_category':'Linebackers', 'side':'Defense' },
    'LB/DE': {'position':'Linebacker', 'position_category':'Linebackers', 'side':'Defense' },
    'LB/S': {'position':'Linebacker', 'position_category':'Linebackers', 'side':'Defense' },
    'MLB': {'position':'Middle Linebacker', 'position_category':'Linebackers', 'side':'Defense' },
    'OLB': {'position':'Outside Linebacker', 'position_category':'Linebackers', 'side':'Defense' },
    'WLB': {'position':'Linebacker', 'position_category':'Linebackers', 'side':'Defense' },
    'SAF': {'position':'Safety', 'position_category':'Defensive Backs', 'side':'Defense' },
    'SP': {'position':'Safety', 'position_category':'Defensive Backs', 'side':'Defense' },
    'ST': {'position':'Special Teams', 'position_category':'Special Teams', 'side':'Offense' },
    'WR-KR': {'position':'Wide Receiver', 'position_category':'Backs and Receivers', 'side':'Offense' },
    'RT': {'position':'Offensive Tackle', 'position_category':'Offensive Line', 'side':'Offense' },
    'LT': {'position':'Offensive Tackle', 'position_category':'Offensive Line', 'side':'Offense' },
    '$LT': {'position':'Offensive Tackle', 'position_category':'Offensive Line', 'side':'Offense' },
    'LG': {'position':'Offensive Guard', 'position_category':'Offensive Line', 'side':'Offense' },
    'RG': {'position':'Offensive Guard', 'position_category':'Offensive Line', 'side':'Offense' },
    'DB/LB': {'position':'Defensive Back', 'position_category':'Defensive Backs', 'side':'Defense' },
    'DL/LB': {'position':'Defensive Lineman', 'position_category':'Defensive Line', 'side':'Defense' },
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_position(position_code):
    try:
        result = positions[position_code]['position']
        return result
    except KeyError:
        return position_code

def get_position_category(position_code):
    try:
        result = positions[position_code]['position_category']
        return result
    except KeyError:
        return 'position_code'

def get_position_side(position_code):
    try:
        result = positions[position_code]['side']
        return result
    except KeyError:
        return 'N/A'


def convert_height_to_inches(height_str):
    try:
        if pd.isna(height_str):  # Handle potential NaN values
            return None
        feet, inches = map(int, height_str.split('-'))
        total_inches = (feet * 12) + inches
        return total_inches
    except ValueError:
        return None

udf_get_position = udf(get_position)
udf_get_position_category = udf(get_position_category)
udf_get_position_side = udf(get_position_side)
udf_convert_height_to_inches = udf(convert_height_to_inches)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_roster_data(team):
    """
    Scrapes NFL team roster data from the team's roster URL
    Returns:
        pandas.DataFrame: DataFrame containing player information
    """
    try:
        # Add headers to mimic browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Get the webpage content
        response = requests.get(team.team_roster, headers=headers, timeout=60)
        response.raise_for_status()
        
        # Parse HTML content
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find first HTML table which is the roster tableteam
        table = soup.find('table')
        
        if not table:
            raise ValueError(f"Could not find roster table for {team.team}")
        
        players_data = []
        headers = [ "player", "number", "position_code", "height", "weight", "age", "experience", "college", "team"]
        for row in table.find_all('tr')[1:]:  # Skip header row
            player = { 'team': team.team}
            for idx, td in enumerate(row.find_all('td')):
                if idx < (len(headers)-1):
                    player[headers[idx]] = td.text.strip()
            if player:  # Only add non-empty rows
                players_data.append(player)

        return (
            spark.createDataFrame(players_data)
                .withColumn("player-key", concat(col('team'), lit('-'), col('number')))                  
                .withColumn('position', udf_get_position(col('position_code')))
                .withColumn('position_category', udf_get_position_category(col('position_code')))
                .withColumn('side', udf_get_position_side(col('position_code')))
                .replace('R', '0', ['experience'])
                .withColumn('height_inches', udf_convert_height_to_inches(col('height')))
                .select(['number', 'player', 'height', 'height_inches', 'weight', 'age', 'experience', 'position_code', 'position', 'position_category', 'side', 'college', 'team', 'player-key'])
            )
        
    except requests.RequestException as e:
        print(f"Error fetching data for {team.team}: {e}")
        return None
    except Exception as e:
        print(f"Error processing data for {team.team}: {e}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# team = nfl_teams[0]

# team_df = get_roster_data(team)

# team_df.printSchema()
# team_df.show()

# team_spark_df.write.mode("overwrite").option("overwriteSchema", "True").format("delta").saveAsTable(f"silver.{team_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Getting NFL player data from offical NFL websites")

for nfl_team in nfl_teams:

    print(f" - Retreiving roster for {nfl_team.team} from {nfl_team.team_roster}")
    df_team_roster = get_roster_data(nfl_team)
    
    if df_team_roster is not None:
        df_team_roster.write \
                      .mode("overwrite") \
                      .option("overwriteSchema", "True") \
                      .format("delta") \
                      .saveAsTable(f"silver.{nfl_team.team}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('Creating master player table')
nfl_players = None
for nfl_team in nfl_teams:
    print(f'- Loading {nfl_team.team}')
    if nfl_players is None:
        nfl_players = spark.sql(f"SELECT * FROM nfl_data.silver.{nfl_team.team}")
    else:
        nfl_players = nfl_players.union( spark.sql(f"SELECT * FROM nfl_data.silver.{nfl_team.team}") )

print("Writing gold.nfl_players")
nfl_players.write \
        .mode("overwrite") \
        .option("overwriteSchema", "True") \
        .format("delta") \
        .saveAsTable(f"gold.nfl_players")

print("Table gold.nfl_players saved successfully")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
