import pymysql
import boto3
import json
import pandas as pd
import tempfile
import os
from datetime import datetime

# Replace with your actual values
RDS_HOST = "your-rds-instance.region.rds.amazonaws.com"
RDS_PORT = 3306
RDS_USER = "admin"
RDS_PASSWORD = "your-password"
RDS_DB_NAME = "pokemon_analyzer"

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Get the bucket name and key from the event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"Processing file {key} from bucket {bucket}")
        
        # Download the file to Lambda's temporary storage
        with tempfile.NamedTemporaryFile(suffix='.csv') as temp_csv:
            s3.download_file(bucket, key, temp_csv.name)
            
            # Process the CSV file
            print("File downloaded, processing data...")
            processed_team, team_id = process_csv(temp_csv.name, key)
            
            # Store data in RDS
            print("Data processed, storing in RDS...")
            store_in_rds(processed_team, team_id)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f"Successfully processed team from {key}",
                    'team_id': team_id
                })
            }
    
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error processing file: {str(e)}"
        }

def process_csv(csv_path, filename):
    """Process the CSV file and extract Pokémon team data"""
    # Read CSV into pandas DataFrame
    df = pd.read_csv(csv_path)
    
    # Clean up column names - handle case and spaces
    df.columns = [col.strip().lower() for col in df.columns]
    
    # Map different column variations to expected format
    column_map = {
        'hp': 'HP',
        'health': 'HP',
        'hitpoints': 'HP',
        'atk': 'attack',
        'attack': 'attack',
        'def': 'defense',
        'defense': 'defense',
        'sp. atk': 'special_attack',
        'sp.atk': 'special_attack',
        'spatk': 'special_attack',
        'special attack': 'special_attack',
        'specialattack': 'special_attack',
        'sp. def': 'special_defense',
        'sp.def': 'special_defense',
        'spdef': 'special_defense',
        'special defense': 'special_defense',
        'specialdefense': 'special_defense',
        'spd': 'speed',
        'speed': 'speed'
    }
    
    # Rename columns using the mapping
    df = df.rename(columns=column_map)
    
    # Define expected columns
    expected_columns = ['name', 'type1', 'type2', 'HP', 'attack', 'defense', 'special_attack', 'special_defense', 'speed']
    
    # Add missing columns if needed
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    
    # Clean Pokemon names and types (standardize case and remove extra spaces)
    if 'name' in df.columns:
        df['name'] = df['name'].apply(lambda x: x.strip().title() if isinstance(x, str) else x)
    
    if 'type1' in df.columns:
        df['type1'] = df['type1'].apply(lambda x: x.strip().lower() if isinstance(x, str) else x)
    
    if 'type2' in df.columns:
        df['type2'] = df['type2'].apply(lambda x: x.strip().lower() if isinstance(x, str) else x)
    
    # Fill null values for stats with specific means
    stat_means = {
        'HP': 71, 
        'attack': 81, 
        'defense': 75, 
        'special_attack': 73, 
        'special_defense': 72, 
        'speed': 70
    }
    
    # Fill null values with specific means for each stat
    for stat, mean_value in stat_means.items():
        df[stat] = df[stat].fillna(mean_value)
    
    # Handle type data
    # If no type1, replace with type2
    mask = df['type1'].isna() & ~df['type2'].isna()
    df.loc[mask, 'type1'] = df.loc[mask, 'type2']
    df.loc[mask, 'type2'] = None
    
    # If no type1 or type2, replace with 'normal'
    df.loc[df['type1'].isna(), 'type1'] = 'normal'
    
    # Calculate type effectiveness
    df['type_matchups'] = df.apply(calculate_type_matchups, axis=1)
    
    # Generate team_id from filename and timestamp
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename_base = os.path.splitext(os.path.basename(filename))[0]
    team_id = f"{filename_base}_{timestamp}"
    
    return df, team_id

def calculate_type_matchups(pokemon_row):
    """
    Calculate offensive and defensive type matchups using a single type chart
    """
    type1 = pokemon_row['type1']
    type2 = pokemon_row['type2'] if not pd.isna(pokemon_row['type2']) else None
    
    # All Pokémon types
    all_types = [
        'normal', 'fire', 'water', 'electric', 'grass', 'ice', 
        'fighting', 'poison', 'ground', 'flying', 'psychic', 
        'bug', 'rock', 'ghost', 'dragon', 'dark', 'steel', 'fairy'
    ]
    
    # Initialize offensive and defensive matchups as neutral (0)
    offensive_matchups = {t: 0 for t in all_types}
    defensive_matchups = {t: 0 for t in all_types}
    
    # Unified type effectiveness chart
    # Value represents how effective the attacking type (key) is against the defending type (subkey)
    # 1 = super effective, 0 = neutral, -1 = not very effective, -2 = no effect (immune)
    type_chart = {
        "Normal": {"Rock": -1, "Ghost": -2, "Steel": -1},
        "Fire": {"Fire": -1, "Water": -1, "Grass": 1, "Ice": 1, "Bug": 1, "Rock": -1, "Dragon": -1,
                 "Steel": 1},
        "Water": {"Fire": 1, "Water": -1, "Grass": -1, "Ground": 1, "Rock": 1, "Dragon": -1},
        "Grass": {"Fire": -1, "Water": 1, "Grass": -1, "Poison": -1, "Ground": 1, "Flying": -1, "Bug": -1, "Rock": 1,
                  "Dragon": -1, "Steel": -1},
        "Electric": {"Water": 1, "Grass": -1, "Electric": -1, "Ground": -2, "Flying": 1, "Dragon": -1},
        "Ice": {"Fire": -1, "Water": -1, "Grass": 1, "Ice": -1, "Ground": 1, "Flying": 1, "Dragon": 1,
                "Steel": -1},
        "Fighting": {"Normal": 1, "Ice": 1, "Poison": -1, "Flying": -1, "Psychic": -1, "Bug": -1, "Rock": 1, "Ghost": -2,
                     "Dark": 1, "Steel": 1, "Fairy": -1},
        "Poison": {"Grass": 1, "Poison": -1, "Ground": -1, "Rock": -1, "Ghost": -1,
                   "Steel": -2, "Fairy": 1},
        "Ground": {"Fire": 1, "Grass": -1, "Electric": 1, "Poison": 1, "Flying": -2, "Bug": -1, "Rock": 1,
                   "Steel": 1},
        "Flying": {"Grass": 1, "Electric": -1, "Fighting": 1, "Bug": 1, "Rock": -1,
                   "Steel": -1},
        "Psychic": {"Fighting": 1, "Poison": 1, "Psychic": -1, "Dark": -2,
                    "Steel": -1},
        "Bug": {"Fire": -1, "Grass": 1, "Fighting": -1, "Poison": -1, "Flying": -1, "Psychic": 1, "Ghost": -1,
                "Dark": 1, "Steel": -1, "Fairy": -1},
        "Rock": {"Fire": 1, "Ice": 1, "Fighting": -1, "Ground": -1, "Flying": 1, "Bug": 1,
                 "Steel": -1},
        "Ghost": {"Normal": -2, "Psychic": 1, "Ghost": 1, "Dark": -1},
        "Dragon": {"Dragon": 1, "Steel": -1, "Fairy": -2},
        "Dark": {"Fighting": -1, "Psychic": 1, "Ghost": 1, "Dark": -1,
                 "Fairy": -1},
        "Steel": {"Fire": -1, "Water": -1, "Electric": -1, "Ice": 1, "Rock": 1, "Steel": -1, "Fairy": 1},
        "Fairy": {"Fire": -1, "Fighting": 1, "Ground": -1, "Dragon": 1, "Dark": 1,
                  "Steel": -1}
    }
    
    # Calculate offensive matchups - how this Pokémon's types fare against others
    if type1 in type_chart:
        for defending_type, effectiveness in type_chart[type1].items():
            offensive_matchups[defending_type] = effectiveness
    
    if type2 and type2 in type_chart:
        for defending_type, effectiveness in type_chart[type2].items():
            # Take the most effective option between type1 and type2
            if effectiveness > offensive_matchups[defending_type]:
                offensive_matchups[defending_type] = effectiveness
    
    # Calculate defensive matchups - how other types fare against this Pokémon
    for attacking_type in type_chart:
        for defending_type in type_chart[attacking_type]:
            # If this attacking type has an effectiveness against one of our types
            if defending_type == type1:
                # The defensive score is the inverse of the offensive score
                defensive_matchups[attacking_type] -= type_chart[attacking_type][defending_type]
            
            if defending_type == type2 and type2 is not None:
                # Stack effects for dual types
                defensive_matchups[attacking_type] -= type_chart[attacking_type][defending_type]
    
    # Clamp defensive values to the range [-2, 2]
    for t in defensive_matchups:
        defensive_matchups[t] = max(-2, min(2, defensive_matchups[t]))
    
    return {
        'offensive': offensive_matchups,
        'defensive': defensive_matchups
    }

def store_in_rds(df, team_id):
    """Store processed team data in RDS"""
    try:
        # Connect to MySQL without specifying a database (to create if needed)
        connection = pymysql.connect(
            host=RDS_HOST,
            port=RDS_PORT,
            user=RDS_USER,
            password=RDS_PASSWORD,
            autocommit=True
        )
        
        # Create database if it doesn't exist
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {RDS_DB_NAME}")
            print(f"✅ Created or verified database '{RDS_DB_NAME}'.")
        
        connection.close()
        
        # Reconnect with the database selected
        connection = pymysql.connect(
            host=RDS_HOST,
            port=RDS_PORT,
            user=RDS_USER,
            password=RDS_PASSWORD,
            database=RDS_DB_NAME,
            autocommit=True
        )
        
        # Create tables if they don't exist
        with connection.cursor() as cursor:
            # Teams table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                id INT AUTO_INCREMENT PRIMARY KEY,
                team_id VARCHAR(255) UNIQUE NOT NULL,
                created_at DATETIME NOT NULL
            )
            """)
            print("✅ Created or verified 'teams' table.")
            
            # Pokemon table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS pokemon (
                id INT AUTO_INCREMENT PRIMARY KEY,
                team_id VARCHAR(255) NOT NULL,
                name VARCHAR(100),
                type1 VARCHAR(50) NOT NULL,
                type2 VARCHAR(50),
                hp FLOAT,
                attack FLOAT,
                defense FLOAT,
                special_attack FLOAT,
                special_defense FLOAT,
                speed FLOAT
            )
            """)
            print("✅ Created or verified 'pokemon' table.")
            
            # Type matchups table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS type_matchups (
                id INT AUTO_INCREMENT PRIMARY KEY,
                pokemon_id INT NOT NULL,
                type VARCHAR(50) NOT NULL,
                offensive_score INT,
                defensive_score INT
            )
            """)
            print("✅ Created or verified 'type_matchups' table.")
            
            # Insert team record
            cursor.execute(
                "INSERT INTO teams (team_id, created_at) VALUES (%s, NOW())",
                (team_id,)  # Fixed: Added comma to make this a tuple
            )
            print(f"✅ Inserted team record with ID: {team_id}")
            
            # Insert Pokémon records and their matchups
            for idx, row in df.iterrows():
                # Insert Pokémon
                cursor.execute("""
                INSERT INTO pokemon (team_id, name, type1, type2, hp, attack, defense, special_attack, special_defense, speed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    team_id,
                    row['name'],
                    row['type1'],
                    row['type2'] if not pd.isna(row['type2']) else None,
                    float(row['HP']),
                    float(row['attack']),
                    float(row['defense']),
                    float(row['special_attack']),
                    float(row['special_defense']),
                    float(row['speed'])
                ))
                
                # Get the Pokémon ID
                pokemon_id = cursor.lastrowid
                print(f"✅ Inserted Pokémon: {row['name']} with ID: {pokemon_id}")
                
                # Insert type matchups
                for type_name in row['type_matchups']['offensive'].keys():
                    cursor.execute("""
                    INSERT INTO type_matchups (pokemon_id, type, offensive_score, defensive_score)
                    VALUES (%s, %s, %s, %s)
                    """, (
                        pokemon_id,
                        type_name,
                        row['type_matchups']['offensive'][type_name],
                        row['type_matchups']['defensive'][type_name]
                    ))
            
            # Verify data was inserted
            cursor.execute("SELECT COUNT(*) FROM pokemon WHERE team_id = %s", (team_id,))
            pokemon_count = cursor.fetchone()[0]
            print(f"✅ Successfully inserted {pokemon_count} Pokémon for team {team_id}")
        
        connection.close()
        print("✅ Database connection closed.")
        
    except Exception as e:
        print(f"❌ Database error: {str(e)}")
        raise e