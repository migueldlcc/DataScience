# %% [markdown]
# This code creates interpolation data for incidents between the specified
# start_date and end_date, and saves it as csv file in the folder:
# /fromIncidentToInterpolationData/incidentInterpolationData

# %%

import matplotlib.pyplot as plt
import pandas as pd
import json
from sqlalchemy import create_engine

# %%
driver='postgresql'
username='dab_ds23241a_223'
dbname=username # it is the same as the username
password='Bh6KFgcWazMMCPvZ'
server='bronto.ewi.utwente.nl'
port='5432'

# Create an engine instance
alchemy_engine = create_engine(f'{driver}://{username}:{password}@{server}:{port}/{dbname}')

# Connect to PostgreSQL server
db_connection = alchemy_engine.connect()

# Read the data into a_df
# a_df = pd.read_sql(f"select * from \"project\".\"No_incidents_segments\"", db_connection)
a_df = pd.read_csv("/Users/vree/GithubRepos/DataScience/basFiles/3hoursFromIncidentToInterpolationData copy/road_segments.csv")

# Check if 'before' and 'after' columns are already dictionaries
if a_df['before'].apply(type).eq(dict).all() and a_df['after'].apply(type).eq(dict).all():
    print('Columns are already dictionaries.')
else:
    # Convert the 'before' and 'after' columns from JSON strings back to dictionaries if they are not already
    a_df['before'] = a_df['before'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    a_df['after'] = a_df['after'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
# Close the database connection
db_connection.close()



# %% [markdown]
# Presets, agreed upon for all diagrams

# %%
x_incident = 3000
t_incident = 60
time_recording_before_incident = 60
time_recording_after_incident = 180

mimimal_number_of_cameras = 6


# filter dates between 15th of March 2020 and 21th of March 2020
start_date = '2020-03-22'
end_date = '2020-03-29'

# the filename for the speedflow data 
file_name = f"/Users/vree/Documents/UTwente/Masters UTwente/Data Science/DM Project/Speed-flow/Maart/intensiteit-snelheid-rotterdam-22-29-Maart/intensiteit-snelheid-rotterdam-22-29-Maart.csv"

start_dates = ['2020-03-01', '2020-03-08', '2020-03-15', '2020-03-22']
end_dates = ['2020-03-07', '2020-03-14', '2020-03-21', '2020-03-29']
file_names = [f"/Users/vree/Documents/UTwente/Masters UTwente/Data Science/DM Project/Speed-flow/Maart/intensiteit-snelheid-rotterdam-1-7-Maart/intensiteit-snelheid-rotterdam-1-7-Maart.csv",
              f"/Users/vree/Documents/UTwente/Masters UTwente/Data Science/DM Project/Speed-flow/Maart/intensiteit-snelheid-rotterdam-8-14-Maart/intensiteit-snelheid-rotterdam-8-14-Maart.csv",
              f"/Users/vree/Documents/UTwente/Masters UTwente/Data Science/DM Project/Speed-flow/Maart/intensiteit-snelheid-rotterdam-15-21-Maart/intensiteit-snelheid-rotterdam-15-21-Maart.csv",
              f"/Users/vree/Documents/UTwente/Masters UTwente/Data Science/DM Project/Speed-flow/Maart/intensiteit-snelheid-rotterdam-22-29-Maart/intensiteit-snelheid-rotterdam-22-29-Maart.csv"]

file_name_for_saving = file_name.split("/")[-1].split(".")[0]

file_names_for_saving = [file_name.split("/")[-1].split(".")[0] for file_name in file_names]


# %%
# returns cameras and their true x for an incident
#  so a list of camera ids and their x values
def create_x_variable_for_incident_cameras(row):
    locations_before = row['before']
    locations_after = row['after']
    
    if isinstance(locations_before, str):
        locations_before = json.loads(locations_before)
    if isinstance(locations_after, str):
        locations_after = json.loads(locations_after)
    
    # create dataframe with distance before, where the row entry is a json with a key
    # for camera id and the value is the distance
    before_cameras_data = []
    
    before_cameras_data = [{'camera_id': key, 'x': x_incident - (value)} for key, value in locations_before.items()]
    
    before_cameras_x = pd.DataFrame(before_cameras_data)
    
    # create dataframe with distance after, where the row entry is a json with a key
    # for camera id and the value is the distance
    after_cameras_data = []
    
    after_cameras_data = [{'camera_id': key, 'x': x_incident + (value)} for key, value in locations_after.items()]
        
    after_cameras_x = pd.DataFrame(after_cameras_data)
    df = pd.concat([before_cameras_x, after_cameras_x])
    print(df.head(3))
    return df

# times must be delivered as pd.datetime
def speed_flow_data_for_incident(camera_ids, start_time, end_time):
    # read speed flow data in chunks of 10000 and filter on camera id and time
    # only keep start_meetperiode, id_meetlocatie, gem_snelheid
    
    
    chunksize = 10000
    chunks = []
    for chunk in pd.read_csv(file_name, chunksize=chunksize):
        chunk = chunk[(chunk['id_meetlocatie'].isin(camera_ids)) & (pd.to_datetime(chunk['start_meetperiode']) >= start_time) & (pd.to_datetime(chunk['start_meetperiode']) <= end_time)]
        chunk = chunk[['start_meetperiode', 'id_meetlocatie', 'gem_snelheid']]
        chunks.append(chunk)
    return pd.concat(chunks)

# aggregate duplicates where x and t are the same, and take the average of v
def aggregate_data(data):
    return data.groupby(['x', 't']).agg({'v': 'mean'}).reset_index()

# retrieving a list of cameras and their x values
def create_interpolation_data_for_incident(incident_row):
    print("incident row: ", incident_row)

    cameras_x_df = create_x_variable_for_incident_cameras(incident_row)
    
        
    camera_ids = cameras_x_df['camera_id'].tolist()

    incident_time = pd.to_datetime(incident_row['starttijd'])
    start_time_for_diagram = incident_time - pd.Timedelta(minutes=time_recording_before_incident)
    end_time_for_diagram = incident_time + pd.Timedelta(minutes=time_recording_after_incident)
    # returns df with start_meetperiode, id_meetlocatie, gem_snelheid
    speedflow_data = speed_flow_data_for_incident(camera_ids, start_time_for_diagram, end_time_for_diagram)

    print("speed flow data for incident was: ", speedflow_data.head())

    # let the start_meetperiode be the time in minutes since the incident
    speedflow_data['start_meetperiode'] = ((pd.to_datetime(speedflow_data['start_meetperiode']) - incident_time).dt.total_seconds() / 60.0)+t_incident

    merged_data = pd.merge(cameras_x_df, speedflow_data, left_on='camera_id', right_on='id_meetlocatie')

    # rename columns: x->x, gem_snelheid->v, start_meetperiode->t
    merged_data = merged_data.rename(columns={'x': 'x', 'gem_snelheid': 'v', 'start_meetperiode': 't'})
    
    return merged_data

import os


def check_number_of_cameras(incident_row):
    locations_before = incident_row['before']
    locations_after = incident_row['after']
    
    return len(locations_before) + len(locations_after) > mimimal_number_of_cameras

def create_x_t_v_for_incident_row(incident_row, file_name_for_saving):

    incident_data = create_interpolation_data_for_incident(incident_row)
    

    # only keep the columns x, v, t
    incident_data = incident_data[['x', 'v', 't']]

    # there's multiple measurements within a minute, which lead to duplicates for x, t . 
    incident_data = aggregate_data(incident_data)
    # show number of rows incident data
    print('number of rows incident data: ', len(incident_data))

    incident_data_remove_negatives = incident_data[incident_data['v'] >= 0]
    # show number of rows after removing negatives
    print('number of rows after removing negatives; ', len(incident_data_remove_negatives))

    # Check if the directory exists, if not, create it
    base_path = 'noIncidents/incidentInterpolationData/'
    full_path = f"{base_path}/{file_name_for_saving}"
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    # write the data to a csv file in ./incidentInterpolationData/, use the incident id as filename
    incident_data_remove_negatives.to_csv(f'noIncidents/incidentInterpolationData/{file_name_for_saving}/{incident_row["id"]}.csv', index=False)
    print("created csv for incident: ", incident_row["id"])
    
    return incident_data_remove_negatives



 
    
    
    
    
    

# %%
import logging

# Set up basic configuration for logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("debug.log"),
                        logging.StreamHandler()
                    ])

# Log some messages
logging.info('This is an info message')
logging.warning('This is a warning message')


# %% [markdown]
# non-parallel

# %%

# import logging

# # Set up logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # Variables to keep track of statistics
# total_incidents_processed = 0
# incidents_with_insufficient_cameras = 0

# for i in range(0, len(start_dates)):
#     logging.info(f'Processing date range {i+1}/{len(start_dates)}: from {start_dates[i]} to {end_dates[i]}')
    
#     start_date = start_dates[i]
#     end_date = end_dates[i]
#     file_name = file_names[i]
#     file_name_for_saving = file_names_for_saving[i]
    
#     current_df = a_df[(a_df['starttijd'] >= start_date) & (a_df['starttijd'] <= end_date)]
#     incidents_count = len(current_df)
#     logging.info(f'Found {incidents_count} incidents in this date range.')
    
#     for j in range(0, incidents_count):
#         incident_row = current_df.iloc[j]
#         total_incidents_processed += 1
        
#         if check_number_of_cameras(incident_row):
#             create_x_t_v_for_incident_row(incident_row, file_name_for_saving)
#         else:
#             incidents_with_insufficient_cameras += 1
#             logging.warning(f'incident {incident_row["id"]} has too few cameras')

# # Provide summary after all processing
# logging.info(f'Total incidents processed: {total_incidents_processed}')
# logging.info(f'Incidents with insufficient cameras: {incidents_with_insufficient_cameras}')

# # If critical issues need immediate attention
# if incidents_with_insufficient_cameras > 0:
#     logging.critical('Some incidents have insufficient cameras and need review.')



# %% [markdown]
# Parallel

# %%


# %%
# Helper function to process each date range
def process_date_range(args):
    start_date, end_date, file_name, file_name_for_saving, df = args
    # set start_date to datetime object which can be used for comparison
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    
    incidents_with_insufficient_cameras = 0
    total_incidents_processed = 0
    
    logging.info(f'Processing from {start_date} to {end_date}')
    current_df = df[(pd.to_datetime(df['starttijd']) >= start_date) & (pd.to_datetime(df['starttijd'] <= end_date))]
    incidents_count = len(current_df)
    
    for j in range(incidents_count):
        incident_row = current_df.iloc[j]
        total_incidents_processed += 1
        
        if check_number_of_cameras(incident_row):
            create_x_t_v_for_incident_row(incident_row, file_name_for_saving)
        else:
            incidents_with_insufficient_cameras += 1
            logging.warning(f'incident {incident_row["id"]} has too few cameras')
    
    return total_incidents_processed, incidents_with_insufficient_cameras

# %%
import logging
from multiprocessing import get_context, Manager
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def main():
    start_dates = [...]  # your start dates
    end_dates = [...]    # your end dates
    file_names = [...]   # input file names
    file_names_for_saving = [...]  # output file names

    manager = Manager()
    args_list = [(start_dates[i], end_dates[i], file_names[i], file_names_for_saving[i], a_df) for i in range(len(start_dates))]

    ctx = get_context("spawn")
    with ctx.Pool(processes=4) as pool:
        results = pool.map(process_date_range, args_list)

    total_processed = sum(result[0] for result in results)
    total_insufficient = sum(result[1] for result in results)

    logging.info(f'Total incidents processed: {total_processed}')
    logging.info(f'Incidents with insufficient cameras: {total_insufficient}')

if __name__ == '__main__':
    main()



