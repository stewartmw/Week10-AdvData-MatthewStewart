# Bulk of the queries are identical to accompanying jupyter notebook.
# See that file for additional comments and analysis.

# Dependencies
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, inspect, func
import pandas as pd
import datetime as dt
from flask import Flask, jsonify

# Create engine
engine = create_engine('sqlite:///Resources/hawaii.sqlite')

# Reflect the database tables
Base = automap_base()
Base.prepare(engine, reflect = True)

# Assign the new classes to variables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Flask routes
app = Flask(__name__)

@app.route("/")
# Home Page
def welcome():
    return (
        f"Welcome to my DU Climate Analysis API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/YYYYmmdd   ...example: /api/v1.0/20180601<br/>"
        f"/api/v1.0/YYYYmmdd/YYYYmmdd   ...example: /api/v1.0/20180601/20180608"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Precipitation data query - matches jupyter notebook
    session = Session(engine)
    prcp_table = (session
                    .query(Measurement.date, Measurement.prcp)
                    .filter(Measurement.date >= '2016-08-23') # Dates calculated in jupyter notebook
                    .filter(Measurement.date <= '2017-08-23')
                    .order_by(Measurement.date)
                    .all())
    prcp_df = pd.DataFrame(prcp_table, columns = ['Date', 'Precipitation'])
    prcp_df.set_index('Date', inplace = True)

    # Convert prcp_df to dictionary - have to first transpose the dataframe with .T
    prcp_dict = prcp_df.T.to_dict('list')
    
    return jsonify(prcp_dict)

@app.route("/api/v1.0/stations")
def stations():
    # Stations data query - matches jupyter notebook
    session = Session(engine)
    station_table = (session
                        .query(Station.id, Station.station, Station.name,
                               Station.latitude, Station.longitude, Station.elevation)
                        .all())
    # Create list of dictionaries that display information for all stations
    station_list = []
    for id, station, name, latitude, longitude, elevation in station_table:
        station_dict = {}
        station_dict["id"] = id
        station_dict["station"] = station
        station_dict["name"] = name
        station_dict["latitude"] = latitude
        station_dict["longitude"] = longitude
        station_dict["elevation"] = elevation
        station_list.append(station_dict)
    
    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    # tobs query (filtered by Station with the most observations)
    session = Session(engine)
    station_observations = (session
                            .query(Measurement.station, func.count(Measurement.tobs))
                            .group_by(Measurement.station)
                            .order_by(func.count(Measurement.tobs).desc())
                            .all())
    station_observations_df = pd.DataFrame(station_observations, columns = ['Station', 'Num_of_Observations'])

    tobs_data = (session
                    .query(Measurement.date, Measurement.tobs)
                    .filter(Measurement.station == station_observations_df.Station[0])
                    .filter(Measurement.date >= '2016-08-23') # Dates calulcated in jupyter notebook
                    .filter(Measurement.date <= '2017-08-23') # Dates calculated in jupyter notebook
                    .order_by(Measurement.date)
                    .all())
    tobs_df = pd.DataFrame(tobs_data, columns = ['Date', 'Temp_Observations'])
    tobs_df.set_index('Date', inplace = True)

    # Convert tobs_df to dictionary - first have to transpose dataframe using .T
    tobs_dict = tobs_df.T.to_dict('list')
    
    return jsonify(tobs_dict)

@app.route("/api/v1.0/<user_start>")
# Display min, max, and avg temp for each date from user's input to final date (8/23/2017)
def date_range_start_only(user_start):
    session = Session(engine)
    
    # Use try-except to protect from user format error
    try:
        loop_date = dt.datetime.strptime(str(user_start), '%Y%m%d')

        results_list = []

        # Make sure user input is within the data range of dates
        if loop_date > dt.datetime(2017, 8, 23):
            return jsonify({'error': 'no data beyond 2017-08-23'})
        
        # Create list of dictionaries that displays temperature data for all applicable dates
        else:
            while loop_date <= dt.datetime(2017, 8, 23):
                loop_date_string = loop_date.strftime('%Y-%m-%d')
                results = (session
                            .query(func.min(Measurement.tobs),
                                func.avg(Measurement.tobs),
                                func.max(Measurement.tobs))
                            .filter(Measurement.date == loop_date_string)
                            .all())
                for TMIN, TAVE, TMAX in results:
                    results_dict = {
                        loop_date_string: {
                            'Low_Temp': f'{TMIN} degrees Fahrenheit',
                            'Avg_Temp': f'{TAVE} degrees Fahrenheit',
                            'High_Temp': f'{TMAX} degrees Fahrenheit'
                        }
                    }
                    results_list.append(results_dict)
                loop_date += dt.timedelta(days = 1)
        return jsonify(results_list)
    except:
        return jsonify({'error': 'date given must be in format YYYYmmdd'})

@app.route("/api/v1.0/<range_start>/<range_end>")
# Same as above, except user now specifies the end date in addtion to the start date
def date_range(range_start, range_end):
    session = Session(engine)
    
    try:
        loop_date = dt.datetime.strptime(str(range_start), '%Y%m%d')
        loop_end = dt.datetime.strptime(str(range_end), '%Y%m%d')

        results_list = []

        if (loop_date > dt.datetime(2017, 8, 23)) or \
        (loop_end > dt.datetime(2017, 8, 23)):
            return jsonify({'error': 'no data beyond 2017-08-23'})
        elif loop_date > loop_end:
            return jsonify({'error': 'start date must be prior to end date'})
        else:
            while loop_date <= loop_end:
                loop_date_string = loop_date.strftime('%Y-%m-%d')
                results = (session
                            .query(func.min(Measurement.tobs),
                                func.avg(Measurement.tobs),
                                func.max(Measurement.tobs))
                            .filter(Measurement.date == loop_date_string)
                            .all())
                for TMIN, TAVE, TMAX in results:
                    results_dict = {
                        loop_date_string: {
                            'Low_Temp': f'{TMIN} degrees Fahrenheit',
                            'Avg_Temp': f'{TAVE} degrees Fahrenheit',
                            'High_Temp': f'{TMAX} degrees Fahrenheit'
                        }
                    }
                    results_list.append(results_dict)
                loop_date += dt.timedelta(days = 1)
        return jsonify(results_list)
    except:
        return jsonify({'error': 'dates given must be in format YYYYmmdd'})

if __name__ == "__main__":
    app.run(debug=True)