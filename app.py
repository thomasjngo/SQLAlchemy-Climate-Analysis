# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt
import json
from datetime import datetime, timedelta

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(bind=engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/<start>/<end>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Code from climate_starter
    most_recent_date_row = session.query(func.max(measurement.date)).first()
    most_recent_date = most_recent_date_row[0]
    one_year_ago = datetime.strptime(most_recent_date, '%Y-%m-%d') - timedelta(days=366)
    last_year_data = session.query(measurement.date, measurement.prcp).filter(measurement.date > one_year_ago).all()
    df_precip = pd.DataFrame(last_year_data, columns=['Date', 'Precipitation'])

    # Convert the precip dataframe to a dictionary
    precip_data = []
    for date, precipitation in last_year_data:
        precip_dict = {}
        precip_dict["Date"] = date
        precip_dict["Precipitation"] = precipitation
        precip_data.append(precip_dict)

    return jsonify(precip_data)


@app.route("/api/v1.0/stations")
def stations():
    # Repurposed code from climate_starter and converted to json
    active_stations = session.query(measurement.station, func.count(measurement.station)) \
                                    .group_by(measurement.station) \
                                    .order_by(func.count(measurement.station).desc()) \
                                    .all()
    
    stations_list = [{'station': row.station} for row in active_stations]
    
    return jsonify(stations_list)


@app.route("/api/v1.0/tobs")
def tobs():
    #Getting the most active station
    most_recent_date_row = session.query(func.max(measurement.date)).first()
    most_recent_date = most_recent_date_row[0]
    one_year_ago = datetime.strptime(most_recent_date, '%Y-%m-%d') - timedelta(days=366)
    last_year_data = session.query(measurement.date, measurement.prcp).filter(measurement.date > one_year_ago).all()
    
    active_stations = session.query(measurement.station, func.count(measurement.station)) \
                                .group_by(measurement.station) \
                                .order_by(func.count(measurement.station).desc()) \
                                .all()
    
    df_active = pd.DataFrame(active_stations, columns=['station', 'observation_count'])
    df_active

    # Find the max count and return the station
    most_active_station = df_active.loc[df_active['observation_count'].idxmax()]
    most_active_station_id = most_active_station['station']

    # Query to retrieve the temperature observations for the most-active station within the date range
    tobs_results = session.query(measurement.date, measurement.tobs).\
        filter(measurement.station == most_active_station_id).\
        filter(measurement.date >= one_year_ago).filter(measurement.date <= most_recent_date).all()

    temp_observation_data = []
    for result in tobs_results:
        temp_observation_data.append({"date": result.date, "tobs": result.tobs})
    
    return jsonify(temp_observation_data)


@app.route("/api/v1.0//<start>")
def temp_start(start):
    start_date = dt.datetime.strptime(start, "%Y-%m-%d")

    # Take all data inclusive of the start date and later
    results = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)) \
        .filter(measurement.date >= start_date) \
        .all()
    
    temp_stats = list(results[0])
    
    return jsonify({"TMIN": temp_stats[0], "TAVG": temp_stats[1], "TMAX": temp_stats[2]})


@app.route("/api/v1.0//<start>/<end>")
def temp_start_end(start, end):
    start_date = dt.datetime.strptime(start, "%Y-%m-%d")
    end_date = dt.datetime.strptime(end, "%Y-%m-%d")
    
    # Take all data inclusive of start date and end date
    results = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)) \
        .filter(measurement.date >= start_date) \
        .filter(measurement.date <= end_date) \
        .all()
    
    temp_stats = list(results[0])
    
    return jsonify({"TMIN": temp_stats[0], "TAVG": temp_stats[1], "TMAX": temp_stats[2]})


if __name__ == '__main__':
    app.run(debug=True)
