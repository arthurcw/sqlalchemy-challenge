#################################################
# import dependency
#################################################
from flask import Flask, jsonify

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

import datetime as dt

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Temperature function
# take start and end dates and return min, avg and max temperature
#################################################
def temp_stats(session, start_date, end_date):
    """TMIN, TAVG, and TMAX between start and end dates.
    Args:
        session: sqlalchemy Session
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
    Returns:
        TMIN, TAVG, and TMAX
    """
    
    return session.query(
        func.min(Measurement.tobs), 
        func.avg(Measurement.tobs), 
        func.max(Measurement.tobs))\
            .filter(Measurement.date.between(start_date, end_date))\
            .all()

#################################################
# Custom Error
#################################################
class DataDateError(Exception):
    """Raised if user-entered data is later than available data"""
    pass

#################################################
# flask setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # find date range of data
    (d_earliest, d_latest) = session.query(
        func.min(Measurement.date),
        func.max(Measurement.date)
    ).first()

    session.close()

    return(
        f""" <h1>Welcome to the Climate App<br></h1>
            Data can be accessed with the following APIs<br>
            <table cellspacing='10'>
                <thead>
                    <tr>
                        <th>API</th>
                        <th>Description</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td><a href="/api/v1.0/precipitation">/api/v1.0/precipitation</a></td>
                        <td>return precipitation data for the last year in dataset</td>
                    </tr>
                    <tr>
                        <td><a href="/api/v1.0/stations">/api/v1.0/stations</a></td>
                        <td>return list of stations</td>
                    </tr>
                    <tr>
                        <td><a href="/api/v1.0/tobs">/api/v1.0/tobs</a></td>
                        <td>return temperature data for the most active station (USC005519281) for the last year in dataset</td>
                    </tr>
                    <tr>
                        <td>/api/v1.0/&ltstart&gt</td>
                        <td>return min, average and max temperature from given start date "YYYY-mm-dd" to end of dataset</td>
                    </tr>
                    <tr>
                        <td>/api/v1.0/&ltstart&gt/&ltend&gt</td>
                        <td>return min, average and max temperature for given start date to end date</td>
                    </tr>
                </tbody>
                <tfoot>
                    <tr>
                        <td colspan='2'> Data available from {d_earliest} to {d_latest}</td>
                    </tr>
                </tfoot>
            </table>
            """
    )

@app.route("/api/v1.0/precipitation")
def precip():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # find date of latest data
    d_latest = session.query(func.max(Measurement.date)).scalar()
    d_latest = dt.datetime.strptime(d_latest, "%Y-%m-%d")
    d_1yr = dt.datetime(
        d_latest.year - 1,
        d_latest.month,
        d_latest.day)

    # Query all precip data
    results = session.query(
        Measurement.date,
        Measurement.prcp)\
        .filter(Measurement.date > d_1yr)\
        .order_by(Measurement.date)\
        .all()

    session.close()

    # Create a dictionary of date and prcp
    prcp_list = [
        {'date': d, 'prcp': p} 
        for d, p in results
        ]
    return jsonify(prcp_list)

@app.route("/api/v1.0/stations")
def station():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query station data
    results = session.query(Station).all()

    session.close()

    # Create a list of stations
    station_list = [
        {
            'id': item.id,
            'station': item.station,
            'name': item.name,
            'latitude': item.latitude,
            'longitude': item.longitude,
            'elevation': item.elevation
        }
        for item in results
    ]  
    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find most active station
    n_station = session.query(
        Measurement.station, 
        func.count(Measurement.station)
        )\
        .group_by(Measurement.station)\
        .order_by(func.count(Measurement.station).desc())\
        .all()
    station_id = n_station[0][0]

    # find date of latest data
    d_latest = session.query(func.max(Measurement.date))\
        .filter(Measurement.station==station_id)\
        .scalar()
    d_latest = dt.datetime.strptime(d_latest, "%Y-%m-%d")
    d_1yr = dt.datetime(
        d_latest.year - 1,
        d_latest.month,
        d_latest.day)

    # Query station temperature data
    results = session.query(
        Measurement.station,
        Measurement.date,
        Measurement.tobs
        )\
        .filter(Measurement.station==station_id)\
        .filter(Measurement.date>d_1yr)\
        .order_by(Measurement.date)\
        .all()

    session.close()

    # Create list of temperature data
    temp_data = [
        {'station': s, 'date': d, 'temperature': t} 
        for s, d, t in results
    ]
    return jsonify(temp_data)

@app.route("/api/v1.0/<start>")
def tobs_start(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # find latest date of data
    d_latest = session.query(func.max(Measurement.date))\
        .scalar()

    try:
        # Check start date is earlier than available data date
        if dt.datetime.strptime(start, "%Y-%m-%d") > \
            dt.datetime.strptime(d_latest, "%Y-%m-%d"):
            raise DataDateError

        # Create our session (link) from Python to the DB
        session = Session(engine)

        # Create query
        [(TMin, TAvg, TMax)] = temp_stats(session, start, d_latest)

        session.close()

        # Return min, max and average temperature
        return(
            f"""
                Temperature statistics between {start} and {d_latest}<br>
                Minimum: {TMin}<br>
                Maximum: {TMax}<br>
                Average: {round(TAvg,1)}
            """
        )
    except ValueError:
        session.close()
        return(f"Please enter a valid date format YYYY-mm-dd")

    except DataDateError:
        session.close()
        return(
            f"Entered date is later than data available<br>"
            f"Please enter a date earlier than {d_latest}"
        )
    
@app.route("/api/v1.0/<start>/<end>")
def tobs_start_end(start, end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # find range of date in data
    [(d_earliest, d_latest)] = session.query(
        func.min(Measurement.date),
        func.max(Measurement.date)
        ).all()

    try:
        # Switch start and end date if start date is later
        if (dt.datetime.strptime(start, "%Y-%m-%d") > \
            dt.datetime.strptime(end, "%Y-%m-%d")):
            start, end = end, start

        # Check if start/end dates are within available data date
        if (dt.datetime.strptime(start, "%Y-%m-%d") > \
            dt.datetime.strptime(d_latest, "%Y-%m-%d") \
            and dt.datetime.strptime(end, "%Y-%m-%d") < \
            dt.datetime.strptime(d_earliest, "%Y-%m-%d")):
            raise DataDateError

        if (dt.datetime.strptime(start, "%Y-%m-%d") < \
            dt.datetime.strptime(d_earliest, "%Y-%m-%d")):
            start = d_earliest
        
        if (dt.datetime.strptime(end, "%Y-%m-%d") > \
            dt.datetime.strptime(d_latest, "%Y-%m-%d")):
            end = d_latest

        # Create our session (link) from Python to the DB
        session = Session(engine)

        # Create query
        [(TMin, TAvg, TMax)] = temp_stats(session, start, end)

        session.close()

        # Return min, max and average temperature
        return(
            f"""
                Temperature statistics between {start} and {end}<br>
                Minimum: {TMin}<br>
                Maximum: {TMax}<br>
                Average: {round(TAvg,1)}
            """
        )
    except ValueError:
        session.close()
        return(f"Please enter a valid date format YYYY-mm-dd")        

    except DataDateError:
        session.close()
        return(
            f"Start and end dates are outside available date range<br>"
            f"Please enter a period between {d_earliest} and {d_latest}"
        )

if __name__ == "__main__":
    app.run(debug=True)