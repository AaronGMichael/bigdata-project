import os
from datetime import date

import pandas as pd
import requests
from fastf1.ergast import Ergast
import fastf1
import json

DATALAKE_ROOT_FOLDER = "../datalake/"


def fetch_lapdata_from_db(**kwargs):
    ergast = Ergast()
    changed = False
    fileList = []
    for year in range(2022, date.today().year):
        response_frame = ergast.get_circuits(season=year)
        for round, circuit in enumerate(response_frame['circuitId'], start=1):
            session = fastf1.get_session(year=year, gp=round, identifier='Race')
            session.load(laps=True, telemetry=False, weather=False, messages=False, livedata=None)
            for driver in session.drivers:
                laps = session.laps.pick_driver(driver)
                driver = session.get_driver(driver)
                driverName = driver.DriverId
                teamName = driver.TeamId
                TARGET_PATH = DATALAKE_ROOT_FOLDER + f"raw/lapData/{year}/{circuit}/{teamName}/{driverName}/"
                if not os.path.exists(TARGET_PATH):
                    os.makedirs(TARGET_PATH)
                # open(TARGET_PATH + f'{circuit}-{year}-{driverName}-lapdata.json', 'w').write(laps.to_json())
                laps.to_csv(TARGET_PATH + "laps.csv", index=False)
                fileList.append(TARGET_PATH + "laps.csv")
                changed = True
    return fileList

def fetch_pitdata_from_db(**kwargs):
    ergast = Ergast()
    changed = False
    fileList = []
    for year in range(2022, date.today().year):
        response_frame = ergast.get_circuits(season=year)
        for round, circuit in enumerate(response_frame['circuitId'], start=1):
            pits = ergast.get_pit_stops(season=year, round=round, auto_cast=False, result_type='raw')
            df = pd.DataFrame(pits[0]["PitStops"])
            TARGET_PATH = DATALAKE_ROOT_FOLDER + f"raw/pitData/{year}/{circuit}/"
            if not os.path.exists(TARGET_PATH):
                os.makedirs(TARGET_PATH)
            df.to_json(TARGET_PATH + "pits.json", index=False)
            fileList.append(TARGET_PATH + "pits.json")
    return fileList
