import os
from datetime import date

import requests
from fastf1.ergast import Ergast
import json

DATALAKE_ROOT_FOLDER = "../datalake/"


def fetch_lapdata_from_db(**kwargs):
    ergast = Ergast()
    changed = False
    for year in range(2022, date.today().year):
        response_frame = ergast.get_circuits(season=year)
        for round, circuit in enumerate(response_frame['circuitId'], start=1):
            constructors = ergast.get_constructor_info(circuit=circuit, season=year)
            for constructor in constructors['constructorId']:
                drivers = ergast.get_driver_info(circuit=circuit, season=year, constructor=constructor)
                for driver in drivers['driverId']:
                    TARGET_PATH = DATALAKE_ROOT_FOLDER + f"raw/lapData/{year}/{circuit}/{constructor}/{driver}/"
                    if not os.path.exists(TARGET_PATH):
                        print(f"Downloading {circuit}/{constructor}/{driver} to {TARGET_PATH}")
                        laps = ergast.get_lap_times(season=year, round=round, driver=driver, result_type='raw',
                                                    auto_cast=False)
                        os.makedirs(TARGET_PATH)
                        open(TARGET_PATH + f'{circuit}-{year}-{driver}-lapdata.json', 'w').write(json.dumps(laps))
                        changed = True
    return changed


fetch_lapdata_from_db()
