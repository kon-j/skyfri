import sys
import json
import uuid
import sqlite3
from glob import glob
import pandas as pd

from src.utils import format_name


class Reporter:
    """
    Load data from pre-loaded JSONs into db 
    and calculate metrics
    """
    def __init__(self, db_name: str,
                 folder_name: str,
                 batch_size: int,
                 output_file_duration: str,
                 output_file_pairs: str) -> None:
        self.batch_sz = batch_size
        self.folder_name = folder_name
        self.output_file_pairs = output_file_pairs
        self.output_file_duration = output_file_duration
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()

    def __close_connection(self):
        self.cursor.close()
        self.connection.close()

    def __insert_stations(self, data: list) -> None:
        q = """insert or ignore  into dim_stations values (?,?)"""
        stations = dict()
        for entry in data:
            stations[entry["start_station_id"]] = format_name(entry["start_station_name"])
            stations[entry["end_station_id"]] = format_name(entry["end_station_name"])
        stations = [(k,v) for k,v in stations.items()]

        for i in range(1 + len(stations) // self.batch_sz):
            batch = stations[i*self.batch_sz:self.batch_sz*(i+1)]
            self.cursor.executemany(q, batch)
        self.connection.commit()

    def __insert_rides(self, data: list) -> None:
        q = """insert into fct_rides values (?,?,?,?,?)"""
        data = [(str(uuid.uuid4()),
                x['start_station_id'],
                x['end_station_id'],
                x['started_at'][:23],
                x['duration']) for x in data]
        for i in range(1 + len(data) // self.batch_sz):
            batch = data[i*self.batch_sz:self.batch_sz*(i+1)]
            self.cursor.executemany(q, batch)
        self.connection.commit()

    def load_data(self):
        """
        Load data from json files to SQL db 
        """
        file_names = glob(f"{self.folder_name}/*/*")
        for i, file_name in enumerate(file_names):
            with open(file_name, "r") as file:
                data = json.load(file)
            self.__insert_stations(data)
            self.__insert_rides(data)
            sys.stdout.write(f"\rFinished processing {i+1:2d}/{len(file_names)} raw files")

    def __get_average_duration(self):
        q = "select sum(duration_sec)/count(*) from fct_rides where strftime('%Y', datetime)='2022'"
        self.cursor.execute(q)
        average_duration = self.cursor.fetchall()[0][0]
        print("AVG duration:", average_duration)
        average_duration = f"{int(average_duration//60)}m:{int(average_duration%60)}s"
        df = pd.DataFrame([{"average_duration": average_duration}])
        df.to_csv(f"{self.output_file_duration}.csv", index=False)

    def __get_frequent_pairs(self):
        q = """
            with t1 as (select start_station_id, end_station_id, count(*) count 
            from fct_rides 
            group by start_station_id, end_station_id
            order by count desc)
            select stations1.name start_station, stations2.name end_station, t1.count from t1 
                inner join dim_stations stations1 on t1.start_station_id=stations1.id
                inner join dim_stations stations2 on t1.end_station_id=stations2.id

        """
        self.cursor.execute(q)
        most_frequent_pairs = self.cursor.fetchall()
        df = pd.DataFrame(most_frequent_pairs)
        df.columns = [x[0] for x in self.cursor.description]
        df.to_csv(f"{self.output_file_pairs}.csv", index=False)

    def prepare_reports(self):
        """
        Prepare reports for average duration 
        and most frequent pairs
        """
        self.__get_average_duration()
        self.__get_frequent_pairs()
        self.__close_connection()
