#!/home/edd/.local/share/virtualenvs/flights_downloader-NCu1d9md/bin/python


import os
import logging
import zipfile
import shutil

import tempfile
import tqdm
import itertools as it
import urllib.request as request
import google.cloud.storage as storage

download_dir = os.path.join(os.getcwd(), "data/raw")
expected_header = 'FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,' \
                  + 'ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,DEST_AIRPORT_ID,' \
                  + 'DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME,DEP_TIME,' \
                  + 'DEP_DELAY,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,' \
                  + 'ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,DISTANCE'

# Exceptions


class DataUnavailable(Exception):
    def __int__(self,message):
        self.message = message


class UnexpectedFormat(Exception):
    def __init__(self, message):
        self.message = message


def ingest_pipeline(years,
                    months,
                    bucket,
                    temp_prefix="ingest_flights",
                    remove_old=True
                    ):
    """
    pipeline function to batch download and ingest files to google cloud
    Args:
        years (iterable): years of data we dowload for
        months:
        bucket:
        temp_prefix:

    Returns:

    """
    temp_dir = tempfile.mkdtemp(prefix=temp_prefix)
    locations = []
    for year, month in tqdm.tqdm(it.product(years, months)):

        try:
            logging.info(f"Ingesting data for {month}-{year}")
            zip_file = download_flights_data(year, month, temp_dir)
            csv_file = unzip_file(zip_file, temp_dir)
            cleaned_csv_file = clean_csv(csv_file,
                                         year,
                                         month,
                                         remove_old
                                         )
            try:
                verify_ingest(cleaned_csv_file)
            except DataUnavailable as error:
                logging.info(f"Try again later: {error.message}")

            gcs_loc = f"flights/raw/{os.path.basename(cleaned_csv_file)}"
            location = upload_to_cloud(cleaned_csv_file, bucket, gcs_loc)
            logging.info(f"complete, uploaded to {location}")
            locations.append(location)

        finally:
            logging.info("cleaning up")
            shutil.rmtree(temp_dir)

    return locations


def upload_to_cloud(csv_file, bucket_name, blob_name):
    """
    uploads csv file to specified bucket and blob in google cloud

    Args:
        csv_file: path of file to upload
        bucket_name: name of bucket
        blob_name: name of blob

    Returns:
        location of the csv_file after uploading to google cloud

    """

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = storage.Blob(blob_name, bucket)
    gcs_location = f"gs://{bucket_name}/{blob_name}"
    logging.info(f"Uploading {csv_file} to google cloud bucket: {bucket} blob: {blob}")
    blob.upload_from_file(csv_file)
    logging.info(f"...uploaded to {gcs_location}")
    return gcs_location


def download_flights_data(year, month, save_path=download_dir):
    """
    Downloads flight timeliness data, it. the ONTIME files

    Args:
        year (string): year of the data we want to download
        month (string): month of the data we want to download e.g '01' for January
        save_path: where we save the data

    Returns:
        local file names where data has been downloaded

    """
    logging.info(f"Requesting data for {year} {month}")

    params = f"UserTableName=On_Time_Performance&DBShortName=&RawDataTable=T_ONTIME&sqlstr=+SELECT+FL_DATE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CCARRIER%2CFL_NUM%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CCRS_DEP_TIME%2CDEP_TIME%2CDEP_DELAY%2CTAXI_OUT%2CWHEELS_OFF%2CWHEELS_ON%2CTAXI_IN%2CCRS_ARR_TIME%2CARR_TIME%2CARR_DELAY%2CCANCELLED%2CCANCELLATION_CODE%2CDIVERTED%2CDISTANCE+FROM++T_ONTIME+WHERE+Month+%3D{month}+AND+YEAR%3D{year}&varlist=FL_DATE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CCARRIER%2CFL_NUM%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CCRS_DEP_TIME%2CDEP_TIME%2CDEP_DELAY%2CTAXI_OUT%2CWHEELS_OFF%2CWHEELS_ON%2CTAXI_IN%2CCRS_ARR_TIME%2CARR_TIME%2CARR_DELAY%2CCANCELLED%2CCANCELLATION_CODE%2CDIVERTED%2CDISTANCE&grouplist=&suml=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time=March&timename=Month&GEOGRAPHY=All&XYEAR={year}&FREQUENCY=3&VarDesc=Year&VarType=Num&VarDesc=Quarter&VarType=Num&VarDesc=Month&VarType=Num&VarDesc=DayofMonth&VarType=Num&VarDesc=DayOfWeek&VarType=Num&VarName=FL_DATE&VarDesc=FlightDate&VarType=Char&VarName=UNIQUE_CARRIER&VarDesc=UniqueCarrier&VarType=Char&VarName=AIRLINE_ID&VarDesc=AirlineID&VarType=Num&VarName=CARRIER&VarDesc=Carrier&VarType=Char&VarDesc=TailNum&VarType=Char&VarName=FL_NUM&VarDesc=FlightNum&VarType=Char&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID&VarType=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarDesc=OriginCityName&VarType=Char&VarDesc=OriginState&VarType=Char&VarDesc=OriginStateFips&VarType=Char&VarDesc=OriginStateName&VarType=Char&VarDesc=OriginWac&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName=DEST_AIRPORT_SEQ_ID&VarDesc=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID&VarDesc=DestCityMarketID&VarType=Num&VarName=DEST&VarDesc=Dest&VarType=Char&VarDesc=DestCityName&VarType=Char&VarDesc=DestState&VarType=Char&VarDesc=DestStateFips&VarType=Char&VarDesc=DestStateName&VarType=Char&VarDesc=DestWac&VarType=Num&VarName=CRS_DEP_TIME&VarDesc=CRSDepTime&VarType=Char&VarName=DEP_TIME&VarDesc=DepTime&VarType=Char&VarName=DEP_DELAY&VarDesc=DepDelay&VarType=Num&VarDesc=DepDelayMinutes&VarType=Num&VarDesc=DepDel15&VarType=Num&VarDesc=DepartureDelayGroups&VarType=Num&VarDesc=DepTimeBlk&VarType=Char&VarName=TAXI_OUT&VarDesc=TaxiOut&VarType=Num&VarName=WHEELS_OFF&VarDesc=WheelsOff&VarType=Char&VarName=WHEELS_ON&VarDesc=WheelsOn&VarType=Char&VarName=TAXI_IN&VarDesc=TaxiIn&VarType=Num&VarName=CRS_ARR_TIME&VarDesc=CRSArrTime&VarType=Char&VarName=ARR_TIME&VarDesc=ArrTime&VarType=Char&VarName=ARR_DELAY&VarDesc=ArrDelay&VarType=Num&VarDesc=ArrDelayMinutes&VarType=Num&VarDesc=ArrDel15&VarType=Num&VarDesc=ArrivalDelayGroups&VarType=Num&VarDesc=ArrTimeBlk&VarType=Char&VarName=CANCELLED&VarDesc=Cancelled&VarType=Num&VarName=CANCELLATION_CODE&VarDesc=CancellationCode&VarType=Char&VarName=DIVERTED&VarDesc=Diverted&VarType=Num&VarDesc=CRSElapsedTime&VarType=Num&VarDesc=ActualElapsedTime&VarType=Num&VarDesc=AirTime&VarType=Num&VarDesc=Flights&VarType=Num&VarName=DISTANCE&VarDesc=Distance&VarType=Num&VarDesc=DistanceGroup&VarType=Num&VarDesc=CarrierDelay&VarType=Num&VarDesc=WeatherDelay&VarType=Num&VarDesc=NASDelay&VarType=Num&VarDesc=SecurityDelay&VarType=Num&VarDesc=LateAircraftDelay&VarType=Num&VarDesc=FirstDepTime&VarType=Char&VarDesc=TotalAddGTime&VarType=Num&VarDesc=LongestAddGTime&VarType=Num&VarDesc=DivAirportLandings&VarType=Num&VarDesc=DivReachedDest&VarType=Num&VarDesc=DivActualElapsedTime&VarType=Num&VarDesc=DivArrDelay&VarType=Num&VarDesc=DivDistance&VarType=Num&VarDesc=Div1Airport&VarType=Char&VarDesc=Div1AirportID&VarType=Num&VarDesc=Div1AirportSeqID&VarType=Num&VarDesc=Div1WheelsOn&VarType=Char&VarDesc=Div1TotalGTime&VarType=Num&VarDesc=Div1LongestGTime&VarType=Num&VarDesc=Div1WheelsOff&VarType=Char&VarDesc=Div1TailNum&VarType=Char&VarDesc=Div2Airport&VarType=Char&VarDesc=Div2AirportID&VarType=Num&VarDesc=Div2AirportSeqID&VarType=Num&VarDesc=Div2WheelsOn&VarType=Char&VarDesc=Div2TotalGTime&VarType=Num&VarDesc=Div2LongestGTime&VarType=Num&VarDesc=Div2WheelsOff&VarType=Char&VarDesc=Div2TailNum&VarType=Char&VarDesc=Div3Airport&VarType=Char&VarDesc=Div3AirportID&VarType=Num&VarDesc=Div3AirportSeqID&VarType=Num&VarDesc=Div3WheelsOn&VarType=Char&VarDesc=Div3TotalGTime&VarType=Num&VarDesc=Div3LongestGTime&VarType=Num&VarDesc=Div3WheelsOff&VarType=Char&VarDesc=Div3TailNum&VarType=Char&VarDesc=Div4Airport&VarType=Char&VarDesc=Div4AirportID&VarType=Num&VarDesc=Div4AirportSeqID&VarType=Num&VarDesc=Div4WheelsOn&VarType=Char&VarDesc=Div4TotalGTime&VarType=Num&VarDesc=Div4LongestGTime&VarType=Num&VarDesc=Div4WheelsOff&VarType=Char&VarDesc=Div4TailNum&VarType=Char&VarDesc=Div5Airport&VarType=Char&VarDesc=Div5AirportID&VarType=Num&VarDesc=Div5AirportSeqID&VarType=Num&VarDesc=Div5WheelsOn&VarType=Char&VarDesc=Div5TotalGTime&VarType=Num&VarDesc=Div5LongestGTime&VarType=Num&VarDesc=Div5WheelsOff&VarType=Char&VarDesc=Div5TailNum&VarType=Char"
    url = 'https://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=236&Has_Group=3&Is_Zipped=0'
    file_name = os.path.join(save_path, f"{year}{month}.zip")

    with open(file_name, "wb") as file:
        response = request.urlopen(url, params.encode("utf-8"))
        file.write(response.read())
        logging.info(f"{file_name} saved")

    return file_name


def unzip_file(file_name, save_path=download_dir):
    """
    Unzips file to directory
    Args:
        file_name: file to unzip
        save_path: path where the file will be unzipped to

    Returns: name of the file unzipped

    """

    zip_ref = zipfile.ZipFile(file_name, 'r')
    working_dir = os.getcwd()
    os.chdir(save_path)
    logging.info(f"extracting {file_name}")
    zip_ref.extractall()
    os.chdir(working_dir)
    expanded_file = os.path.join(save_path, zip_ref.namelist()[0])
    zip_ref.close()

    return expanded_file


def clean_csv(csv_file,
              year,
              month,
              remove_old=True
              ):
    """
    cleans csv file by removing commas and quotes, renames the file according to the year and month of data it contains

    Args:
        csv_file: location of the file to be cleaned
        year: year of the data in the csv file
        month: month of the data in the csv
        remove_old: flag to remove old file, default is True.

    Returns: path of cleaned file

    """
    new_file_name = f"{month}-{year}.csv"
    logging.info(f"cleaning {csv_file}")
    try:
        output_file = os.path.join(os.path.dirname(csv_file), new_file_name)

        # hmmmmm, is there a better way of nesting these?
        with open(csv_file) as input_file:
            with open(output_file, 'w') as output_file:
                # and a nested loop?... messy
                for line in input_file:
                    # the line that does the cleaning
                    output_line = line.rstrip().rstrip(",").translate(str.maketrans('', '', '"'))
                    output_file.write(output_line)
                    output_file.write("\n")

        logging.info(f"cleaning complete; clean file: {new_file_name}")

    finally:
        if remove_old:
            logging.info(f"removing old file {csv_file}")
            os.remove(csv_file)

    new_file_path = os.path.join(os.path.dirname(csv_file), new_file_name)
    return new_file_path


def verify_ingest(out_file, expected=expected_header):
    """
    checks files have expected header and some content
    Args:
        out_file: path to file being checked
        expected: expected header

    Raises:
        UnexpectedFormat: if header is incorrect
        DataUnavailable: if no content besides header is present

    """
    logging.info(f"checking header of {out_file}...")
    with open(out_file, 'r') as file:
        first_line = file.readline().strip()

        if first_line != expected:
            os.remove(out_file)
            message = f"In {file} got header: {first_line} \n expected: {expected}"
            logging.error(message)
            raise UnexpectedFormat(message)
        else:
            logging.info("...verified")

        logging.info("checking if some content is present")

        if next(file, None) is None:
            os.remove(out_file)
            message = f"{file} has header with no content"
            logging.error(message)
            raise DataUnavailable(message)
        else:
            logging.info("has content")


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description="Ingest data from BTS to Google Cloud")
    parser.add_argument("--bucket",
                        help="Cloud bucket to ingest data",
                        required=True
                        )
    parser.add_argument("--start_year",
                        help="first year to upload data, e.g. 2015",
                        required=True,
                        type=int
                        )
    parser.add_argument("--end_year",
                        help="last year to upload data for e.g. 2018",
                        required=True,
                        type=int
                        )

    input_args = parser.parse_args()

    months = [month for month in range(1, 13)]
    years = [year for year in range(input_args.start_year, input_args.end_year+1)]

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

    try:
        logging.info("Starting batch ingestion...")
        locations = ingest_pipeline(years, months, input_args.bucket)
        logging.info("Data ingested in {locations}")

    except DataUnavailable as e:
        logging.error(f"Ingest Failed: {e.message}")

