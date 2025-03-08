from typing import Dict
import requests
from pandas import DataFrame, read_csv, read_json, to_datetime

def temp() -> DataFrame:
    """Get the temperature data.
    Returns:
        DataFrame: A dataframe with the temperature data.
    """
    return read_csv("data/temperature.csv")

def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Obtiene los días festivos en Brasil para un año dado desde una API pública.
    
    Args:
        public_holidays_url (str): URL base de la API de días festivos.
        year (str): Año a consultar.

    Returns:
        DataFrame: Un DataFrame con los días festivos.
    """
    url = f"{public_holidays_url}/{year}/BR"
    response = requests.get(url)

    try:
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise SystemExit(f"Error al obtener los días festivos: {e}")

    holidays_df = read_json(response.text)
    holidays_df.drop(columns=["types", "counties"], errors="ignore", inplace=True)
    holidays_df["date"] = to_datetime(holidays_df["date"])

    return holidays_df

def extract(
    csv_folder: str, csv_table_mapping: Dict[str, str], public_holidays_url: str
) -> Dict[str, DataFrame]:
    """Extract the data from the csv files and load them into the dataframes.
    Args:
        csv_folder (str): The path to the csv's folder.
        csv_table_mapping (Dict[str, str]): The mapping of the csv file names to the
        table names.
        public_holidays_url (str): The url to the public holidays.
    Returns:
        Dict[str, DataFrame]: A dictionary with keys as the table names and values as
        the dataframes.
    """
    dataframes = {
        table_name: read_csv(f"{csv_folder}/{csv_file}")
        for csv_file, table_name in csv_table_mapping.items()
    }

    holidays = get_public_holidays(public_holidays_url, "2017")

    dataframes["public_holidays"] = holidays

    return dataframes
