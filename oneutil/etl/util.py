from datetime import datetime
import re


def extract_date_from_string(filename, format="default"):
    """
    Extracts the date from a filename using a regular expression pattern.

    The function takes a filename as input and searches for a pattern in the format of 'YYYYMMDD',
    representing a date. If the pattern is found in the filename, the function extracts the date
    and converts it into a datetime.date object.

    Parameters:
        filename (str): The filename from which the date needs to be extracted.

    Returns:
        datetime.date or None: If the date pattern is found and successfully extracted from the filename,
        the function returns a datetime.date object representing the date. If no date pattern is found
        in the filename, the function returns None.

    Example:
        >>> extract_date_from_filename("data_report_20230801.csv")
        datetime.date(2023, 8, 1)

        >>> extract_date_from_filename("report.txt")
        None
    """

    if format == "default":
        return extract_date_default(filename)
    if format == "hyphen":
        return extract_date_hyphen(filename)
    if format == "underscore":
        return extract_date_underscore(filename)


def extract_date_default(filename):
    # Regular expression pattern to find the date part in the filename
    # default:  yyyyddd format
    date_pattern = r"\d{8}"
    # Search for the date pattern in the filename
    match = re.search(date_pattern, filename)
    if match:
        date_str = match.group()
        # Convert the date string to a datetime object
        date_object = datetime.strptime(date_str, "%Y%m%d").date()
        return date_object
    else:
        return None


def extract_date_hyphen(filename):
    match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
    if match:
        date_str = match.group()
        # Convert the date string to a datetime object
        date_object = datetime.strptime(date_str, "%Y-%m-%d").date()
        return date_object
    else:
        return None


def extract_date_underscore(filename):
    match = re.search(r"\d{4}_\d{2}_\d{2}", filename)
    if match:
        date_str = match.group()
        # Convert the date string to a datetime object
        date_object = datetime.strptime(date_str, "%Y_%m_%d").date()
        return date_object
    else:
        return None
