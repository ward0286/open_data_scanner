"""Helper functions for other modules."""

import calendar
import datetime as dt
import os
import re
from typing import List


def check_and_create_path(path: str) -> None:
    """Checks if the given path exist. If not, creates required 
    directories. The last subdirectory must end with a slash.
    """
    if not path.startswith('./'):
        if path.startswith('/'):
            path = '.' + path
        else:
            path = './' + path
    path_pieces: List[str] = path.split('/')
    subpath: str
    for i in range(2, len(path_pieces)):
        subpath = '/'.join(path_pieces[:i])
        if not os.path.isdir(subpath):
            os.mkdir(subpath)


def date_ago(n: float, unit: str, 
             from_: dt.datetime = dt.datetime.now()) -> dt.datetime:
    """Returns the date n units ago (unit can be day/week/month/year), 
    starting from the given from_ date if provided, from the current date 
    otherwise.
    """
    if n < 0:
        raise ValueError(f"Illegal argument (n = {n}). n must be >= 0")
    date: dt.datetime
    match unit:
        case 'day':
            duration = dt.timedelta(days=n)
            date = from_ - duration
        case 'week':
            duration = dt.timedelta(weeks=n)
            date = from_ - duration
        # no dt.timedelta native construct for months and years
        case 'month':
            # if n is integer
            if n % 1 == 0:
                date = from_
                # search year
                n_from_jan: int = int(n) - date.month + 1
                if n_from_jan > 0:
                    n_years: int = (n_from_jan - 1) // 12 + 1
                    date = date.replace(year=date.year-n_years)
                # search month
                month: int = int(from_.month - n) % 12
                if month == 0: 
                    month = 12  # 12 modulo 12 = 0 -> December
                # check if day is not out of range for that month
                day_max: int
                _, day_max = calendar.monthrange(date.year, month)
                if from_.day > day_max: 
                    date.replace(day=day_max)
                date = date.replace(month=month)
            # if n is decimal
            else: 
                decimals: float = n % 1
                decimals_in_days: int = round(30.43 * decimals)
                days_delta = dt.timedelta(days=decimals_in_days)
                base_date: dt.datetime = date_ago(int(n), 'month', from_)
                date = base_date - days_delta
        case 'year':
            n_in_months: int = round(n * 12) # e.g. 0.33 year -> 4 months
            date = date_ago(n_in_months, 'month', from_)
        case _:
            raise ValueError(f'Illegal argument (unit = {unit}). Allowed' +\
                             ' values are day, week, month and year.')
    return date


def infer_name_from_email(email: str) -> str:
    """(Utility method) Infer name of the email owner from the given email
    address (splits and capitalizes words before @).
    """

    def upper_after_mac(m: re.Match) -> str:
        return m.group(1) + m.group(2).upper()

    if email and email != '':
        name: str = ' '.join(re.split(r'[.\-_]', 
                             email.split('@')[0].lower())).title()
        name = re.sub(r'(Ma?c)([a-z])', upper_after_mac, name)
        name = re.sub(r'^MacKenzie', 'Mackenzie', name)
        return name
    else:
        return ''
