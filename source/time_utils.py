import datetime


def days_range(start_date, end_date, increment=300):
    """
    Returns a range of days between the specified start and end times sliced by the
    specified increment value in days.

    :param start_date: The start datetime of the days range (ISO 8601).
    :param end_date: The end datetime of the days range (ISO 8601).
    :param increment: The days range increment.
    """
    for n in range(0, int((end_date - start_date).days, increment)):
        yield start_date + datetime.timedelta(n)


def minutes_range(start_date, end_date, increment=300):
    """
    Returns a range of minutes between the specified start and end times sliced by the
    specified increment value in minutes.

    :param start_date: The start datetime of the minutes range (ISO 8601).
    :param end_date: The end datetime of the minutes range (ISO 8601).
    :param increment: The minutes range increment.
    """
    for n in range(0, int((end_date - start_date).days) * 24 * 60, increment):
        yield start_date + datetime.timedelta(minutes=n)
