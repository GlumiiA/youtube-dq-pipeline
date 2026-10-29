from datetime import timedelta, datetime

# С октября 2024 года YouTube разрешает Shorts длиной до 3 минут.
# Прямого признака Shorts в YouTube Data API нет, поэтому тип определяется по длительности.
SHORTS_MAX_DURATION = timedelta(minutes=3)

def parse_duration(duration_str):
    duration_str = duration_str.replace('P', '').replace('T', '')
    components = ['D', 'H', 'M', 'S']
    values = {'D': 0, 'H': 0, 'M': 0, 'S': 0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    total_duration = timedelta(
        days = values['D'], hours = values['H'], minutes = values['M'], seconds = values['S']
    )

    return total_duration

def transform_data(row):
    duration_td = parse_duration(row['Duration'])
    row['Duration'] = (datetime.min + duration_td).time() # Convert timedelta to time format
    row['Video_Type'] = 'Shorts' if duration_td <= SHORTS_MAX_DURATION else 'Normal'
    return row