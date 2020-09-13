import click
import json
import pandas
import pendulum
import requests
import sys


def extract_measurement(message):

    samples = message['data'].get('sensor', {}).get('samples', [])

    for sample in samples:
        message_time = str(pendulum.parse(message['created_at']))
        sequence = message['data'].get('frame', {}).get('sequence')
        uptime = message['data'].get('state', {}).get('uptime')
        voltage1 = message['data'].get('battery', {}).get('voltage1')
        voltage2 = message['data'].get('battery', {}).get('voltage2')
        latitude = message['data'].get('tracking', {}).get('latitude')
        longitude = message['data'].get('tracking', {}).get('longitude')
        ecl = message['data'].get('network', {}).get('nuestats', {}).get('ecl')
        sample_timestamp = sample.get('timestamp')
        dendrometer_avg = sample.get('dendrometer_avg')
        dendrometer_min = sample.get('dendrometer_min')
        dendrometer_max = sample.get('dendrometer_max')
        int_temperature_avg = sample.get('int_temperature_avg')
        int_temperature_min = sample.get('int_temperature_min')
        int_temperature_max = sample.get('int_temperature_max')
        air_temperature_avg = sample.get('air_temperature_avg')
        air_temperature_min = sample.get('air_temperature_min')
        air_temperature_max = sample.get('air_temperature_max')
        air_humidity_avg = sample.get('air_humidity_avg')
        air_humidity_min = sample.get('air_humidity_min')
        air_humidity_max = sample.get('air_humidity_max')
        soil_temperature_avg = sample.get('soil_temperature_avg')
        soil_temperature_min = sample.get('soil_temperature_min')
        soil_temperature_max = sample.get('soil_temperature_max')
        soil_moisture_avg = sample.get('soil_moisture_avg')
        soil_moisture_min = sample.get('soil_moisture_min')
        soil_moisture_max = sample.get('soil_moisture_max')
        acceleration_x = sample.get('acceleration_x')
        acceleration_y = sample.get('acceleration_y')
        acceleration_z = sample.get('acceleration_z')
        orientation = sample.get('orientation')

        if sample_timestamp is None:
            continue

        sample_time = str(pendulum.from_timestamp(sample_timestamp))

        """
        dt = pendulum.from_timestamp(sample_timestamp)
        dt = dt.in_timezone('Europe/London')
        sample_date = dt.to_datetime_string()
        """

        yield {
            'label': message['label'],
            'message_time': message_time,
            'sequence': sequence,
            'uptime': uptime,
            'voltage1': voltage1,
            'voltage2': voltage2,
            'latitude': latitude,
            'longitude': longitude,
            'ecl': ecl,
            'sample_time': sample_time,
            'dendrometer_avg': dendrometer_avg,
            'dendrometer_min': dendrometer_min,
            'dendrometer_max': dendrometer_max,
            'int_temperature_avg': int_temperature_avg,
            'int_temperature_min': int_temperature_min,
            'int_temperature_max': int_temperature_max,
            'air_temperature_avg': air_temperature_avg,
            'air_temperature_min': air_temperature_min,
            'air_temperature_max': air_temperature_max,
            'air_humidity_avg': air_humidity_avg,
            'air_humidity_min': air_humidity_min,
            'air_humidity_max': air_humidity_max,
            'soil_temperature_avg': soil_temperature_avg,
            'soil_temperature_min': soil_temperature_min,
            'soil_temperature_max': soil_temperature_max,
            'soil_moisture_avg': soil_moisture_avg,
            'soil_moisture_min': soil_moisture_min,
            'soil_moisture_max': soil_moisture_max,
            'acceleration_x': acceleration_x,
            'acceleration_y': acceleration_y,
            'acceleration_z': acceleration_z,
            'orientation': orientation
        }


class FetchException(Exception):
    pass


# This class handles fetching of device list from the provided group
class DeviceFetcher:

    def __init__(self, group_id, api_token):
        self._group_id = group_id
        self._api_token = api_token

    def _get(self, limit=100, offset=0):
        params = {
            'group_id': self._group_id,
            'limit': limit,
            'offset': offset
        }
        headers = {
            'Authorization': 'Bearer {}'.format(self._api_token)
        }
        r = requests.get('https://api.hardwario.cloud/v1/devices', params=params, headers=headers)
        if r.status_code != 200:
            raise FetchException
        return json.loads(r.text)

    def fetch(self):
        found = False; records = []; offset = 0; limit = 100
        while True:
            found = True; click.echo('.', nl=False)
            data = self._get(limit=limit, offset=offset)
            count = len(data)
            records += data; offset += count
            if count == 0 or count < limit:
                break
        if found:
            click.echo()
        return records


# This class handles fetching of message list from the provided device
class MessageFetcher:

    def __init__(self, group_id, device_id, api_token):
        self._group_id = group_id
        self._device_id = device_id
        self._api_token = api_token

    def _get(self, limit=100, offset=0, since=None):
        params = {
            'group_id': self._group_id,
            'device_id': self._device_id,
            'limit': limit,
            'offset': offset
        }
        if since is not None:
            params['since'] = int(since) * 1000
        headers = {
            'Authorization': 'Bearer {}'.format(self._api_token)
        }
        r = requests.get('https://api.hardwario.cloud/v1/messages', params=params, headers=headers)
        if r.status_code != 200:
            print(r.status_code)
            raise FetchException
        return json.loads(r.text)

    def fetch(self, since):
        found = False; records = []; offset = 0; limit = 100
        while True:
            found = True; click.echo('.', nl=False)
            data = self._get(limit=limit, offset=offset, since=since)
            count = len(data)
            records += data; offset += count
            if count == 0 or count < limit:
                break
        if found:
            click.echo()
        return records


@click.command()
@click.option('--xlsx-file', '-x', metavar='XLSX_FILE', required=True, help='Specify output XLSX file.')
@click.option('--since', '-s', metavar='SINCE', required=False, help='Specify timestamp since when to fetch data.')
@click.option('--group-id', '-g', metavar='GROUP_ID', required=True, help='Specify group identifier.')
@click.option('--api-token', '-t', metavar='API_TOKEN', required=True, help='Specify group API token.')
def main(xlsx_file, since, group_id, api_token):

    # Fetch all devices in group
    click.echo('Fetching data for group: {}...'.format(group_id))
    devices = DeviceFetcher(group_id=group_id, api_token=api_token).fetch()

    # Initialize sheets
    sheets = []

    for device in devices:

        # Fetch all messages for device
        click.echo('Fetching messages for device: {} ({})...'.format(device['id'], device.get('name', '-')))
        messages = MessageFetcher(device['group_id'], device['id'], device['api_token']).fetch(since)

        # Initialize measurements
        measurements = []

        # Walk through messages
        for m in messages:

            # Extract desired measurement out of the message
            for measurement in extract_measurement(m):
                # Append the measurement if it was extracted
                if measurement is not None:
                    measurements.append(measurement)

        # Append sheet as Pandas data frame
        df = pandas.DataFrame(measurements)
        sheets.append({'name': device['id'][:30], 'data': df})

    # Write measurements to XLSX
    click.echo('Generating XLSX file...')
    with pandas.ExcelWriter(xlsx_file) as writer:
        for sheet in sheets:
            sheet['data'].to_excel(writer, sheet_name=sheet['name'])


if __name__ == '__main__':
    try:
        main()
    except FetchException:
        click.echo('Request to REST API failed. Please, check your parameters!', err=True)
    except KeyboardInterrupt:
        pass
