from flask import Flask, jsonify, request, render_template
import os
import requests
import csv
import io
import pandas as pd
from datetime import datetime, timezone

app = Flask(__name__)

KBC_URL = os.environ.get('KBC_URL', 'https://connection.us-east4.gcp.keboola.com')
STORAGE_TOKEN = (
    os.environ.get('KEBOOLA_TOKEN') or
    os.environ.get('KBC_TOKEN') or
    os.environ.get('STORAGE_API_TOKEN', '')
)
BUCKET_ID = 'in.c-device-inventory'
TABLE_ID = 'in.c-device-inventory.devices'


def get_user_email():
    for header in ['X-Forwarded-User', 'X-Forwarded-Email',
                   'X-Kbc-User-Email', 'X-Auth-Request-Email']:
        val = request.headers.get(header, '').strip()
        if val:
            return val
    return os.environ.get('KBC_REALUSER_EMAIL', '').strip()


def _storage_post(path, **kwargs):
    return requests.post(
        f'{KBC_URL.rstrip("/")}{path}',
        headers={'X-StorageApi-Token': STORAGE_TOKEN},
        **kwargs
    )


def _storage_get(path, **kwargs):
    return requests.get(
        f'{KBC_URL.rstrip("/")}{path}',
        headers={'X-StorageApi-Token': STORAGE_TOKEN},
        **kwargs
    )


def ensure_bucket_and_table():
    r = _storage_post(
        '/v2/storage/buckets',
        data={
            'name': 'device-inventory',
            'stage': 'in',
            'description': 'Electronic device inventory submitted by Keboola employees',
        }
    )
    if r.status_code not in (200, 201, 400, 422):
        r.raise_for_status()

    r = _storage_post(
        f'/v2/storage/buckets/{BUCKET_ID}/tables',
        files={'data': ('data.csv',
                        b'submitted_by,device_name,serial_number,submitted_at\n',
                        'text/csv')},
        data={'name': 'devices'}
    )
    if r.status_code not in (200, 201, 400, 422):
        r.raise_for_status()


@app.route('/')
def index():
    user_email = get_user_email()
    return render_template('index.html', user_email=user_email)


@app.route('/api/devices', methods=['GET'])
def get_devices():
    user_email = get_user_email()
    if not user_email or not STORAGE_TOKEN:
        return jsonify([])
    try:
        r = _storage_get(
            f'/v2/storage/tables/{TABLE_ID}/data-preview',
            params={'limit': 1000}
        )
        if r.status_code == 404:
            return jsonify([])
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        if df.empty or 'submitted_by' not in df.columns:
            return jsonify([])
        user_df = (
            df[df['submitted_by'] == user_email][
                ['device_name', 'serial_number', 'submitted_at']
            ]
            .sort_values('submitted_at', ascending=False)
            .reset_index(drop=True)
        )
        return jsonify(user_df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/devices', methods=['POST'])
def post_devices():
    user_email = get_user_email()
    body = request.get_json(silent=True) or {}

    if not user_email:
        user_email = body.get('email', '').strip()
    if not user_email or '@' not in user_email:
        return jsonify({'error': 'Valid email required'}), 400

    devices = body.get('devices', [])
    if not devices:
        return jsonify({'error': 'No devices provided'}), 400
    if not STORAGE_TOKEN:
        return jsonify({'error': 'Storage token not configured'}), 500

    try:
        ensure_bucket_and_table()

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(['submitted_by', 'device_name', 'serial_number', 'submitted_at'])
        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        for d in devices:
            w.writerow([
                user_email,
                d.get('device_name', '').strip(),
                d.get('serial_number', '').strip(),
                now,
            ])

        r = _storage_post(
            f'/v2/storage/tables/{TABLE_ID}/import',
            files={'data': ('data.csv', buf.getvalue().encode('utf-8'), 'text/csv')},
            data={'incremental': '1'}
        )
        r.raise_for_status()
        return jsonify({'success': True, 'count': len(devices)})
    except requests.HTTPError as e:
        return jsonify({'error': f'API error {e.response.status_code}: {e.response.text}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
