from flask import Flask, jsonify, request, render_template
import os
import requests
import csv
import io
import uuid
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
TABLE_ID = 'in.c-device-inventory.inventory_devices_ids'
EMPLOYEES_TABLE_ID = 'in.c-keboola-ex-google-drive-01kmq8vxhe01pzb3rdz37raz6m.seznam-zamestnancu-3_2026-SEZNAM-ZAMESTNANCU'
FLOW_ID = '01kmqd9xqyzxs9yy198erre1de'
QUEUE_URL = os.environ.get('KBC_QUEUE_URL', 'https://queue.us-east4.gcp.keboola.com')

ADMIN_EMAILS = [
    'petra.griffin@keboola.com',
    'pavel.synek@keboola.com',
]


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


def _trigger_flow():
    """Trigger the device inventory flow via Keboola Queue API."""
    r = requests.post(
        f'{QUEUE_URL.rstrip("/")}/jobs',
        headers={
            'X-StorageApi-Token': STORAGE_TOKEN,
            'Content-Type': 'application/json',
        },
        json={
            'component': 'keboola.flow',
            'config': FLOW_ID,
        }
    )
    r.raise_for_status()
    return r.json()



def _load_employees():
    """Load employee list from Keboola Storage. Returns list of {name, email} dicts."""
    r = _storage_get(
        f'/v2/storage/tables/{EMPLOYEES_TABLE_ID}/data-preview',
        params={'limit': 1000}
    )
    if r.status_code == 404:
        return []
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    if df.empty or 'Work_Email' not in df.columns:
        return []
    return [
        {'name': row['Last_name_First_name'], 'email': row['Work_Email']}
        for _, row in df.iterrows()
        if pd.notna(row['Work_Email'])
    ]


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
                        b'submitted_by,device_name,serial_number,submitted_at,device_id\n',
                        'text/csv')},
        data={'name': 'devices'}
    )
    if r.status_code not in (200, 201, 400, 422):
        r.raise_for_status()


@app.route('/')
def index():
    user_email = get_user_email()
    is_admin = user_email.lower() in [e.lower() for e in ADMIN_EMAILS]
    return render_template('index.html', user_email=user_email, is_admin=is_admin)


@app.route('/api/devices', methods=['GET'])
def get_devices():
    user_email = get_user_email()
    if not user_email or not STORAGE_TOKEN:
        return jsonify([])
    try:
        r = _storage_get(
            f'/v2/storage/tables/{TABLE_ID}/data-preview',
            params={'limit': 150}
        )
        if r.status_code == 404:
            return jsonify([])
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        if df.empty or 'submitted_by' not in df.columns:
            return jsonify([])
        cols = ['device_name', 'serial_number', 'submitted_at']
        if 'device_id' in df.columns:
            # Replace NaN/empty with None so JSON serializes as null (not NaN)
            df['device_id'] = df['device_id'].where(pd.notna(df['device_id']), None)
            df['device_id'] = df['device_id'].replace('', None)
            cols.append('device_id')
        user_df = (
            df[df['submitted_by'] == user_email][cols]
            .sort_values('submitted_at', ascending=False)
            .reset_index(drop=True)
        )
        return jsonify(user_df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/devices', methods=['GET'])
def get_all_devices():
    user_email = get_user_email()
    if user_email.lower() not in [e.lower() for e in ADMIN_EMAILS]:
        return jsonify({'error': 'Forbidden'}), 403
    if not STORAGE_TOKEN:
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
        df = df.sort_values('submitted_at', ascending=False).reset_index(drop=True)
        return jsonify(df[['submitted_by', 'device_name', 'serial_number', 'submitted_at']].to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/status', methods=['GET'])
def get_admin_status():
    user_email = get_user_email()
    if user_email.lower() not in [e.lower() for e in ADMIN_EMAILS]:
        return jsonify({'error': 'Forbidden'}), 403
    if not STORAGE_TOKEN:
        return jsonify({'error': 'Storage token not configured'}), 500
    try:
        employees = _load_employees()

        r = _storage_get(
            f'/v2/storage/tables/{TABLE_ID}/data-preview',
            params={'limit': 1000}
        )
        if r.status_code == 404:
            devices_df = pd.DataFrame(columns=['submitted_by', 'device_name', 'serial_number', 'submitted_at'])
        else:
            r.raise_for_status()
            devices_df = pd.read_csv(io.StringIO(r.text))
            if devices_df.empty or 'submitted_by' not in devices_df.columns:
                devices_df = pd.DataFrame(columns=['submitted_by', 'device_name', 'serial_number', 'submitted_at'])

        submitted_emails = {e.lower() for e in devices_df['submitted_by'].dropna()}

        completed = []
        pending = []
        for emp in employees:
            emp_email = emp['email'].lower()
            if emp_email in submitted_emails:
                emp_devices = (
                    devices_df[devices_df['submitted_by'].str.lower() == emp_email]
                    [['device_name', 'serial_number', 'submitted_at']]
                    .sort_values('submitted_at', ascending=False)
                    .to_dict(orient='records')
                )
                completed.append({'name': emp['name'], 'email': emp['email'], 'devices': emp_devices})
            else:
                pending.append({'name': emp['name'], 'email': emp['email']})

        return jsonify({
            'total': len(employees),
            'completed_count': len(completed),
            'completed': completed,
            'pending': pending,
        })
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
        w.writerow(['submitted_by', 'device_name', 'serial_number', 'submitted_at', 'device_id'])
        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        for d in devices:
            w.writerow([
                user_email,
                d.get('device_name', '').strip(),
                d.get('serial_number', '').strip(),
                now,
                str(uuid.uuid4()),
            ])

        r = _storage_post(
            f'/v2/storage/tables/{TABLE_ID}/import',
            files={'data': ('data.csv', buf.getvalue().encode('utf-8'), 'text/csv')},
            data={'incremental': '1'}
        )
        r.raise_for_status()

        _trigger_flow()

        return jsonify({'success': True, 'count': len(devices)})
    except requests.HTTPError as e:
        return jsonify({'error': f'API error {e.response.status_code}: {e.response.text}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/devices/<device_id>', methods=['PUT'])
def update_device(device_id):
    user_email = get_user_email()
    body = request.get_json(silent=True) or {}

    if not user_email:
        user_email = body.get('email', '').strip()
    if not user_email or '@' not in user_email:
        return jsonify({'error': 'Valid email required'}), 400
    if not STORAGE_TOKEN:
        return jsonify({'error': 'Storage token not configured'}), 500

    new_name = body.get('device_name', '').strip()
    new_serial = body.get('serial_number', '').strip()
    if not new_name or not new_serial:
        return jsonify({'error': 'Device name and serial number are required'}), 400

    try:
        r = _storage_get(
            f'/v2/storage/tables/{TABLE_ID}/data-preview',
            params={'limit': 1000}
        )
        if r.status_code == 404:
            return jsonify({'error': 'Device not found'}), 404
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))

        if df.empty or 'device_id' not in df.columns:
            return jsonify({'error': 'Device not found'}), 404

        mask = (df['device_id'].astype(str) == device_id) & \
               (df['submitted_by'].str.lower() == user_email.lower())
        if not mask.any():
            return jsonify({'error': 'Device not found'}), 404

        df.loc[mask, 'device_name'] = new_name
        df.loc[mask, 'serial_number'] = new_serial

        buf = io.StringIO()
        df.to_csv(buf, index=False)

        r = _storage_post(
            f'/v2/storage/tables/{TABLE_ID}/import',
            files={'data': ('data.csv', buf.getvalue().encode('utf-8'), 'text/csv')},
            data={'incremental': '0'}
        )
        r.raise_for_status()

        _trigger_flow()

        return jsonify({'success': True})
    except requests.HTTPError as e:
        return jsonify({'error': f'API error {e.response.status_code}: {e.response.text}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
