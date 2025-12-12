# handler.py
import json
import base64
import boto3
from datetime import datetime
import os

s3 = boto3.client('s3')
S3_BUCKET = os.environ.get('OUTPUT_BUCKET')  # e.g. rt-cart-processed-<suffix>
S3_PREFIX = os.environ.get('OUTPUT_PREFIX', 'processed/')

def compute_cart_total(items):
    # items: list of {productId, price, qty}
    return sum(float(i.get('price',0)) * int(i.get('qty',1)) for i in items)

def lambda_handler(event, context):
    # Kinesis event -> records
    out_lines = []
    for rec in event.get('Records', []):
        payload = base64.b64decode(rec['kinesis']['data']).decode('utf-8')
        try:
            ev = json.loads(payload)
        except Exception:
            # ignore malformed
            continue
        ev['processed_at'] = datetime.utcnow().isoformat() + 'Z'
        items = ev.get('items', [])
        ev['cart_total'] = compute_cart_total(items)
        out_lines.append(json.dumps(ev))
    if not out_lines:
        return {'status': 'no_records'}
    # upload to s3 as a small file
    key = f"{S3_PREFIX}batch-{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body="\n".join(out_lines).encode('utf-8'))
    return {'status': 'ok', 'wrote': key}
