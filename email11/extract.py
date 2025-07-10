import base64
from transform import extract_attachments


def fetch_emails(service, query='is:unread'):
    response = service.users().messages().list(userId='me', q=query).execute()
    return response.get('messages', [])

def parse_email(service, msg_id, s3, bucket):
    msg = service.users().messages().get(userId='me', id=msg_id).execute()
    payload = msg['payload']
    headers = {h['name']: h['value'] for h in payload.get('headers', [])}

    email_data = {
        'sender_name': headers.get('From'),
        'receiver_name': headers.get('To'),
        'cc': headers.get('Cc', ''),
        'subject': headers.get('Subject', ''),
        'body': extract_body(payload),
        'attachments': extract_attachments(service, msg_id, payload, s3, bucket)
    }
    return email_data

def extract_body(payload):
    if 'parts' in payload:
        for part in payload['parts']:
            if part['mimeType'] == 'text/plain' and 'data' in part['body']:
                return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
    elif 'body' in payload and 'data' in payload['body']:
        return base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8')
    return ''