import base64
import os
from connect import connect_to_s3 

def extract_attachments(service, msg_id, payload,s3,bucket):
    urls = []

    if 'parts' in payload:
        for part in payload['parts']:
            if part.get('filename'):
                att_id = part['body'].get('attachmentId')
                att = service.users().messages().attachments().get(userId='me', messageId=msg_id, id=att_id).execute()
                data = base64.urlsafe_b64decode(att['data'])
                filename = part['filename']
                
                with open(filename, 'wb') as f:
                    f.write(data)

                s3.upload_file(filename, bucket, filename)
                urls.append(f"s3://{bucket}/{filename}")
                os.remove(filename)  # Clean up the local file

    return urls