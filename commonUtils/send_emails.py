# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 15:11:05 2022
credit: @https://www.thepythoncode.com/article/use-gmail-api-in-python
@author: SatishVenkataraman
"""

# for getting full paths to attachements
import os
# for encoding messages in base64
from base64 import urlsafe_b64encode
# for dealing with attachement MIME types
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from mimetypes import guess_type as guess_mime_type
from email import encoders
from common import our_email, gmail_authenticate

# Adds the attachment with the given filename to the given message
def add_attachment(message, filename):
    content_type, encoding = guess_mime_type(filename)
    if content_type is None or encoding is not None:
        content_type = 'application/octet-stream'
    main_type, sub_type = content_type.split('/', 1)
    if main_type == 'text':
        fp = open(filename, 'rb')
        msg = MIMEText(fp.read().decode(), _subtype=sub_type)
        fp.close()
    elif main_type == 'image':
        fp = open(filename, 'rb')
        msg = MIMEImage(fp.read(), _subtype=sub_type)
        fp.close()
    elif main_type == 'audio':
        fp = open(filename, 'rb')
        msg = MIMEAudio(fp.read(), _subtype=sub_type)
        fp.close()
    else:
        fp = open(filename, 'rb')# Open the files in binary mode.  
        msg = MIMEBase(main_type, sub_type)
        msg.set_payload(fp.read())
        encoders.encode_base64(msg) #added by Shristi. When the data is unprintable, it encodes the payload using Base64
        fp.close()
        
    filename = os.path.basename(filename)
    msg.add_header('Content-Disposition', 'attachment', filename=filename)
    message.attach(msg)

def build_message(destination,Cc, obj,  body, attachments=[]):
    if not attachments: # no attachments given
        message = MIMEText(body, 'html')
        message['to'] = destination
        message['from'] = our_email
        message['Cc'] = Cc
        message['subject'] = obj
        
    else:
        message = MIMEMultipart() # Create the container (outer) email message.
        message['to'] = destination
        message['from'] = our_email
        message['Cc'] = Cc
        message['subject'] = obj       
        message.attach(MIMEText(body,'html'))
        for filename in attachments:
         add_attachment(message, filename)
    return {'raw': urlsafe_b64encode(message.as_bytes()).decode()} # An object containing a base64url encoded email object.By encoding our data, we improve the chances of it being processed correctly by various systems.

def send_message(service, destination,Cc, obj, body, attachments=[]):
    return service.users().messages().send(
      userId="me",
      body=build_message(destination,Cc, obj,  body,  attachments)
    ).execute()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Email Sender using Gmail API")
    parser.add_argument('destination', type=str, help='The destination email address')
    parser.add_argument('Cc', type=str, help='Cc emailer receiver')
    parser.add_argument('subject', type=str, help='The subject of the email')
    parser.add_argument('body', type=str, help='The body of the email')
    parser.add_argument('-f', '--files', type=str, help='email attachments', nargs='+')   
    #print(args.destination)
    
    service = gmail_authenticate()
    #send_message(service, args.destination, args.subject, args.body, args.Cc, args.files)
    send_message(service,'shristi.amatya@gsa.gov','shristi.amatya@peridotsolutions.com','testing testing 1..2..3..','email sent from commonutil library')
    