#
#   Chat GPT
#
#
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from timeit import default_timer as timer
import extn_utils as eu

class EmailParams:
    def __init__(self, sender_email, recipient_email, cc_email, bcc_email, reply_to_email, subject, message, attachment_path,origFileName=None):
        self.sender_email = sender_email
        self.recipient_email = recipient_email
        self.reply_to_email = reply_to_email
        self.cc_email = cc_email
        self.bcc_email = bcc_email
        self.subject = subject
        self.message = message
        self.attachment_path = attachment_path
        self.origFileName = origFileName
        
def send_email_with_starttls(email_params_list):
    smtp_server = "smtp.gsa.gov"
    smtp_port = 25

    # Create a secure TLS connection
    smtp_connection = smtplib.SMTP(smtp_server, smtp_port)
    smtp_connection.ehlo()
    smtp_connection.starttls()
    smtp_connection.ehlo()

    for email_params in email_params_list:
        try:
            success = False
            # Create the email message
            message = MIMEMultipart()
            message["From"] = email_params.sender_email
            message["To"] = email_params.recipient_email
            message["Cc"] = email_params.cc_email
            message["Reply-To"] = email_params.reply_to_email
            message["Bcc"] = email_params.bcc_email
            message["Subject"] = email_params.subject
            origFileName = email_params.origFileName
            # Add message body
#            message.attach(MIMEText(email_params.message, "plain"))

            # Add HTML body
            body = MIMEText(email_params.message, "html")
            message.attach(body)

            # Add attachment
            if email_params.attachment_path != "":
             for fileLocation in  email_params.attachment_path:  
                with open(fileLocation, "rb") as attachment:
                    part = MIMEApplication(attachment.read())
                if origFileName is None:
                    filenamePattern = re.search(r'[^\\\/]+$',fileLocation)
                    fileName= filenamePattern.group()
                else:
                    fileName = origFileName
                part.add_header("Content-Disposition", f"attachment;filename={fileName}")
                message.attach(part)

            toEmails_Lst = email_params.recipient_email.split(",")
            ccEmails_Lst = email_params.cc_email.split(",")
            bccEmails_Lst = email_params.bcc_email.split(",")
            replyToEmails_Lst = email_params.reply_to_email.split(",")
            toAddress = toEmails_Lst + ccEmails_Lst+ bccEmails_Lst+replyToEmails_Lst

            # Send the email
            #smtp_connection.sendmail(email_params.sender_email, toAddress,  message.as_string())
            smtp_connection.sendmail(email_params.sender_email, toAddress,  message.as_string())
            success = True
            eu.print_colored("Email sent successfully!", "green")

        except Exception as e:
            eu.print_colored("An error occurred while sending the email:"+ str(e), "red")
    # Close the SMTP connection
    smtp_connection.quit()
    return success

def main():
    # Create a list of email parameter objects
    email_params_list = []
    start = timer()
    # Send emails for the list of email parameter objects
    send_email_with_starttls(email_params_list)
    end = timer()
    print ("Took " + str(end - start) + " seconds")

if __name__ == "__main__":
    main()
