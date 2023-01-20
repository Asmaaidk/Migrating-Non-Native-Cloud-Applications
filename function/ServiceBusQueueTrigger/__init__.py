import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_URL"],
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PW"]
    )
    logging.info("Successfully connected to database")

    try:
        # TODO: Get notification message and subject from database using the notification_id

        cursor = conn.cursor()
        notification = f"SELECT message, subject FROM notification WHERE id={str(notification_id)}"
        cursor.execute(notification)
        message, subject = cursor.fetchone()
        logging.info(f"Notification ID {str(notification_id)} \n\t Subject: {subject} \n\t Message: {message}")

        # TODO: Get attendees email and name

        attendees = f"SELECT first_name, last_name, email FROM attendee;"
        cursor.execute(attendees)
        attendees = cursor.fetchall()


        
        # TODO: Loop through each attendee and send an email with a personalized subject

        for attendee in attendees:
            first_name=attendee[0]
            last_name=attendee[1]
            email=attendee[2]
            email_subject=f"Hello {first_name}! | {subject}"

            mail = Mail(
                from_email=os.environ['ADMIN_EMAIL_ADDRESS'],
                to_emails=email,
                subject=email_subject,
                plain_text_content=message
            )
            

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified

        date = datetime.now()
        status = f"Notified {str(len(attendees))} attendees"
        
        update=f"UPDATE notification SET status='{status}', completed_date='{date}' WHERE id={notification_id};"
        cursor.execute(update)
        conn.commit()




    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        conn.rollback()
    finally:
        # TODO: Close connection
        cursor.close()
        conn.close()