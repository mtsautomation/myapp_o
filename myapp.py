from flask import Flask, request, jsonify
import requests
from datetime import datetime
import pandas as pd
import pymysql
import sys

app = Flask(__name__)

# WhatsApp API credentials (replace with your actual token)
ACCESS_TOKEN = "EAANEdLMCZCT0BOw2pHfxwr9eOvzeSWlDq928hDOR8ZBpjM6kbp5a46tHvOFRZBJh6e5nFuf9eQwnpiFDKNkeRGZCGiWPIVdSS8YF4yXZCFKsHLeCeXqripQZAbkeLkZCAcYhU7S0VxBkoI3ZChqSGvEUR7EB5KEeb4GMlH04mHzBY5uA0UAUC9MNKckq9MECgbh6sAZDZD"
PHONE_NUMBER_ID = "556402947548969"

# Webhook verification
@app.route('/webhook', methods=['GET'])
def verify_webhook():

    verify_token = ACCESS_TOKEN
    mode = request.args.get('hub.mode')
    token = request.args.get('hub.verify_token')
    challenge = request.args.get('hub.challenge')
    print("Webhook invoqued")
    if mode == "subscribe" and token == verify_token:
        return challenge, 200
    else:
        return "Verification failed", 403

# Receive messages
@app.route('/webhook', methods=['POST'])
def receive_message():
    logs, contact_df = service_logs()
    try:
        data = request.json  # Parse incoming JSON payload
        messages = data.get('entry', [{}])[0].get('changes', [{}])[0].get('value', {}).get('messages')
        sender = messages[0]['from']  # Sender number
        message_id = messages[0]['id']

        if (contact_df['principalPhoneNumber'].isin([sender]).any()) & (~logs['message_id'].isin([message_id]).any()):
            timestamp = messages[0]['timestamp']
            message_type = messages[0]['type']  # Type of message

            for message in messages:
                subset_contact = contact_df[contact_df['principalPhoneNumber'] == sender]  # Filter contact info
                # Transforming timestamp data into a friendly user readable format
                timestamp_int = int(timestamp)  # Convert the string timestamp to an integer
                datetime_obj = datetime.utcfromtimestamp(timestamp_int)  # Convert the timestamp to a datetime object
                date = datetime_obj.strftime('%Y-%m-%d')  # Format the date object into a readable string
                hour = datetime_obj.strftime('%H:%M:%S')  # Format the hour object into a readable string

                # Handle image messages
                if message_type == 'image':
                    image_data = message.get('image', {})
                    image_id = image_data.get('id')  # Media ID of the image
                    caption = image_data.get('caption', 'No caption')  # Optional caption

                    print(f"Received an image from {sender}. Caption: {caption} at {hour} on {date}")

                    # Fetch the image URL using the media API
                    image_url = get_media_url(image_id)
                    print(f"Direct URL to image: {image_url}")
                    # send_message(sender, caption, image_url, date, hour)
                    return jsonify({"image_url": image_url, "caption": caption, "sender": sender})

                elif message_type == 'text':
                    image_url = ""
                    text = message['text']['body']  # Text message content
                    print(text)
                    message_df = get_message(text, image_url)
                    # Update logs, spreadsheet and service database
                    update_services(message_df, message_id)  # Service database
                    send_message(sender, message_df, date, hour, subset_contact)
                    print(f"Received a message from {sender} at {hour} on {date}")
                    return "Message:sent. ", 200

    except Exception as e:
        print(f"Error processing the request: {e}")
        return "There was an error", 500


def get_message(m_text, m_url):
    try:
        if not m_url:  # Check for empty URL
            # Split the message into lines and find the header position
            lines = m_text.split('\n')
            position = next(
                (i for i, line in enumerate(lines) if 'RETAIL' in line or 'TIENDA' in line),
                None
            )

            if position is None:
                raise ValueError("Header line not found in message")

            # Extract and clean the header
            header = lines[position].split('\t')
            replacement_map = {
                'RETAIL': 'STORE',
                'FECHA': 'FECHA DE SOLICITUD',
                'FECHA DE SOLICITUD ': 'FECHA DE SOLICITUD',
                'FECHADESOLICITUD': 'FECHA DE SOLICITUD',
                'NOMBREDETIENDA': 'NOMBRE DE TIENDA',
                'TIENDA': 'NOMBRE DE TIENDA',
                'MUNICIPIO': 'ZONA/CD',
                'VIN': 'CHASIS',
                'NOMBRE CSA / DEALER': 'CSA/DEALER',
                'CSA DEALER': 'CSA/DEALER',
                'CSA / DEALER': 'CSA/DEALER'
            }
            lst_stores = ['LIVERPOOL', 'SUBURBIA', 'SEARS', 'COPPEL']

            # Clean header and apply replacements
            final_header = [
                replacement_map.get(col.strip().replace(' ', ''), col.strip().replace(' ', ''))
                for col in header
            ]
            if 'RETAIL' not in final_header:
                final_header.insert(1, 'RETAIL')  # Add 'RETAIL' if missing

            # Process rows
            rows = []
            for line in lines[position + 1:]:
                columns = line.split('\t')
                # Align column length to header
                columns += [''] * (len(final_header) - len(columns))

                # Add 'RETAIL' value based on the first column
                col_to_compare = columns[0].strip()
                if 'RETAIL' in final_header:
                    retail_value = col_to_compare if col_to_compare in lst_stores else ' '
                    columns.insert(1, retail_value)

                # Ensure 'MOTOSUR' in column 9
                if len(columns) > 9 and columns[9] != "MOTOSUR":
                    columns[9] = "MOTOSUR"

                rows.append(columns)

            # Create DataFrame and replace empty values
            msgs = pd.DataFrame(rows, columns=final_header).replace("", 'No data')
            return msgs, 200

    except Exception as e:
        print(f"Error processing message: {e}")
        return None, 500


# Database functions
def service_logs():
    try:
        # Establish connection
        connection = pymysql.connect(
            host="database-1.czao0sewwhuc.us-east-2.rds.amazonaws.com",
            user="admin",
            password="Motosur2025",
            database="bajaj_shops"
        )

        # SQL Query Logs
        query = "SELECT * FROM services;"  # Replace with your table name

        # Execute query and fetch data
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()  # Fetch all rows
            columns = [desc[0] for desc in cursor.description]  # Get column names

        # Convert result to DataFrame
        logs_df = pd.DataFrame(result, columns=columns)

        # SQL Query contacts
        query = "SELECT * FROM shops;"  # Replace with your table name

        # Execute query and fetch data
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()  # Fetch all rows
            columns = [desc[0] for desc in cursor.description]  # Get column names

        # Convert result to DataFrame
        contacts_df = pd.DataFrame(result, columns=columns)

        return logs_df, contacts_df

    except pymysql.MySQLError as e:
        print(f"Connection error: {e}")
        return f"Connection error: {e}", 400
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()
            print("Connection closed.")

def update_services(df, message_id):
    print('Updating services database')
    try:
        # Connect to the database
        connection = pymysql.connect(
            host="database-1.czao0sewwhuc.us-east-2.rds.amazonaws.com",
            user="admin",
            password="Motosur2025",
            database="bajaj_shops"
        )

        # Prepare SQL query
        query = """
        INSERT INTO services (systemDate, RETAIL, # TIENDA, FACTURA, FECHA DE SOLICITUD, NOMBRE DE TIENDA, ZONA/CD, 
                              ESTADO, MODELO, CHASIS, CSA/DEALER, SHOP,message_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s. %s);
        """

        # Execute query
        with connection.cursor() as cursor:
            cursor.execute(query, (df['RETAIL'], df['# TIENDA'], df['FACTURA'], df['FECHA DE SOLICITUD'],
                                   df['NOMBRE DE TIENDA'], df['ZONA/CD'], df['ESTADO'], df['MODELO'], df['CHASIS'],
                                   df['CSA/DEALER'], df['SHOP'],message_id))
            connection.commit()
            print(f"Chasis {df['CHASIS']} inserted into database.")

    except pymysql.IntegrityError:
        print(f"Message {message_id} is already processed (duplicate).")
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

# End of database functions

# Image processing
def get_media_url(media_id):
    try:
        # Step 1: Get the media URL from WhatsApp
        media_url_response = requests.get(
            f"https://graph.facebook.com/v21.0/{media_id}",
            headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
        )

        if media_url_response.status_code != 200:
            print(f"Failed to get media URL: {media_url_response.status_code} - {media_url_response.text}")
            return None

        # Extract the URL from the response
        media_url = media_url_response.json().get('url')
        if not media_url:
            print("No URL found in the response.")
            return None

        # Step 2: Request the actual media content
        response = requests.get(media_url, headers={"Authorization": f"Bearer {ACCESS_TOKEN}"})

        if response.status_code == 200:
            print("Media successfully retrieved.")
            return media_url  # Return the URL of the media
        else:
            print(f"Failed to retrieve media: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


# Distribute Messages
def send_message(sender, text, date, hour, contact):

    # Get data from the request
    recipient_number = '+529995565617'  # Recipient's phone number (in E.164 format)

    def sending(mess):
        phone_number_id = "556402947548969"
        print("About to send the message")
        # WhatsApp API endpoint
        url = f"https://graph.facebook.com/v21.0/{phone_number_id}/messages"

        # API request headers
        headers = {
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }

        # API request payload
        payload = {
            "messaging_product": "whatsapp",
            "to": recipient_number,
            "type": "text",
            "text": {"body": mess}
        }

        # Send the message
        response = requests.post(url, json=payload, headers=headers)

        # Check response status
        if response.status_code == 200:
            return "Message sent", 200
        else:
            return response.status_code

    try:
        print("Preparing values to send the message")
        for index, row in text.iterrows():
            try:
                final_message = (
                    f"Hola {contact['contact'].iloc[0]} buenos días/tardes.\n\n"
                    f"Tenemos una activación para la tienda {row['#TIENDA']} de {row['RETAIL']} "
                    f"en {row['ZONA/CD']} de una motocicleta {row['MODELO']} con número de serie {row['CHASIS']} "
                    f"y fecha de solicitud {row['FECHA DE SOLICITUD']}.\n\n"
                    "IMPORTANTE: Tenemos 12 hrs para realizar esta activación.\n\n"
                    "NO OLVIDES:\n"
                    "* Llenar la Hoja de verificación PDI\n"
                    "* El Talón de activación\n"
                    "* La fotografía para poder procesar tu pago."
                )

                response_sending = sending(final_message)
                print("Sending function response:", response_sending)
                return 'Sending message done', 200
            except Exception as e:
                return f'Something with {e} happened creating the message', 500

    except Exception as e:
        return f'Something with {e} happened preparing the message', 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
