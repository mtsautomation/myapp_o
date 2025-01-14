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
        print("First try")
        data = request.json  # Parse incoming JSON payload
        messages = data.get('entry', [{}])[0].get('changes', [{}])[0].get('value', {}).get('messages', [])
        if not messages:
            return jsonify({"error": "No messages found"}), 400

        sender = "+" + messages[0]['from']  # Sender number
        message_id = messages[0]['id']
        print(message_id, ~logs['message_id'].isin([message_id]).any(), contact_df['principalPhoneNumber'].isin([sender]).any())
        # Check sender and message ID validity
        if (contact_df['principalPhoneNumber'].isin([sender]).any()) and \
                (~logs['message_id'].isin([message_id]).any()):
            print('Receive_messages condition True')
            # Process messages
            for message in messages:
                timestamp = int(message['timestamp'])  # Convert timestamp
                datetime_obj = datetime.utcfromtimestamp(timestamp)
                date, hour = datetime_obj.strftime('%Y-%m-%d'), datetime_obj.strftime('%H:%M:%S')
                message_type = message['type']  # Message type
                print(timestamp, datetime_obj, message_type)
                # Handle message types
                if message_type == 'image':
                    image_data = message.get('image', {})
                    image_id = image_data.get('id')
                    caption = image_data.get('caption', 'No caption')

                    # Fetch the image URL
                    image_url = get_media_url(image_id)
                    if image_url:
                        return jsonify({"image_url": image_url, "caption": caption, "sender": sender}), 200
                    else:
                        return jsonify({"error": "Failed to fetch image URL"}), 500

                elif message_type == 'text':
                    text = message['text']['body']  # Text message
                    image_url = ""
                    message_df = get_message(text, image_url)
                    print(type(message_df))
                    print(message_df)
                    if message_df is None:
                        return jsonify({"error": "Failed to process message"}), 500
                    send_message(sender, message_df, date, hour,
                                 contact_df[contact_df['principalPhoneNumber'] == sender], message_id)
                    return jsonify({"message": "Message processed successfully"}), 200

        return jsonify({"message": "Message ignored (already processed or sender unknown)"}), 200

    except Exception as e:
        print(f"Error processing the request: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500


def get_message(m_text, m_url):
    try:
        if m_url == "":
            # Split the message into lines
            lines = m_text.split('\n')
            counting = -1
            position = 0
            for i in range(len(lines)):
                counting = counting + 1
                if ('RETAIL' in lines[i]) | ('TIENDA' in lines[i]):
                    position = counting

            # The first line is the header
            header = lines[position].split('\t')
            replacement_map = {
                'RETAIL': 'STORE',
                'FECHA': 'FECHA DE SOLICITUD',
                'FECHA DE SOLICITUD ': 'FECHA DE SOLICITUD',
                'FECHADESOLICITUD': 'FECHA DE SOLICITUD',
                'NOMBREDETIENDA': 'NOMBRE DE TIENDA',
                '#TIENDA': '# TIENDA',
                'TIENDA': 'NOMBRE DE TIENDA',
                'MUNICIPIO': 'ZONA/CD',
                'VIN': 'CHASIS',
                'NOMBRE CSA / DEALER': 'CSA/DEALER',
                'CSA DEALER': 'CSA/DEALER',
                'CSA / DEALER': 'CSA/DEALER'
                }
            lst_stores = ['LIVERPOOL', 'SUBURBIA', 'SEARS', 'COPPEL']
            # Step 1: Clean up headers by removing spaces (leading, trailing, and internal spaces)

            cleaned_header = [col.strip().replace(' ', '') for col in header]

            # Step 2: Replace column names based on the mapping dictionary
            final_header = [replacement_map.get(col, col) for col in cleaned_header]

            if 'RETAIL' not in header:
                final_header.insert(1, 'RETAIL')  # Insert 'RETAIL' at position 1

            # Initialize a list to hold the rows
            rows = []
            # Iterate over the remaining lines to extract the data

            for line in lines[position + 1:]:
                # Split each line into columns
                columns = line.split('\t')

                # Ensure the columns list aligns with the updated header length
                while len(columns) < len(header):
                    columns.append('')  # Append empty values for missing columns

                # Insert "change" as the value for 'RETAIL' if it was added

                col_to_compare = columns[0].replace(' ', '')

                if 'RETAIL' in final_header and len(columns) > 1 and col_to_compare in lst_stores:
                    index = lst_stores.index(col_to_compare)
                    columns.insert(1, lst_stores[index])

                elif 'RETAIL' in final_header and len(columns) > 1 and col_to_compare not in lst_stores:
                    columns.insert(1, ' ')

                # Create a dictionary for each row, using the cleaned header as keys
                if columns[9] != "MOTOSUR":
                    columns[9] = "MOTOSUR"
                rows.append(columns)

            msgs = pd.DataFrame(rows, columns=final_header)
            # msgs = msgs.replace({"": 'No data'})
            return msgs

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
            print("Connection closed on service_logs.")

def update_services(df, message_id, date, hour):
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
            INSERT INTO services (
                systemDate, RETAIL, `# TIENDA`, FACTURA, `FECHA DE SOLICITUD`, 
                `NOMBRE DE TIENDA`, `ZONA/CD`, ESTADO, MODELO, CHASIS, `CSA/DEALER`, SHOP, message_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Execute query
        with connection.cursor() as cursor:
            cursor.execute(query, ((date + hour), df['RETAIL'], df['# TIENDA'], df['FACTURA'], df['FECHA DE SOLICITUD'],
                                   df['NOMBRE DE TIENDA'], df['ZONA/CD'], df['ESTADO'], df['MODELO'], df['CHASIS'],
                                   df['CSA/DEALER'], df['SHOP'], message_id))
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
def send_message(sender, df, date, hour, contact, message_id):

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
        print(type(df))
        for index, row in df.iterrows():
            try:
                print("Row inside iterrows", row)
                update_services(row, message_id, date, hour)  # Update service database
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
