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
    data = request.json  # Parse incoming JSON payload
    if 'hub.challenge' in request.args:
        return request.args.get('hub.challenge'), 200
        # Respond to event-type requests
    if data:
        try:
            # Fallback response
            messages = data.get('entry', [{}])[0].get('changes', [{}])[0].get('value', {}).get('messages', [])
            if not messages:
                print("Status: No messages found")
                return jsonify({"error": "No messages found"}), 200
            else:
                logs, contact_df, = service_logs()
                # service_number = "+52999******" #Replace the number for the number that will work as server
                sender = "+" + messages[0]['from']  # Sender number
                message_id = messages[0]['id']
                print(message_id, ~logs['message_id'].isin([message_id]).any(), contact_df['principalPhoneNumber'].isin([sender]).any())
                # Replace contact_df['principalPhoneNumber'].isin([sender]).any() with service_number == sender
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
                            # Insert code to validate if the chasis already exist
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
            def process_text_lines(m_text):
                lines = m_text.splitlines()  # Split into individual lines
                lines_with_fields = []
                for line in lines:
                    if line.strip():  # Skip empty lines
                        fields = line.split('\t')  # Split each line by tab
                        # Ensure every field is treated as an individual element
                        for field in fields:
                            lines_with_fields.append([field])  # Add as a separate list
                return lines_with_fields

            m_text = m_text.upper()

            headers = ["RETAIL", "# TIENDA", "FACTURA", "FECHA", "NOMBRE DE TIENDA", "ZONA/CD",
                       "ESTADO", "MODELO", "CHASIS", "CSA / DEALER", "SHOP"]

            # Define the replacement map
            replacement_map = {
                'MOTO SUR': "MOTOSUR",
                "MOTO SUR REFACCIONES Y SERVICIO": "MOTOSUR",
                "MOTOSUR REFACCIONES Y SERVICIO": "MOTOSUR",
                'FECHA': 'FECHA DE SOLICITUD',
                'FECHA DE SOLICITUD ': 'FECHA DE SOLICITUD',
                'FECHADESOLICITUD': 'FECHA DE SOLICITUD',
                'NOMBREDETIENDA': 'NOMBRE DE TIENDA',
                '#TIENDA': '# TIENDA',
                '#': '# TIENDA',
                'TIENDA': 'NOMBRE DE TIENDA',
                'MUNICIPIO': 'ZONA/CD',
                'VIN': 'CHASIS',
                'NOMBRE CSA / DEALER': 'CSA/DEALER',
                'CSA DEALER': 'CSA/DEALER',
                'CSA / DEALER': 'CSA/DEALER'
            }

            # List of bike models to treat as single words
            bikes = ['PLATINA 100 ES', 'PLATINA 125', 'PULSAR NS 125', 'BOXER 150', 'DISCOVER 125 ST',
                     'PULSAR 150', 'PULSAR 160 NS', 'PULSAR 160 TD', 'PULSAR 220', 'PULSAR 200NS FI',
                     'PULSAR 200NS', 'PULSAR 200RS', 'AVENGER CRUISE 220', 'AVENGER STREET 220',
                     'DOMINAR 250', 'DOMINAR 400', 'DOMINAR 400 UG', 'N250', 'N160', 'PULSAR NS 125 UG']

            lines = process_text_lines(m_text)
            print("Lines", lines)
            if len(lines) < 10:
                lines = m_text.split('\n')
                print("Entro a split")
                print("Split", lines)
                counting = -1
                position = 0
                for i in range(len(lines)):
                    counting = counting + 1
                    if ('RETAIL' in lines[i]) | ('TIENDA' in lines[i]):
                        position = counting

                # The first line is the header
                header = lines[position].split('\t')

                lst_stores = ['LIVERPOOL', 'SUBURBIA', 'SEARS', 'COPPEL']
                # Step 1: Clean up headers by removing spaces (leading, trailing, and internal spaces)
                print("Will clean the header")
                cleaned_header = [col.strip().replace(' ', '') for col in header]
                print("Cleaned header ", cleaned_header),'\n'
                # Step 2: Replace column names based on the mapping dictionary
                final_header = [replacement_map.get(col, col) for col in cleaned_header]
                print("final_header ", final_header, '\n')
                if 'RETAIL' not in header:
                    final_header.insert(0, 'RETAIL')  # Insert 'RETAIL' at position 1

                # Initialize a list to hold the rows
                rows = []
                # Iterate over the remaining lines to extract the data
                for line in lines[position + 1:]:
                    # Split each line into columns
                    columns = line.split('\t')

                    # Ensure the columns list aligns with the updated header length
                    while len(columns) < len(header):
                        columns.append('NO DATA')  # Append empty values for missing columns
                    # Insert "No data" as the value for 'RETAIL' if it was added

                    col_to_compare = columns[0].replace('', 'No data')

                    if 'RETAIL' in final_header and len(columns) < 11 and col_to_compare in lst_stores:
                        print("Adding  stores")
                        print('RETAIL' in final_header, len(columns) < 11, col_to_compare in lst_stores)
                        index = lst_stores.index(col_to_compare)
                        columns.insert(0, lst_stores[index])

                    elif 'RETAIL' in final_header and len(columns) < 11 and col_to_compare not in lst_stores:
                        columns.insert(0, 'NO data')

                    # Create a dictionary for each row, using the cleaned header as keys

                    if columns[9] != "MOTOSUR":
                        columns[9] = "MOTOSUR"
                    rows.append(columns)

                print(final_header, len(final_header))
                print(rows, len(rows))
                msgs = pd.DataFrame(rows, columns=final_header)
                return msgs
            else:

                def replace_values(data, replacement_map):
                    """
                    Replaces values in a list (flat or nested) based on a replacement map.

                    Args:
                        data (list): A list containing values to be replaced (can be nested).
                        replacement_map (dict): A dictionary with keys as values to be replaced
                                                and values as their replacements.

                    Returns:
                        list: A new list with the replaced values.
                    """
                    if isinstance(data, list):
                        # If it's a list, recursively process each element
                        return [replace_values(item, replacement_map) for item in data]
                    else:
                        # If it's not a list, replace it if it matches a key in the map
                        return replacement_map.get(data, data)

                updated_list = replace_values(lines, replacement_map)
                """print(type(lines))
                updated_lines = []
                for line in lines:
                    print(line)
                    for key, value in replacement_map.items():
                        print("Type", type(line))
                        print(line)
                        line = line.replace(key, value)  # Replace all occurrences of key with value
                    updated_lines.append(line)
                print("Lines inside the else",  updated_lines, '\n')
                # Find the positions of keywords indicating the start of headers"""

                positions = next((i for i, sublist in enumerate(updated_list) if 'SHOP' in sublist), None)
                print('POSITIONS', positions, '\n')

                if not positions:
                    raise ValueError("No header keyword ('SHOP') found in the message.")

                # Extract the values that come after the headers
                print(lines[positions + 2:])
                extracted_values = lines[positions + 2:]
                print('Extracted Values:', extracted_values)

                # Parse the values into chunks corresponding to the headers
                values = extracted_values[:len(headers)]

                # Create and return a DataFrame
                msgs = pd.DataFrame([values], columns=headers)
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

        return print(f"Chasis {df['CHASIS']} inserted into database."), 200

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
import requests  # Ensure this is imported at the top


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

        try:
            # Send the message
            response = requests.post(url, json=payload, headers=headers)

            # Check response status
            if response.status_code == 200:
                print(f"Message sent successfully: {response.json()}")
                return "Message sent", 200
            else:
                print(f"Error sending message: {response.status_code}, {response.text}")
                return f"Error {response.status_code}: {response.text}"
        except requests.RequestException as req_err:
            print(f"Request failed: {req_err}")
            return f"Request failed: {req_err}"

    try:
        print("Preparing values to send the message")
        df = df.fillna('No data')  # Replace NaN with 'No data'
        msg_responses = []

        for index, row in df.iterrows():
            try:
                print(f"Processing CHASIS: {row.get('CHASIS', 'Unknown')}")
                update_services(row, message_id, date, hour)  # Update service database

                print('Processing the message before sending')
                contact_name = contact['contact'].iloc[0] if not contact['contact'].empty else 'Usuario'

                # Construct the message
                final_message = (
                    f"Hola {contact_name}, buenos días/tardes.\n\n"
                    f"Tenemos una activación para la tienda {row['# TIENDA']} de {row['RETAIL']} "
                    f"en {row['ZONA/CD']} de una motocicleta {row['MODELO']} con número de serie {row['CHASIS']} "
                    f"y fecha de solicitud {row['FECHA DE SOLICITUD']}.\n\n"
                    "IMPORTANTE: Tenemos 12 hrs para realizar esta activación.\n\n"
                    "NO OLVIDES:\n"
                    "* Llenar la Hoja de verificación PDI\n"
                    "* El Talón de activación\n"
                    "* La fotografía para poder procesar tu pago."
                )

                # Send the message
                response_sending = sending(final_message)
                msg_responses.append(response_sending)
            except Exception as inner_exception:
                print(f"Error while processing row {index}: {inner_exception}")
                msg_responses.append(f"Failed for row {index}: {inner_exception}")

        # Return the last message response
        print("Sending function response:", msg_responses[-1] if msg_responses else "No responses")
        return "All messages sent", 200
    except Exception as e:
        print(f"Error occurred: {e}")
        return f'Something happened while creating the message: {e}', 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
