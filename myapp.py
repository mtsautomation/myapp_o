from flask import Flask, request, jsonify
import requests
from datetime import datetime
import pandas as pd
import pymysql

app = Flask(__name__)

# WhatsApp API credentials (replace with your actual token)
ACCESS_TOKEN = "EAANEdLMCZCT0BOw2pHfxwr9eOvzeSWlDq928hDOR8ZBpjM6kbp5a46tHvOFRZBJh6e5nFuf9eQwnpiFDKNkeRGZCGiWPIVdSS8YF4yXZCFKsHLeCeXqripQZAbkeLkZCAcYhU7S0VxBkoI3ZChqSGvEUR7EB5KEeb4GMlH04mHzBY5uA0UAUC9MNKckq9MECgbh6sAZDZD"
PHONE_NUMBER_ID = "556402947548969"

# Webhook verification
@app.route('/webhook', methods=['GET'])
def verify_webhook():
    verify_token = "EAANEdLMCZCT0BOw2pHfxwr9eOvzeSWlDq928hDOR8ZBpjM6kbp5a46tHvOFRZBJh6e5nFuf9eQwnpiFDKNkeRGZCGiWPIVdSS8YF4yXZCFKsHLeCeXqripQZAbkeLkZCAcYhU7S0VxBkoI3ZChqSGvEUR7EB5KEeb4GMlH04mHzBY5uA0UAUC9MNKckq9MECgbh6sAZDZD"
    mode = request.args.get('hub.mode')
    token = request.args.get('hub.verify_token')
    challenge = request.args.get('hub.challenge')

    if mode == "subscribe" and token == verify_token:
        return challenge, 200
    else:
        return "Verification failed", 403

# Receive messages
@app.route('/webhook', methods=['POST'])
def receive_message():
    try:
        data = request.json  # Parse incoming JSON payload
        try:
            # Navigate through the nested structure
            messages = data['entry'][0]['changes'][0]['value']['messages']

            # Print each message
            for message in messages:
                contact_df = contacts()
                print('Contacts actualizado')
                sender = message['from']
                print(type(sender), sender)
                print(contact_df)
                if sender in contact_df['principalPhoneNumber']:
                    subset_contact = contact_df[contact_df['principalPhoneNumber'] == sender]

                    timestamp = message['timestamp']
                    # Convert the string timestamp to an integer

                    timestamp_int = int(timestamp)
                    # Convert the timestamp to a datetime object
                    datetime_obj = datetime.utcfromtimestamp(timestamp_int)
                    # Format the datetime object into a readable string
                    date = datetime_obj.strftime('%Y-%m-%d')
                    hour = datetime_obj.strftime('%H:%M:%S')

                    message_type = message.get('type')  # Type of message

                    # Handle image messages
                    if message_type == 'image':
                        image_data = message.get('image', {})
                        image_id = image_data.get('id')  # Media ID of the image
                        caption = image_data.get('caption', 'No caption')  # Optional caption

                        print(f"Received an image from {sender}. Caption: {caption} at {hour} on {date}")

                        # Fetch the image URL using the media API
                        image_url = get_media_url(image_id)
                        print(f"Direct URL to image: {image_url}")
                        #send_message(sender, caption, image_url, date, hour)
                        return jsonify({"image_url": image_url, "caption": caption, "sender": sender})

                    elif message_type == 'text':
                        image_url = ""
                        text = message['text']['body']  # Text message content
                        send_message(sender, text, image_url, date, hour, subset_contact)
                        print(f"Received a message from {sender}. Message: {text} at {hour} on {date}")
                else:
                    return "Event_not_processed", 200

        except KeyError as e:
            print(f"KeyError: {e}. Check the structure of your JSON data.")

        return "Event_received", 200

    except Exception as e:
        print(f"Error processing the request: {e}")
        return "There was an error", 500


def contacts():
    try:
        # Establish connection
        connection = pymysql.connect(
            host="database-1.czao0sewwhuc.us-east-2.rds.amazonaws.com",
            user="admin",
            password="Motosur2025",
            database="bajaj_shops"
        )


        # SQL Query
        query = "SELECT * FROM shops;"  # Replace with your table name

        # Execute query and fetch data
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()  # Fetch all rows
            columns = [desc[0] for desc in cursor.description]  # Get column names

        # Convert result to DataFrame
        contacts_df = pd.DataFrame(result, columns=columns)
        print("Successfully connected!")
        return contacts_df

    except pymysql.MySQLError as e:
        print(f"Connection error: {e}")
        return f"Connection error: {e}", 400
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()
            print("Connection closed.")


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


def send_message(sender, text, image_url, date, hour, contact):
    print("Creating message")
    # Get data from the request
    recipient_number = '+529995565617'  # Recipient's phone number (in E.164 format)

    def sending(mess):

        print("About to send the message")
        # WhatsApp API endpoint
        url = f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages"

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

        if image_url == "":
            assignation = text
            print("assign", assignation)
            # Split the message into lines
            lines = assignation.split('\n')
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
                'FECHA': 'FECHA DE SOLICITUD ',
                'FECHADESOLICITUD': 'FECHA DE SOLICITUD ',
                'TIENDA': 'NOMBRE DE TIENDA',
                'MUNICIPIO': 'ZONA/CD',
                'VIN': 'CHASIS',
                'NOMBRE CSA / DEALER': 'CSA/DEALER',
                'CSA DEALER': 'CSA/DEALER',
                'CSA / DEALER':'CSA/DEALER'
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
            print('Los mensajes a enviar son:')
            print(msgs)
            for message in msgs[:-1]:

                final_message = f"Hola {contact['name']} buenos dias/tardes, tenemos una cativacíon para la tienda " \
                                f"{message['# Tienda']} de {message['RETAIL']} en {message['ZONA/CD']} de una " \
                                f"motocicleta {message['MODELO']} con numero de serie {message['CHASIS']} y fecha de " \
                                f"solicitud {message[' FECHA DE SOLICITUD']} \n IMPORTANTE: Tenemos 12 hrs para " \
                                f"realizar esta activacion. NO OLVIDES--> * LLenar el PDI y * Talon de activacion, " \
                                f"asi como la fotografia para poder procesar tu pago."
                sending(final_message)
        else:
            message_text = f"{sender}, te ha enviado {text}, desde el numero de prueba a las at {hour} del {date} " \
                           f"y este URL{image_url}"  # Message text

        if not recipient_number or not message_text:
            return "Not recipient number", 400

    except Exception as e:
        return f"Any message sent an error occurred {e}", 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
