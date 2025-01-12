from flask import Flask, request, jsonify
import requests

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
        if data and 'entry' in data:
            for entry in data['entry']:
                for change in entry.get('changes', []):
                    value = change.get('value', {})
                    messages = value.get('messages', [])

                    for message in messages:
                        time = message.get('text', {}).get('timestamp')
                        sender = message.get('from')  # Sender's phone number
                        message_type = message.get('type')  # Type of message

                        # Handle image messages
                        if message_type == 'image':
                            image_data = message.get('image', {})
                            image_id = image_data.get('id')  # Media ID of the image
                            caption = image_data.get('caption', 'No caption')  # Optional caption

                            print(f"Received an image from {sender}. Caption: {caption} at {time}")

                            # Fetch the image URL using the media API
                            image_url = get_media_url(image_id)
                            print(f"Direct URL to image: {image_url}")
                            send_message(sender, caption, image_url, time)
                            return jsonify({"image_url": image_url, "caption": caption, "sender": sender})

                        elif message_type == 'text':
                            image_url = ""
                            text = message.get('text', {}).get('body')  # Text message content
                            send_message(sender, text, image_url , time)
                            print(f"Received a message from {sender}. Message: {text} at {time}")

        return 200

    except Exception as e:
        print(f"Error processing the request: {e}")
        return 500


# Helper function to fetch the media URL
def get_media_url(media_id):
    # Get media URL from WhatsApp
    media_url_response = requests.get(
        f"https://graph.facebook.com/v21.0/{media_id}",
        headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
    )
    if media_url_response.status_code != 200:
        print(f"Failed to get media URL: {media_url_response.text}")
        return None

    # Extract and return the URL
    media_url = media_url_response.json().get('url')

    return media_url

def send_message(sender, text, image_url, time):
    try:
        # Get data from the request
        recipient_number = '+529995565617'  # Recipient's phone number (in E.164 format)

        if image_url == "":
            message_text = f"{sender}, te ha enviado {text}, desde el numero de prueba a las {time}"  # Message text
        else:
            message_text = f"{sender}, te ha enviado {text}, desde el numero de prueba a las {time} y este URL{image_url}"  # Message text

        if not recipient_number or not message_text:
            return 400

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
            "text": {"body": message_text}
        }

        # Send the message
        response = requests.post(url, json=payload, headers=headers)

        # Check response status
        if response.status_code == 200:
            return 200
        else:
            return response.status_code

    except Exception as e:
        return 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
