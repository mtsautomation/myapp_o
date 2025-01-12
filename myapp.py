from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# WhatsApp API credentials (replace with your actual token)
ACCESS_TOKEN = "YOUR_WHATSAPP_ACCESS_TOKEN"


# Webhook verification
@app.route('/webhook', methods=['GET'])
def verify_webhook():
    verify_token = "YOUR_VERIFY_TOKEN"
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

                            return jsonify({"image_url": image_url, "caption": caption, "sender": sender})
                        elif message_type == 'text':
                            text = message.get('text', {}).get('body')  # Text message content
                            print(f"Received an image from {sender}. Message: {text} at {time}")

        return "EVENT_RECEIVED", 200

    except Exception as e:
        print(f"Error processing the request: {e}")
        return jsonify({"error": "Internal server error"}), 500


# Helper function to fetch the media URL
def get_media_url(media_id):
    # Get media URL from WhatsApp
    media_url_response = requests.get(
        f"https://graph.facebook.com/v17.0/{media_id}",
        headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
    )
    if media_url_response.status_code != 200:
        print(f"Failed to get media URL: {media_url_response.text}")
        return None

    # Extract and return the URL
    media_url = media_url_response.json().get('url')
    return media_url


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
