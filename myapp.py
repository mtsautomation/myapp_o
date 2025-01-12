from flask import Flask, request, jsonify

app = Flask(__name__)

# Webhook verification
@app.route('/webhook', methods=['GET'])
def verify_webhook():
    verify_token = "EAANEdLMCZCT0BOyxT96l0JlviAD95YDTFaisAQDEH1lW5k1ZCXAwXv1bZCZBOaEuK8IGWijd7TjEDgaAHZCdNjD5HhoA9836LncbVEenm1FDZCuF6lk4Ud3IfGHQuxZAIoKDhCeInSqM7kGadKkQ2lTgdhHspp0mwNCuwX4VX91qavuOdFYwosHbCejZAoYaokYavsdPTZBZBScZBZA9NnPubKtSgWBOhOHe6naEl1N7XJgWxAZDZD"  # Replace with your token
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
        # Use Flask's built-in JSON parsing
        data = request.json  # Directly access the JSON payload as a dictionary
        print(type(data))
        print(data)
        # Process the JSON data
        if data and 'entry' in data:
            for entry in data['entry']:
                for change in entry.get('changes', []):
                    value = change.get('value', {})
                    messages = value.get('messages', [])

                    for message in messages:
                        timestamp = message.get('timestamp')  # Timestamp of the message
                        sender = message.get('from')  # Sender's phone number
                        text = message.get('text', {}).get('body')  # Text message content
                        print(f"Message from {sender}: {text} at {timestamp}")
        else:
            print("Invalid JSON structure: 'entry' key is missing.")
            return jsonify({"error": "Invalid JSON structure"}), 400

    except Exception as e:
        # Handle unexpected errors gracefully
        print(f"Error processing the request: {e}")
        return jsonify({"error": "Internal server error"}), 500

    return "EVENT_RECEIVED", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
