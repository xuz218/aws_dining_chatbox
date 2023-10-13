import json
import boto3
from datetime import datetime, timedelta, date


def lambda_handler(event, context):
    print(event)

    # Check the Cloudwatch logs to understand data inside event and
    # parse it to handle logic to validate user input and send it to Lex

    # Lex called LF1 with the user message and previous related state so
    # you can verify the user input. Validate and let Lex know what to do next
    resp = {"statusCode": 200, "sessionState": event["sessionState"]}

    sqs = boto3.resource('sqs')
    queue = sqs.Queue('https://sqs.us-east-1.amazonaws.com/924698316170/diningQueue')
    print(queue.url)

    cuisines = ['chinese', 'japanese', 'indian', 'mexican', 'thai', 'korean', 'american']
    slots = event['interpretations'][0]['intent']['slots']
    # nextState = event['proposedNextState']['intent']['slots']

    if "proposedNextState" not in event:
        resp["sessionState"]["dialogAction"] = {"type": "Close"}
    else:
        resp["sessionState"]["dialogAction"] = event["proposedNextState"][
            "dialogAction"
        ]
        if (slots["city"] is None) or ('manhattan' not in slots["city"]["value"]['interpretedValue'].lower()):
            slots["city"] = None
            resp["sessionState"]["dialogAction"] = {
                'type': 'ElicitSlot',
                'intentName': 'DiningSuggestionsIntent',
                'slotToElicit': 'city'
            }
            return resp
        if (slots["cuisine"] is None) or (slots["cuisine"]["value"]['interpretedValue'].lower() not in cuisines):
            slots["cuisine"] = None
            resp["sessionState"]["dialogAction"] = {
                'type': 'ElicitSlot',
                'intentName': 'DiningSuggestionsIntent',
                'slotToElicit': 'cuisine'
            }
            return resp

        if slots["date"] is None:
            resp["sessionState"]["dialogAction"] = {
                'type': 'ElicitSlot',
                'intentName': 'DiningSuggestionsIntent',
                'slotToElicit': 'date'
            }
            return resp

        # check date
        curr_date = date.today()
        input_date = slots["date"]["value"]['interpretedValue']
        dt = datetime.strptime(input_date, "%Y-%m-%d")

        if dt.date() < curr_date:
            slots["date"] = None
            slots["time"] = None
            resp["sessionState"]["dialogAction"] = {
                'type': 'ElicitSlot',
                'intentName': 'DiningSuggestionsIntent',
                'slotToElicit': 'date'
            }
            return resp

        if slots["time"] is None:
            resp["sessionState"]["dialogAction"] = {
                'type': 'ElicitSlot',
                'intentName': 'DiningSuggestionsIntent',
                'slotToElicit': 'time'
            }
            return resp

        # check date and time
        combined_time = input_date + " " + slots["time"]["value"]['interpretedValue']
        dt2 = datetime.strptime(combined_time, "%Y-%m-%d %H:%M")
        now = datetime.now() - timedelta(hours=4)

        if now > dt2:
            slots["time"] = None
            resp["sessionState"]["dialogAction"] = {
                'type': 'ElicitSlot',
                'intentName': 'DiningSuggestionsIntent',
                'slotToElicit': 'time'
            }
            return resp

        if slots["peopleNum"] is None:
            resp["sessionState"]["dialogAction"] = {
                'type': 'ElicitSlot',
                'intentName': 'DiningSuggestionsIntent',
                'slotToElicit': 'peopleNum'
            }
            return resp
        elif slots["peopleNum"] != None:
            try:
                num = int(slots["peopleNum"]["value"]['interpretedValue'])
                if num <= 0 or num > 100:
                    slots["peopleNum"] = None
                    resp["sessionState"]["dialogAction"] = {
                        'type': 'ElicitSlot',
                        'intentName': 'DiningSuggestionsIntent',
                        'slotToElicit': 'peopleNum'
                    }
                    return resp
            except Exception:
                resp["sessionState"]["dialogAction"] = {
                    'type': 'ElicitSlot',
                    'intentName': 'DiningSuggestionsIntent',
                    'slotToElicit': 'peopleNum'
                }
                return resp
        if slots["email"] is None:
            resp["sessionState"]["dialogAction"] = {
                'type': 'ElicitSlot',
                'intentName': 'DiningSuggestionsIntent',
                'slotToElicit': 'email'
            }
            return resp
    ans = {key: value["value"]["interpretedValue"] for key, value in slots.items()}
    response = queue.send_message(MessageBody=json.dumps(ans))
    return resp
