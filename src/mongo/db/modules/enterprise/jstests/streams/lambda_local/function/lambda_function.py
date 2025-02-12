def handler(event, context):
    if event.get("action", "") == "hello":
        return "Hello World!"

    return event
