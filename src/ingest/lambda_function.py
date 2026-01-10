import json
from gdrive_to_s3 import main

def lambda_handler(event, context):

    print(f"lambda_handler received event: {event}")
    main(
        full_refresh=event.get("full_refresh"), 
        persist_state=event.get("persist_state")
    )

    return {
        'statusCode': 200,
        'body': json.dumps(f'Lambda execution successful')
    }

