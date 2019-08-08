def my_handler(event, context):
    message = 'Hello world from a lambda function!'  
    return { 
        'message' : message
    }  