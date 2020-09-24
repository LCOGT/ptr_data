import serverless_sdk
sdk = serverless_sdk.SDK(
    org_id='photonadmin',
    application_name='photonranch',
    app_uid='t4x1mMxvVy9pkQwdm2',
    org_uid='dnkslnnhmBqHhhlP2j',
    deployment_uid='5e050efd-e736-445f-b25e-742c00c562b6',
    service_name='ptrdata',
    stage_name='dev',
    plugin_version='3.3.0'
)
handler_wrapper_kwargs = {'function_name': 'ptrdata-dev-insert_data2', 'timeout': 6}
try:
    user_handler = serverless_sdk.get_user_handler('insert_data.main2')
    handler = sdk.handler(user_handler, **handler_wrapper_kwargs)
except Exception as error:
    e = error
    def error_handler(event, context):
        raise e
    handler = sdk.handler(error_handler, **handler_wrapper_kwargs)
