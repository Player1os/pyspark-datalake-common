# Load external modules.
import argparse
import apiclient.discovery
import httplib2
import oauth2client
import oauth2client.file
import oauth2client.tools

# Define the google api parameters.
scopes = ['https://www.googleapis.com/auth/analytics.readonly']
client_secrets_file_path = '/home/ohassanein/google_analytics_reporting/client_secrets.json'

def initialize():
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(formatter_class = argparse.RawDescriptionHelpFormatter, parents = [oauth2client.tools.argparser])
    flags = parser.parse_args(['--noauth_local_webserver'])

    # Set up a Flow object to be used if we need to authenticate.
    flow = oauth2client.client.flow_from_clientsecrets(
        client_secrets_file_path,
        scope = scopes,
        message = oauth2client.tools.message_if_missing(client_secrets_file_path)
    )

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client flow.
    # The Storage object will ensure that if successful the good credentials will get written back to a file.
    storage = oauth2client.file.Storage('/home/ohassanein/google_analytics_reporting/storage.dat')
    credentials = storage.get()
    if (credentials is None) or (credentials.invalid):
        credentials = oauth2client.tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http = httplib2.Http())

    # Build the service object.
    return apiclient.discovery.build('analyticsreporting', 'v4', http = http)
