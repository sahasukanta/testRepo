import gspread
from oauth2client.service_account import ServiceAccountCredentials
from Google import Create_Service
from github import Github
# from typing import List

class Handler:

    def __init__(self, sheetsDriveCredsJson):
        """
        Authenticates client API keys for Google Drive and Google Sheets APIs
        and authenticates oauth2 key for Google Drive
        """
        # .json files with credentials
        self.sheetsDriveCredsJson = sheetsDriveCredsJson
#         self.driveCredsJson = driveCredsJson
#         self.gitToken = gitToken
        # Google Drive and Google Sheets API key authentication
        self.sheetsDriveClient = self.authenticateDriveSheetsAPIKeys(self.sheetsDriveCredsJson)
        # Google Drive oauth2 authentication
#         self.driveService = self.authenticateOauth2GDrive(self.driveCredsJson)
        # github object
#         self.gitService = Github(self.gitToken)

    def getSheetsDriveClient(self):
        return self.sheetsDriveClient

    def getDriveService(self):
        return self.driveService

    def authenticateDriveSheetsAPIKeys(self, jsonFilename:str) -> "gspread.Client":
        """
        Authorises API keys for Google Drive and Google Sheets APIs
        from jsonFilename with Google Cloud Platform.
        returns: authorised client as gspread.Client object
        """
        assert type(jsonFilename) == str, "jsonFilename must be a str"
        assert jsonFilename.endswith(".json"), f"{jsonFilename} is not a .json file"

        SCOPES = ["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(jsonFilename, SCOPES)
        client = gspread.authorize(creds)

        return client

    def getSheetsData(self, client:gspread.Client, file:str, by:str="id", sheetNum:int=0) -> "list[dict]":
        """
        Gets data from sheetNum of file using an authenticated Google Cloud Platform client object
        returns: list of each row in the spreadsheet as a dict with each col as key
        - example: [{"Serial No": 0, "Name": "Abby"}, {"Serial No": 1, "Name": "Jason"}]
        """
        assert type(file) == str, "file must be a str"
        assert type(sheetNum) == int and sheetNum >= 0, "sheetNum must be an int and at least 0"
        assert by in ["name", "id", "url"], "by must be one of name, id or url"

        # open file
        if by == "id":
            sheets = client.open_by_key(file)
        elif by == "name":
            sheets = client.open(file)
        elif by == "url":
            sheets = client.open_by_url(file)

        # get the data from the specific sheetNum
        data = sheets.get_worksheet(sheetNum)
        data = data.get_all_records()

        return data

    def authenticateOauth2GDrive(self, client_secretJson:str):
        """
        Authenticates Google Drive API using oauth2 key from user's .json credential file
        returns: service (googleapiclient.discovery.Resource obj)
        """
        CLIENT_SECRET_FILE = client_secretJson
        API_NAME, API_VERSION = "drive", "v3"
        SCOPES = ["https://www.googleapis.com/auth/drive"]
        service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

        return service

    def getFileListInFolder(self, folderID:str, service) -> "list[dict]":
        """
        Uses the Google Drive folder ID to get the list of all file metadata (kind, id, name, mimeType)
        returns: list of dict values for each file in folder
        """
        folder_id = folderID
        query = f"parents = '{folder_id}'"
        response = service.files().list(q=query).execute()
        files = response.get('files')
        nextPageToken = response.get('nextPageToken')

        while nextPageToken:
            response = service.files().list(q=query, pageToken=nextPageToken).execute()
            files.extend(response.get('files'))
            nextPageToken = response.get('nextPageToken')

        return files

#     def getRepo(self, repoDir):
#         return self.gitService.get_repo(repoDir)






