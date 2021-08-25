import GoogleDriveSheets as gds
import DataChecks as dc
import pandas as pd
import numpy as np
from io import BytesIO
import os

def getAllFileIDs(handler):
    """
    Gets all file IDs from the Google Drive folders (raw, clean, byHand)
    returns: cleaned{"byHand":[ids],"fromInst":[ids]}, raw[ids]
    """
    FOLDER_RAW_ID = "17JUv2o-fKmFsgg2m65HNO-TMDUdn5Q2U"
    FOLDER_CLEANED_ID = "191OoRTm1ip05Zuk7My-eMa-t9B2IeJbD"
    FOLDER_BYHAND_ID = "1hbsLRm_1x6adC1OZgULKw16O-li9hRBq"

    CLEANED_SHEETS_IDs = {"byHand":[], "fromInst":[]}
    FILES_IN_RAW_IDs = []

    filesInCleaned = handler.getFileListInFolder(FOLDER_CLEANED_ID, handler.getDriveService())
    for file in filesInCleaned:
        if file["mimeType"] == "application/vnd.google-apps.spreadsheet":
            CLEANED_SHEETS_IDs["fromInst"].append(file["id"])

    filesInByHand = handler.getFileListInFolder(FOLDER_BYHAND_ID, handler.getDriveService())
    for file in filesInByHand:
        if file["mimeType"] == "application/vnd.google-apps.spreadsheet":
            CLEANED_SHEETS_IDs["byHand"].append(file["id"])

    filesInRaw = handler.getFileListInFolder(FOLDER_RAW_ID, handler.getDriveService())
    for file in filesInRaw:
        FILES_IN_RAW_IDs.append([file["id"]])
        # print(file)
        # fileMimeType = file["mimeType"]
        # while fileMimeType == "application/vnd.google-apps.folder":
        #     childFilesinFile = handler.getFileListInFolder(file["id"], handler.getDriveService())
        #     FILES_IN_RAW_ID[file["id"]].add

    return CLEANED_SHEETS_IDs, FILES_IN_RAW_IDs


def commitFile(file):
    pass

def getAllJournals(handler) -> pd.DataFrame:
    journals = handler.getSheetsData(handler.getSheetsDriveClient(), "1W-A354T_93Nra8rKL_MY5tmwMDlfaLAdLKTwNUJv2EA")
    j_df = pd.DataFrame(journals)[["journal", "issn"]]
    return j_df

def addUniCol(uniName, df):
    uniCol = [uniName]*1372
    df["university"] = uniCol
    df = df[["university", "journal", "issn", "access", "notes"]]
    return df

def mergeMainDB(repo, mainDBPath, newDf):
    oldDB = repo.get_contents(mainDBPath)
    oldDBContent = oldDB.decoded_content  # dtype=bytes
    oldDBContent = pd.read_csv(BytesIO(oldDBContent))  # dtype=pd.df
    oldDBContent = oldDBContent.astype("string")
    oldDBContent = oldDBContent[["university", "journal", "issn", "access", "notes"]]
    updatedMainDB = pd.concat([newDf, oldDBContent], ignore_index=True)
    return oldDB, updatedMainDB

def updateMainDBGit(repo, oldDB, updatedMainDB, updatedMainDBPath):
    updatedMainDB = updatedMainDB.to_csv()
    repo.delete_file(oldDB.path, "commit message", oldDB.sha)
    repo.create_file(updatedMainDBPath, "commit message", updatedMainDB)



def main():

    # authenticating Drive, Sheets and GitHub API keys
    sheetsDriveJson = "creds.json"
    driveServiceJson = "client_secrets_GDrive-oauth2.json"
#     gitToken = secret
    handler = gds.Handler(sheetsDriveJson, driveServiceJson)

#     # repo
#     repo = handler.getRepo("sahasukanta/testRepo")

#     # google drive sheets (IDs only)
#     ALL_JOURNAL_ISSN = getAllJournals(handler)   # pd.DataFrame
#     CLEANED_SHEETS_IDs, FILES_IN_RAW_IDs = getAllFileIDs(handler)

#     # gitHub repo
#     repoDir = "sahasukanta/testRepo"
#     repo = handler.getRepo(repoDir)

#     sheetID = CLEANED_SHEETS_IDs["fromInst"][4]
#     sheet = handler.getSheetsData(handler.getSheetsDriveClient(), sheetID)
#     df = pd.DataFrame(sheet).astype("string")


#     # data checks
#     correctShape, correctCols, noDuplicates = dc.basicChecks(df)
#     if not correctShape: raise dc.DataChecksException("DataFrame shape is not (1372, 4).", sheetID, "correctShape", f"shape is {df.shape}")
#     if not correctCols: raise dc.DataChecksException("DataFrame columns are not ['journal', 'issn', 'access', 'notes'].", sheetID, "correctCols", f"columns are {df.columns}")
#     if not noDuplicates: raise dc.DataChecksException("DataFrame contains duplicates.", sheetID, "correctCols", "")

#     hasNaN, detail = dc.hasNaN(df)
#     if hasNaN:
#         detail = "NaN values found in columns: " + str(detail)
#         raise dc.DataChecksException(f"DataFrame contains NaN values", sheetID, "hasNaN", detail)

#     allJournalsCounted, detail = dc.allJournalsCounted(df, list(ALL_JOURNAL_ISSN["journal"]))
#     if allJournalsCounted == False:
#         detail = "uncounted journals: " + str(detail)
#         raise dc.DataChecksException(f"Not all journals are present in DataFrame",  sheetID, "allJournalsCounted", detail)

#     observed_df_journals_ISSN = df.drop(["access", "notes"], axis=1, inplace=False)
#     journalsMatchISSN, detail = dc.journalsMatchISSN(ALL_JOURNAL_ISSN, observed_df_journals_ISSN)
#     if journalsMatchISSN == False:
#         detail = "mismatched journals: " + str(detail)
#         raise dc.DataChecksException(f"Journal and ISSN mismatch found in DataFrame", sheetID, "journalsMatchISSN", detail)

#     # upload sheet to git repo
#     # df_CSVstr = df.to_csv()
#     # repo.create_file("ualberta.csv", "test Commit", df_CSVstr)

#     df = addUniCol("mississippi", df)
#     oldDB, updatedMainDB = mergeMainDB(repo, "mainDB.csv", df)
#     updateMainDBGit(repo, oldDB, updatedMainDB, "mainDB.csv")



main()

