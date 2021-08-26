import GoogleDriveSheets as gds
import DataChecks as dc
import pandas as pd
import numpy as np
import os
from io import BytesIO

def getAllFileIDs(handler):
    """
    Gets all file IDs from the Google Drive folders (raw, clean, byHand)
    returns: cleaned{"byHand":[{id:name}, {id:name}, ...],"fromInst":[{id:name}, {id:name}, ...]}, raw[ids]
    """
    FOLDER_RAW_ID = "17JUv2o-fKmFsgg2m65HNO-TMDUdn5Q2U"
    FOLDER_CLEANED_ID = "191OoRTm1ip05Zuk7My-eMa-t9B2IeJbD"
    FOLDER_BYHAND_ID = "1hbsLRm_1x6adC1OZgULKw16O-li9hRBq"

    CLEANED_SHEETS_IDs = {"byHand":{}, "fromInst":{}}
    FILES_IN_RAW_IDs = []

    filesInCleaned = handler.getFileListInFolder(FOLDER_CLEANED_ID, handler.getDriveService())
    for file in filesInCleaned:
        if file["mimeType"] == "application/vnd.google-apps.spreadsheet":
            fileID = file["id"]
            fileName = file["name"]
            CLEANED_SHEETS_IDs["fromInst"][fileID] = fileName

    filesInByHand = handler.getFileListInFolder(FOLDER_BYHAND_ID, handler.getDriveService())
    for file in filesInByHand:
        if file["mimeType"] == "application/vnd.google-apps.spreadsheet":
            fileID = file["id"]
            fileName = file["name"]
            CLEANED_SHEETS_IDs["byHand"][fileID] = fileName

    filesInRaw = handler.getFileListInFolder(FOLDER_RAW_ID, handler.getDriveService())
    for file in filesInRaw:
        FILES_IN_RAW_IDs.append([file["id"]])
        # print(file)
        # fileMimeType = file["mimeType"]
        # while fileMimeType == "application/vnd.google-apps.folder":
        #     childFilesinFile = handler.getFileListInFolder(file["id"], handler.getDriveService())
        #     FILES_IN_RAW_ID[file["id"]].add

    return CLEANED_SHEETS_IDs, FILES_IN_RAW_IDs

def addNewUniToRepo(repo, df, filePath):
    """
    Adds processed sheets data from Google Drive to the repo as uniName.csv file
    """
    dfCSV_str = df.to_csv()
    repo.create_file(filePath, "commiting new uni data from sheets in drive", dfCSV_str)

def getAllJournals(handler) -> pd.DataFrame:
    """
    Returns a df with all journal names and their issn
    """
    journals = handler.getSheetsData(handler.getSheetsDriveClient(), "1W-A354T_93Nra8rKL_MY5tmwMDlfaLAdLKTwNUJv2EA")
    j_df = pd.DataFrame(journals)[["journal", "issn"]]

    return j_df

def addUniCol(uniName, df):
    """
    Adds the "university" column to the processed sheet df with uniName
    returns: df with uniName col
    """
    uniCol = [uniName]*1372
    df["university"] = uniCol
    df = df[["university", "journal", "issn", "access", "notes"]]

    return df

def mergeMainDB(repo, mainDBPath, newDf):
    """
    Merges the new df with the old mainDB.csv from GitHub repo. Note: This does NOT push the merged
    data to the repo. This only puts the two old DB with the new university data into a new df
    returns:
    - oldDB: mainDB.csv pygithub.ContentFile
    - updatedMainDB: pd.DataFrame with the merged data
    """
    oldDB = repo.get_contents(mainDBPath)
    oldDBContent = oldDB.decoded_content  # dtype=bytes
    oldDBContent = pd.read_csv(BytesIO(oldDBContent))  # dtype=pd.df
    oldDBContent = oldDBContent.astype("string")
    oldDBContent = oldDBContent[["university", "journal", "issn", "access", "notes"]]
    updatedMainDB = pd.concat([newDf, oldDBContent], ignore_index=True)

    return oldDB, updatedMainDB

def updateMainDBGit(repo, oldDB, updatedMainDB, updatedMainDBPath):
    """
    This updates the old mainDB.csv with the new df from mergeMainDB(repo, mainDBPath, newDf)
    """
    updatedMainDB = updatedMainDB.to_csv()
    # repo.delete_file(oldDB.path, "commit message", oldDB.sha)
    # repo.create_file(updatedMainDBPath, "test commit", updatedMainDB)
    repo.update_file(oldDB.path, "updated mainDB.csv", updatedMainDB, oldDB.sha, branch="main")

def getListOfUpdatedSheets(handler):
    """
    Gets the Google Sheets file from SheetsUpdatedToRepo from the drive
    returns: a list of sheets IDs that were already updated to repo
    """
    SHEETS_IN_REPO_FILE_ID = "1jsxtnEHbKTkoPgtcsawsu6oZ7wNOgzqO5dGvtbx2pM4"
    sheet = handler.getSheetsData(handler.getSheetsDriveClient(), SHEETS_IN_REPO_FILE_ID)
    sheetsUpdatedToRepo_df = pd.DataFrame(sheet)
    updatedSheetIDs = list(sheetsUpdatedToRepo_df["sheetID"])

    return updatedSheetIDs

def updateSheetOnDrive(handler, sheetID, ALL_CLEANED_SHEETS):
    SHEETS_IN_REPO_FILE_ID = "1jsxtnEHbKTkoPgtcsawsu6oZ7wNOgzqO5dGvtbx2pM4"
    sheet = handler.getSheetObject(SHEETS_IN_REPO_FILE_ID)
    index = len(sheet.get_all_values()) + 1
    sheet.insert_row([sheetID, ALL_CLEANED_SHEETS[sheetID]], index)


def main():

    # authenticating Drive, Sheets and GitHub API keys
    sheetsDriveJson = "creds.json"
    driveServiceJson = "client_secrets_GDrive-oauth2.json"
    gitToken = os.environ.get("TEST_SECRET")
    handler = gds.Handler(sheetsDriveJson, driveServiceJson, gitToken)

    # google drive sheets (IDs only)
    ALL_JOURNAL_ISSN = getAllJournals(handler)   # pd.DataFrame
    CLEANED_SHEETS_IDs, FILES_IN_RAW_IDs = getAllFileIDs(handler)
    ALL_CLEANED_SHEETS = {**CLEANED_SHEETS_IDs["byHand"], **CLEANED_SHEETS_IDs["fromInst"]}

    # gitHub repo
    repoDir = "sahasukanta/testRepo"
    repo = handler.getRepo(repoDir)

    updatedSheetIDs = getListOfUpdatedSheets(handler)
    failureLog = {}

    allCleanedSheetIDs = list(CLEANED_SHEETS_IDs["fromInst"].keys()) + list(CLEANED_SHEETS_IDs["byHand"].keys())
    for sheetID in allCleanedSheetIDs:

        if sheetID not in updatedSheetIDs:   # this will filter out the sheets that were already processed on previous runs

            if sheetID in list(CLEANED_SHEETS_IDs["byHand"].keys()):
                uniName = CLEANED_SHEETS_IDs["byHand"][sheetID]
            else:
                uniName = CLEANED_SHEETS_IDs["fromInst"][sheetID]

            # getting data from sheet as df
            sheet = handler.getSheetsData(handler.getSheetsDriveClient(), sheetID)
            df = pd.DataFrame(sheet).astype("string")

            try:
                # data checks
                noDuplicates = dc.noDuplicates(df)
                if not noDuplicates: raise dc.DataChecksException("DataFrame contains duplicates.", sheetID, "noDuplicates", "")

                hasAllCols, detail = dc.hasAllColumns(df)
                if not hasAllCols:
                    detail = "Missing columns: " + str(detail)
                    raise dc.DataChecksException(f"DataFrame does not contain all the columns", sheetID, "hasAllCols", detail)

                hasNaN, detail = dc.hasNaN(df)
                if hasNaN:
                    detail = "NaN values found in columns: " + str(detail)
                    raise dc.DataChecksException(f"DataFrame contains NaN values", sheetID, "hasNaN", detail)

                allJournalsCounted, detail = dc.allJournalsCounted(df, list(ALL_JOURNAL_ISSN["journal"]))
                if allJournalsCounted == False:
                    detail = "uncounted journals: " + str(detail)
                    raise dc.DataChecksException(f"Not all journals are present in DataFrame",  sheetID, "allJournalsCounted", detail)

                observed_df_journals_ISSN = df.drop(["access", "notes"], axis=1, inplace=False)
                journalsMatchISSN, detail = dc.journalsMatchISSN(ALL_JOURNAL_ISSN, observed_df_journals_ISSN)
                if journalsMatchISSN == False:
                    detail = "mismatched journals: " + str(detail)
                    raise dc.DataChecksException(f"Journal and ISSN mismatch found in DataFrame", sheetID, "journalsMatchISSN", detail)

            except Exception as e:
                print(f"Sheet for {uniName} did not pass DataChecks. Sheet avoided.")
                print("Error:", e, end='\n')
                failureLog[sheetID] = [uniName, e]

            else:
                try:
                    addNewUniToRepo(repo, df, f"data/from-GDrive/{uniName}.csv")
                except Exception as e:
                    print(f"Google sheet for {uniName} could not be added. It may already exist in repo.")
                    print("Error:", e, end='\n')
                    failureLog[sheetID] = [uniName, e]

                else:
                    print(f"\nNew Google Sheet for {uniName} successfully added to Repo. Will be merged to mainDB.csv now...")
                    try:
                        df = addUniCol(uniName, df)
                        oldDB, updatedMainDB = mergeMainDB(repo, "data/from-GDrive/mainDB.csv", df)
                        updateMainDBGit(repo, oldDB, updatedMainDB, "data/from-GDrive/mainDB.csv")
                        print(f"Data from {uniName} successfully merged and updated to mainDB.csv!")
                    except Exception as e:
                        errorMsg = f"Sheet {uniName} could not be updated to mainDB.csv. But was added seperately as {uniName}.csv."
                        print(errorMsg)
                        print("Error:", e, end='\n')
                        failureLog[sheetID] = [uniName, e, errorMsg]

                    else:
                        try:
                            updateSheetOnDrive(handler, sheetID, ALL_CLEANED_SHEETS)
                            print(f"Sheet ID and name for {uniName} updated to SheetsUpdatedToRepo sheet on the Drive\n")
                        except Exception as e:
                            errorMsg = f"Data from {uniName} updated to mainDB.csv but this could not be updated to SheetsUpdatedToRepo sheet in Google Drive."
                            print(errorMsg)
                            print("Error:", e, end='\n')
                            failureLog[sheetID] = [uniName, e, errorMsg]

    if len(failureLog) > 0:
        try:
            raise Exception
        except Exception:
            for key in failureLog.keys():
                print(key, failureLog[key], sep='\n', end='\n')


main()
