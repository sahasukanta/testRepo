import numpy as np
import pandas as pd

class DataChecksException(Exception):

    def __init__(self, msg, sheetID, ref, detail):
        self.msg = msg
        self.sheetID = sheetID
        self.ref = ref
        self.detail = detail

    def getSheetID(self):
        return self.sheetID

    def getType(self):
        return self.type

    def getDetail(self):
        return self.detail

    def __str__(self):
        return f"{self.msg} \nSheetID: {self.sheetID} \nRef: {self.ref} \nDetail: {self.detail}"


def check_issn(issn:str) -> bool:
    # TODO:
    # - check that the first 7 digits are numbers and the last digit is either a number or "X"
    # - check that the last character is valid (matches the calculations based on the first 7 digits)
    # - check that t
    isVALID_C = validate_cISSN(issn)
    pass

def check_journal(name:str) -> bool:
    pass

def check_access(zero_or_one:int) -> bool:
    pass

def check_notes(notes:str) -> bool:
    pass

def validate_cISSN(issn:str) -> bool:
    """
    Validates the last character (c) of the ISSN number, based on the first 7 digits
    returns: boolean: True if c is valid False otherwise
    """
    assert type(issn) == str, "issn must be a string"

    issn_num = issn[:4] + issn[5:-1]
    issn_c = issn[-1]

    # check c validity
    issn_num_sum = 0
    inv_index = 8
    for num in issn_num:
        num = int(num)
        issn_num_sum += num*inv_index
        inv_index -= 1

    mod = issn_num_sum%11
    if mod == 0: c = 0
    else:
        c = 11-mod
        if c == 10: c = 'X'

    return str(c) == issn_c


# print(validate_cISSN("0046-225X"))


def basicChecks(df:pd.core.frame.DataFrame) -> "tuple(bool, bool, bool)":
    SHAPE = (1372, 4)
    COLS = ["journal", "issn", "access", "notes"]
    correctShape = df.shape == SHAPE
    correctCols = list(df.columns) == COLS
    noDuplicates = list(df.duplicated().unique()) == [False]

    return correctShape, correctCols, noDuplicates


def hasNaN(df:pd.core.frame.DataFrame, includeNotes=False) -> "tuple(bool, list)":
    """
    Checks if there are any missing values in each column of the df
    returns: (True, [cols with missing values]) if missing values present in any column, (False, []) otherwise
    """
    if not includeNotes:
        df = df.drop("notes", inplace=False, axis=1)

    df.fillna(value=np.nan, inplace=True) # for replacing None values (.replace does not work with None)
    oddWords = ["missing", "MISSING", "Missing", "null", "Null", "NULL",
                "None", "none", "NONE", "N/A", "n/a", "-", '', ' ',
                "  ", "   ", "x", np.inf]
    for word in oddWords:
        df.replace(word, np.nan, inplace=True)

    isnullCols = {}
    hasNaNCols = []
    hasNaN = False
    for col in df.columns:
        isnullCols[col] = df[col].isnull().unique()
        if (True in isnullCols[col]):
            hasNaN = True
            hasNaNCols.append(col)

    return hasNaN, hasNaNCols

def allJournalsCounted(df:pd.core.frame.DataFrame, allJournals:list) -> "tuple(bool, list)":
    """
    Checks if all journals are recorded for a university df
    returns: (True, []) if all journals are present, (False, [uncountedJournals]) otherwise
    """
    df_journals = list(df["journal"])
    uncountedJournals = []
    allAreCounted = True
    for journal in allJournals:
        if journal not in df_journals:
            allAreCounted = False
            uncountedJournals.append(journal)

    return allAreCounted, uncountedJournals

def journalsMatchISSN(gtruth_df:pd.core.frame.DataFrame, observed_df:pd.core.frame.DataFrame) -> "tuple(bool, list)":
    """
    Compares the journal, ISSN pairing in gtruth_df with observed_df to check if they match.
    Both parameters require pd.DataFrames with two columns (journal, issn).
    returns: (True, []) if no mismatch found, (False, [mismatchedJournals]) otherwise
    """
    gTruthDf = gtruth_df.set_index("journal", inplace=False).astype("string")
    observedDf = observed_df.set_index("journal", inplace=False).astype("string")
    mismatchedJournals = []
    noMismatch = True
    for j in list(gTruthDf.index):
        if str(gTruthDf.loc[j]) != str(observedDf.loc[j]):
            noMismatch = False
            mismatchedJournals.append(j)

    return noMismatch, mismatchedJournals


