# %%
import os
import random
import xml.etree.ElementTree as ElementTree
import pandas as pd
from lib import getXmlData, exportData, reshapeExtInfo
from postproc import getEssays, parseExtInfo

# File paths
SOURCE_DIR = 'R:/project/NAEP Research and Analysis/2017/2017 Event Data/DecompressedFiles/2017Grade8Writing'
OUTPUT_DIR = 'R:/project/DSY/NAEP Writing ETA/data'

# Use predetermined files if NUM_RAND_FILES = 0; otherwise select NUM_RAND_FILES random files
# Or if ALL_FILES is True, use all files in SOURCE_DIR
ALL_FILES = False
NUM_RAND_FILES = 0
NUM_SEQ_FILES = 500

FILE_LIST = [file_name for file_name in os.listdir(
    SOURCE_DIR) if file_name[-3:].lower() == 'xml']

if ALL_FILES:
    SOURCE_FILES = FILE_LIST
elif NUM_RAND_FILES:
    SOURCE_FILES = (random.choice(FILE_LIST) for _ in range(NUM_RAND_FILES))
elif NUM_SEQ_FILES:
    SOURCE_FILES = (FILE_LIST[i] for i in range(NUM_SEQ_FILES))
else:
    # PROBLEMATIC FILES:
    # (Files with POS > text length + 2)
    # 3513001666_ObservableData.Xml
    # 3523008531_ObservableData.Xml
    SOURCE_FILES = [
        '3523008531_ObservableData.Xml'
    ]
# Prepend path to files
SOURCE_FILES = [SOURCE_DIR + '/' + file_name for file_name in SOURCE_FILES]


ESSAY_COLS = ['bookletNumber', 'accessionNumber', 'essay_text']
SURVEY_COLS = ['bookletNumber', 'accessionNumber',
               'blockCode', 'EventTime', 'ExtendedInfo']
# %%
# Master dataframe for export
ESSAY_DATA = pd.DataFrame(
    columns=ESSAY_COLS)
SURVEY_DATA = pd.DataFrame(columns=SURVEY_COLS)

for xml_file in SOURCE_FILES:
    xml_root = ElementTree.parse(xml_file).getroot()
    print('Processing {0}...'.format(os.path.basename(xml_file)) +
          f'({SOURCE_FILES.index(xml_file)+1} of {len(SOURCE_FILES)})')

    # Get bookletNumber
    book_num = getXmlData(xml_root,
                          start_tag='bookletNumber',
                          row_tag='bookletNumber')['text_bookletNumber'][0]

    # Get item data
    df = getXmlData(xml_root, start_tag='itemResult',
                    query='(interpretation=="Pilot Observables" & itemType=="ExtendedText") | (interpretation=="Click Choice" & itemType=="BQChoices")')
    df['bookletNumber'] = book_num
    print('Item data loaded.')

    # Skip if no data found
    if df.empty:
        continue

    # Reshape dataset
    keep_cols = [
        'bookletNumber',
        'itemType',
        'interpretation',
        'accessionNumber',
        'blockCode',
        'fieldIdentifier',
        'ID_outcomeVariable',
        'text_value'
    ]

    df = reshapeExtInfo(df[keep_cols])
    df = df.loc[df.ExtendedInfo.notna()]

    ##############################
    Get essay text for student
    essay_df = df.loc[(df['interpretation'] == 'Pilot Observables')][['accessionNumber',
                                                                      'ID_outcomeVariable', 'EventTime', 'ExtendedInfo']].copy()
    essay_df = parseExtInfo(essay_df)
    essay_df = essay_df.loc[essay_df.ExtendedInfo_name == 'text.change']
    essays = getEssays(essay_df)

    # Append to master dataframe
    for item in essays:
        ESSAY_DATA = ESSAY_DATA.append({'bookletNumber': book_num,
                                        'accessionNumber': item,
                                        'essay_text': essays[item]}, ignore_index=True)

    ##############################
    # Get other data
    survey_df = df.loc[(df['interpretation'] ==
                        'Click Choice')][SURVEY_COLS]
    SURVEY_DATA = SURVEY_DATA.append(survey_df)


if not ESSAY_DATA.empty:
    ESSAY_FILE = OUTPUT_DIR + '/' + 'WRI_G8_2017_ESSAYS.csv'
    exportData(ESSAY_DATA, format_name='CSV', file_path=ESSAY_FILE)

if not SURVEY_DATA.empty:
    SURVEY_FILE = OUTPUT_DIR + '/' + 'WRI_G8_2017_SURVEY.csv'
    exportData(SURVEY_DATA, format_name='CSV', file_path=SURVEY_FILE)

# %%
