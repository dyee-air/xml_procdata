# %%
import json
import xml.etree.ElementTree as ElementTree
import dateutil.parser as dtparser
import pandas as pd
from lib import dictFind, getXmlData, pivotMerge, reshapeExtInfo


def getEssays(data_frame):
    """Requires columns 'accessionNumber' and 'ExtendedInfo_diffs'"""

    item_nums = list(data_frame['accessionNumber'].drop_duplicates())
    out_txts = dict.fromkeys(item_nums)

    for item in item_nums:
        print('Processing essay item "{}"...'.format(item))
        item_cmds = (cmd for row in data_frame.loc[data_frame.accessionNumber ==
                                                   item].ExtendedInfo_diffs if row for cmd in row)
        text = ''
        for cmd in item_cmds:
            text = processInput(text, **cmd)

        out_txts[item] = text

    return out_txts


def processInput(in_str, **kwargs):

    try:
        cmd_str = kwargs['edit']
        txt_pos = kwargs['pos']
        txt_len = kwargs['len']
        txt = kwargs['text']
    except KeyError:
        return None

    chars = list(in_str)

    def cmdDel():
        nonlocal chars, txt_len, txt_pos
        for _ in range(txt_len):
            # Position can sometimes be greater than the length??  Default to end instead
            txt_pos = min([txt_pos, len(chars)])
            if chars:
                chars.pop(txt_pos-1)

    def cmdIns():
        # Some (but not all) spaces seem to be encoded.  Replace these
        nonlocal chars, txt, txt_pos
        txt = txt.replace('\xa0', ' ')
        for i, char in enumerate(list(txt)):
            chars.insert(txt_pos-1+i, char)

    cmd = {'INS': cmdIns,
           'DEL': cmdDel}

    cmd[cmd_str]()

    return ''.join(chars)


# %%


def parseExtInfo(data_frame, attr_list=('name', 'diffs', 'textLength')):
    """Convert ExtendedInfo JSON data from Pilot Observables into columns"""

    if not 'ExtendedInfo' in data_frame.columns:
        return None

    # '3513001666_ObservableData.Xml': Why is there invalid JSON data in a "Pilot Observables" row?
    # Should be "Vertical Item Scroll"?
    # Need to work around this
    ext_info = list()
    for val in data_frame['ExtendedInfo']:
        try:
            ext_info.append(json.loads(val))
        except json.JSONDecodeError:
            ext_info.append(val)

    for colname in attr_list:
        data_frame['ExtendedInfo_{0}'.format(colname)] = [dictFind(
            val, colname) or val for val in ext_info]

    return data_frame


def postProcWriting(data_frame):
    # Somewhat hacky at the moment - needs clean up and generalization
    df = data_frame.query('interpretation=="Pilot Observables"')
    df = df[['accessionNumber', 'itemType', 'blockCode', 'ID_outcomeVariable',
             'fieldIdentifier', 'text_value']]

    # Columns to uniquely identify transformed rows
    id_cols = ['accessionNumber', 'itemType',
               'blockCode', 'ID_outcomeVariable']

    # Pivot to create one column for time and another for ExtendedInfo
    cols_df = df.drop_duplicates(id_cols)[id_cols]
    data_df = df.pivot(index='ID_outcomeVariable',
                       columns='fieldIdentifier', values='text_value')

    # Drop missing ExtendedInfo and re-merge ID columns
    data_df = data_df[data_df['ExtendedInfo'].notna()]
    data_df = pd.merge(cols_df, data_df, on='ID_outcomeVariable')

    # Parse EventTime as datetime and sort
    data_df['EventTime'] = [dtparser.parse(
        dtstr) for dtstr in list(data_df['EventTime'])]
    data_df = data_df.sort_values(
        by=['EventTime', 'accessionNumber', 'ID_outcomeVariable'])

    # Unwind columns
    # '3513001666_ObservableData.Xml': Why is there invalid JSON data in a "Pilot Observables" row?
    # Should be "Vertical Item Scroll"?
    # Need to work around this
    ext_info = list()
    for val in data_df['ExtendedInfo']:
        try:
            ext_info.append(json.loads(val))
#   ext_info = [json.loads(val) for val in data_df['ExtendedInfo']]
        except json.JSONDecodeError:
            ext_info.append({})

    for colname in ['name', 'diffs', 'textContext', 'textLength']:
        data_df['ExtendedInfo_{0}'.format(colname)] = [dictFind(
            val_dict, colname) for val_dict in ext_info]

    return data_df.loc[(data_df.ExtendedInfo_name == 'text.change') & (data_df.ExtendedInfo_diffs.notna())]


# %%
