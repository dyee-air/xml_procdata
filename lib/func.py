from uuid import uuid4
import dateutil.parser as dtparser
import pandas as pd


def dictFind(in_dict, key):
    '''
    Recursively searches for `key` in `in_dict` and any sub-dicts found
    in `in_dict`.  Returns the first value associated with `key`, or None
    if not found.
    '''
    if not isinstance(in_dict, dict):
        return None

    if key in in_dict:
        return in_dict[key]

    for keyval in in_dict:
        if isinstance(in_dict[keyval], dict):
            child_dict = dictFind(in_dict[keyval], key)
            if child_dict:
                return child_dict

    return None


def getRowList(xml_node, row_tag=None):
    '''
    Returns a list of dicts containing element.attrib for
    element `xml_node` and all child nodes.  If `row_tag`
    is specified, children of elements with `row_tag` are ignored,
    as are any branches not containing `row_tag` elements.
    '''

    # This is a leaf (terminal) node if any of the following:
    #   - Node has no children
    #   - `row_tag` is specified and:
    #       1. Node's tag == `row_tag`, OR
    #       2. No descendants have `row_tag`
    is_leaf = not list(xml_node)

    if row_tag:
        is_leaf = is_leaf or \
            xml_node.tag == row_tag or \
            row_tag not in {elem.tag for elem in xml_node.iter()}

    row_dict = xml_node.attrib.copy()
    row_dict['ID_{0}'.format(xml_node.tag)] = uuid4()

    if xml_node.text and xml_node.text.strip():
        row_dict.update(
            {'text_{0}'.format(xml_node.tag): xml_node.text.strip()})

    if not list(xml_node) or is_leaf:
        return [row_dict]

    return [child_dict.update(row_dict) or child_dict
            for child_node in list(xml_node)
            for child_dict in getRowList(child_node)]


def rowListToDict(row_list):

    colnames = {k for row in row_list for k in row.keys()}

    return {col: [row.get(col, None)
                  for row in row_list]
            for col in colnames}


def rowListToDataFrame(row_list):

    return pd.DataFrame.from_dict(rowListToDict(row_list))


def getXmlData(xml_node, start_tag=None, row_tag=None, query=None):
    '''
    `query`: String to be passed to pandas.DataFrame.query()
    '''
    row_list = list()

    for node in xml_node.iter(start_tag):
        row_list.extend(getRowList(node, row_tag=row_tag))

    data = rowListToDataFrame(row_list)

    if query:
        # Ignore query if invalid or col not found
        try:
            data = data.query(query)
        except:
            pass

    return data


def pivotMerge(data_frame,
               index='ID_outcomeVariable',
               columns='fieldIdentifier',
               values='text_value'):
    """Pivots data frame and re-merges non-ID columns"""

    aux_cols = [col for col in data_frame.columns
                if col not in (columns, values)]

    left_df = data_frame[aux_cols].drop_duplicates()
    right_df = data_frame.pivot(
        index=index, columns=columns, values=values)

    out_df = pd.merge(left_df, right_df, on=index)

    return out_df


def reshapeExtInfo(data_frame):
    """Reshapes EventTime & ExtendedInfo and sorts by EventTime"""

    reshape_df = pivotMerge(data_frame)

    # Parse EventTime as DateTime and sort
    reshape_df['EventTime'] = [dtparser.parse(
        dtstr) for dtstr in list(reshape_df['EventTime'])]
    reshape_df = reshape_df.sort_values(
        by='EventTime')

    return reshape_df
