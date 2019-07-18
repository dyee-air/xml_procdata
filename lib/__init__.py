from .func import getXmlData, dictFind, pivotMerge, reshapeExtInfo
from .exporters import EXPORT_FORMATS, exportNull


def exportData(data_frame, file_path, format_name=None, **kwargs):
    fmt = format_name or ''
    exporter = EXPORT_FORMATS.get(fmt.lower(), exportNull)
    return exporter(data_frame, file_path, **kwargs)
