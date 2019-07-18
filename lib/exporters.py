import csv


def exportCsv(data_frame, file_path, **kwargs):
    kwargs = kwargs or {'index': False, 'quoting': csv.QUOTE_NONNUMERIC}
    with open(file_path, 'w', newline='\n', encoding='utf-8') as f:
        data_frame.to_csv(f, **kwargs)


def exportNull(data_frame, *args, **kwargs):
    print(data_frame)

EXPORT_FORMATS = {
    'csv': exportCsv
}