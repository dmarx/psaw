import json
import csv

from utilities import slice_dict


class Writer(object):
    def __init__(self):
        self.fp = None

    def header(self):
        pass

    def footer(self):
        pass

    def open(self, fp):
        if hasattr(fp, 'write'):
            self.fp = fp
        else:
            self.fp = open(fp, 'w')

    def close(self):
        if hasattr(self.fp, 'close_intelligently'):
            self.fp.close_intelligently()
        else:
            self.fp.close()


class JsonWriter(Writer):
    """
    Output comments/submissions in JSON format

    """
    def __init__(self, fields, delimiter=',', prettify=False, **kwargs):
        super().__init__()
        self.fields = fields
        self.prettify = prettify
        self.delimiter = delimiter
        self.items = 0

        if prettify:
            self.indent = 2
            self.delimiter = self.delimiter + '\n'
        else:
            self.indent = None

    def write(self, obj):
        obj = slice_dict(obj, self.fields)
        json.dump(obj, self.fp, indent=self.indent)
        self.items += 1


class JsonBatchWriter(Writer):
    """
    Output comments/submissions in JSON format

    """
    def __init__(self, fields, delimiter=',', prettify=False, **kwargs):
        super().__init__()
        self.fields = fields
        self.prettify = prettify
        self.delimiter = delimiter
        self.items = 0

        if prettify:
            self.indent = 2
            self.delimiter = self.delimiter + '\n'
        else:
            self.indent = None

    def header(self):
        self.fp.write('[')

    def footer(self):
        self.fp.write(']')

    def write(self, obj):
        obj = slice_dict(obj, self.fields)

        if self.items > 0:
            # we've already written something, so
            # append a comma to make this a json list
            self.fp.write(self.delimiter)

        json.dump(obj, self.fp, indent=self.indent)
        self.items += 1


class CsvBatchWriter(Writer):
    """
    Output comments/submissions in CSV format

    """
    def __init__(self, fields, delimiter=',', **kwargs):
        super().__init__()
        self.fields = fields
        self.items = 0
        self.writer = None
        self.delimiter = delimiter

    def open(self, fp):
        super().open(fp)
        self.writer = csv.DictWriter(fp,
                                     delimiter=self.delimiter,
                                     fieldnames=self.fields)

    def header(self):
        self.writer.writeheader()

    def footer(self):
        pass

    def write(self, obj):
        obj = slice_dict(obj, self.fields)
        self.writer.writerow(obj)
        self.items += 1
