import json
import csv

from .utilities import slice_dict


class Writer(object):
    """
    Base Writer class

    """
    def __init__(self, fields):
        self.fields = fields
        self.fp = None

    def header(self):
        """
        Write header to output file if necessary

        """
        pass

    def footer(self):
        """
        Write footer to output file if necessary

        """
        pass

    def open(self, fp):
        """
        Open output file for writing if necessary

        """
        if hasattr(fp, 'write'):
            self.fp = fp
        else:
            self.fp = open(fp, 'w', encoding='utf8', newline='')

    def close(self):
        """
        Close output file if necessary

        """
        if hasattr(self.fp, 'close_intelligently'):
            self.fp.close_intelligently()
        else:
            self.fp.close()


class JsonWriter(Writer):
    """
    Output comments/submissions in JSON format, all things to a single file

    """
    def __init__(self, fields, prettify=False, delimiter=',', **kwargs):
        super().__init__(fields=fields)
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


class JsonBatchWriter(JsonWriter):
    """
    Output comments/submissions in JSON format, one file per thing

    """

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


class CsvWriter(Writer):
    """
    Output comments/submissions in CSV format, one file per thing

    """
    def __init__(self, fields, delimiter=',', **kwargs):
        super().__init__(fields=fields)
        self.items = 0
        self.writer = None
        self.delimiter = delimiter

    def open(self, fp):
        super().open(fp)
        self.writer = csv.DictWriter(self.fp,
                                     delimiter=self.delimiter,
                                     fieldnames=self.fields)

    def header(self):
        self.writer.writeheader()

    def write(self, obj):
        obj = slice_dict(obj, self.fields)
        self.writer.writerow(obj)
        self.items += 1


class CsvBatchWriter(CsvWriter):
    """
    Output comments/submissions in CSV format, all to a single file
    """
    # defined just for consistency with Json/JsonBatch
    pass
