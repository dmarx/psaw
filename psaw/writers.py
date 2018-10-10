import json
import csv


class JsonWriter(object):
    """
    Output comments/submissions in JSON format

    """
    def __init__(self, fp, multiple_results_per_file):
        self.fp = fp
        self.multiple_results_per_file = multiple_results_per_file
        self.items = 0

    def header(self):
        if self.multiple_results_per_file:
            self.fp.write('[')

    def footer(self):
        if self.multiple_results_per_file:
            self.fp.write(']')

    def write(self, obj):
        if self.multiple_results_per_file and self.items > 0:
            # we've already written something, so
            # append a comma to make this a json list
            self.fp.write(',')
        json.dump(obj, self.fp)
        self.items += 1


class CsvWriter(object):
    """
    Output comments/submissions in CSV format

    """
    def __init__(self, fp, multiple_results_per_file, fields, delimiter=','):
        self.fp = fp
        self.multiple_results_per_file = multiple_results_per_file
        self.fields = fields
        self.writer = csv.DictWriter(fp, delimiter=delimiter, fieldnames=fields)
        self.items = 0

    def header(self):
        self.writer.writeheader()

    def footer(self):
        pass

    def write(self, obj):
        self.writer.writerow(obj)
        self.items += 1

