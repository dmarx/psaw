import itertools


def validate_fields(item, fields=None):
    """
    Check that all requested fields were returned

    :param item: comment or submission
    :param fields: list[str]
    :return: list[str]
    """
    actual_fields = item.d_.keys()

    if fields is None:
        requested_fields = actual_fields
    else:
        requested_fields = fields

    missing_fields = set(requested_fields).difference(actual_fields)

    # drop extra fields returned from api
    final_fields = set(requested_fields).intersection(actual_fields)
    return final_fields, missing_fields


def peek_first_item(gen):
    """
    Peek at first item from generator if available, else return None

    :param gen: generator
    :return: first item, generator

    """
    try:
        item = next(gen)
    except StopIteration:
        item = None

    gen = itertools.chain([item], gen)

    return item, gen


def slice_dict(d, keys):
    """
    Only return subset of dict keys

    :param d: dict
    :param keys: list
    :return: dict
    """
    keys = set(keys).intersection(d.keys())

    sd = {k:d[k] for k in keys}

    return sd


def build_search_kwargs(d, **kwargs):
    """
    Builds kwargs for api.search

    Only includes non None items in d

    :param d: dict
    :param kwargs:
    :return: dict

    """
    nd = d.copy()
    for k, v in kwargs.items():
        if v is not None:
            nd[k] = v

    return nd


def string_to_list(s):
    """
    Convert argument string (of potentially a list of values) to a list of strings

    :param s: str
    :return: list[str]

    """
    if s is not None:
        s = [c.strip() for c in s.split(',')]
    return s


class DummyProgressBar(object):
    """
    Dummy progress bar that just returns generator but displays no output.

    """
    def __init__(self, things):
        self.things = things

    def __enter__(self):
        return self.things

    def __exit__(self, type, value, traceback):
        pass
