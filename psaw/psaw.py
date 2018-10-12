import sys

import click
from PushshiftAPI import PushshiftAPI
from writers import CsvBatchWriter, JsonBatchWriter

from utilities import validate_fields, peek_first_item
from utilities import build_search_kwargs, string_to_list


@click.command()
@click.argument('type', type=click.Choice(['comments', 'submissions']), default='csv')
@click.option("-q", "--query", help='search term(s)', type=str)
@click.option("-s", "--subreddits", help='restrict search to subreddit(s)', type=str)
@click.option("-a", "--authors", help='restrict search to author(s)', type=str)
@click.option("-l", "--limit", default=20, help='maximum number of items to retrieve')
@click.option("-o", "--output", type=click.File(mode='w'))
@click.option('--format', type=click.Choice(['json', 'csv']), default='csv')
@click.option("-f", "--fields", type=str,
              help="fields to retrieve (must be in quotes or have no spaces), defaults to all")
@click.option("--proxy")
def psaw(type, query, subreddits, authors, limit, output, format, fields, proxy):
    api = PushshiftAPI()
    search_args = dict()

    query = string_to_list(query)
    fields = string_to_list(fields)
    authors = string_to_list(authors)
    subreddits = string_to_list(subreddits)

    # use a dict to pass args to search_comments() and search_submissions
    # as we can't simply pass some options (eg, filter=None) as that returns no fields
    search_args = build_search_kwargs(
        search_args,
        q=query,
        subreddit=subreddits,
        author=authors,
        limit=limit,
        filter=fields,
    )
    print(output)
    gen = api.search_comments(**search_args)
    item, gen = peek_first_item(gen)
    if item is None:
        click.secho("no results found", err=True, bold=True)
        return

    fields, missing_fields = validate_fields(item, fields)

    if missing_fields:
        missing_fields = sorted(missing_fields)
        click.secho("following fields were not retrieved: {}".format(missing_fields),
                    bold=True, err=True)

    if format == 'json':
        writer = JsonBatchWriter(fields=fields)
    elif format == 'csv':
        writer = CsvBatchWriter(fields=fields)

    if output:
        save_to_single_file(gen, output, writer=writer, count=limit)


def save_to_single_file(gen, output_file, writer, count):
    writer.open(output_file)
    writer.header()
    try:
        with click.progressbar(gen, length=count) as things:
            for thing in things:
                writer.write(thing.d_)
    finally:
        writer.footer()
        writer.close()


if __name__ == '__main__':
    psaw()



