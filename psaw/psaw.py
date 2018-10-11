import click
from PushshiftAPI import PushshiftAPI
from writers import CsvWriter, JsonWriter

from utilities import validate_fields, peek_first_item
from utilities import _build_search_kwargs, _string_to_list


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

    query = _string_to_list(query)
    fields = _string_to_list(fields)
    authors = _string_to_list(authors)
    subreddits = _string_to_list(subreddits)

    # use a dict to pass args to search_comments() and search_submissions
    # as we can't simply pass some options (eg, filter=None) as that returns no fields
    search_args = _build_search_kwargs(
        search_args,
        q=query,
        subreddit=subreddits,
        author=authors,
        limit=limit,
        filter=fields,
    )

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
        writer_class = JsonWriter
    elif format == 'csv':
        writer_class = CsvWriter

    if output:
        save_to_single_file(gen, output, writer_class=writer_class, fields=fields,
                            count=limit)


def save_to_single_file(gen, output_file, writer_class, fields, count):
    print("writing to file {}".format(output_file))
    writer = writer_class(output_file, multiple_results_per_file=True, fields=fields)

    writer.header()
    try:
        with click.progressbar(gen, length=count) as things:
            for thing in things:
                writer.write(thing.d_)
    finally:
        writer.footer()


if __name__ == '__main__':
    psaw()



