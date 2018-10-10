import itertools

import click
from PushshiftAPI import PushshiftAPI
from writers import JsonWriter, CsvWriter



@click.group()
@click.option("--proxy")
@click.pass_context
def psaw(ctx, proxy):
    print("setting up api")
    ctx.ensure_object(dict)
    ctx.obj['api'] = PushshiftAPI()


@psaw.command()
@click.option("--subreddit")
@click.option("--author")
@click.option("--limit", default=20, help='maximum number of comments to retrieve')
@click.option("--output", type=click.File(mode='w'))
@click.option("--fields", type=str,
              help="fields to retrieve (must be in quotes or have no spaces), defaults to all")
@click.pass_context
def comments(ctx, subreddit, author, output, limit, fields):
    api = ctx.obj['api']

    if fields is not None:
        fields = [c.strip() for c in fields.split(',')]

    gen = api.search_comments(subreddit=subreddit, author=author, limit=limit, filter=fields)
    item, gen = peek_first_item(gen)

    if item is None:
        click.echo("no results found")
        return

    if output:
        save_to_single_file(gen, output, CsvWriter)


def save_to_single_file(gen, output_file, writer_class):
    print("writing to file {}".format(output_file))
    item, gen = peek_first_item(gen)
    fields = item.d_.keys()
    writer = writer_class(output_file, multiple_results_per_file=True, fields=fields)

    writer.header()
    try:
        for comment in gen:
            writer.write(comment.d_)
    finally:
        writer.footer()


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



if __name__ == '__main__':
    psaw()




