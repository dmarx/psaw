import click
from .PushshiftAPI import PushshiftAPI
from . import writers as wt
from . import utilities as ut
from pathlib import Path
import pprint


@click.command(context_settings=dict(max_content_width=100))
@click.argument('search_type', type=click.Choice(['comments', 'submissions']), default='comments')
@click.option("-q", "--query", help='search term(s)', type=str)
@click.option("-s", "--subreddits", help='restrict search to subreddit(s)', type=str)
@click.option("-a", "--authors", help='restrict search to author(s)', type=str)
@click.option("-l", "--limit", default=20, help='maximum number of items to retrieve')
@click.option("--before", help='restrict to results before date '
                               '(datetime or int + s,m,h,d; eg, 30d for 30 days)', type=str)
@click.option("--after", help='restrict to results after date '
                              '(datetime or int + s,m,h,d; eg, 30d for 30 days)', type=str)
@click.option("-o", "--output", type=click.Path(),
              help="output file for saving all results in a single file")
@click.option("--output-template", type=str,
              help="""
              output file name template for saving each result in a separate file
              template can include output directory and fields from each result
              note, if using --filter, any fields in output-template MUST be
              included in filtered fields
              
              example:
              'output_path/{author}.{id}.csv'
              'output_path/{subreddit}_{created_utc}.json'

              """)
@click.option('--format', type=click.Choice(['json', 'csv']), default='csv')
@click.option("-f", "--filter", "filter_", type=str,
              help="filter fields to retrieve (must be in quotes or have no spaces), defaults to all")
@click.option("--prettify", is_flag=True, default=False,
              help="make output slightly less ugly (for json only)")
@click.option("--dry-run", is_flag=True, default=False,
              help="print potential names of output files, but don't actually write any files")
@click.option("--no-output-template-check", is_flag=True, default=False)
@click.option("--proxy", type=str, default=None)
@click.option("--verbose", is_flag=True, default=False)
def cli(search_type, query, subreddits, authors, limit, before, after,
        output, output_template, format, filter_, prettify, dry_run,
        no_output_template_check, proxy, verbose):
    """
    retrieve comments or submissions from reddit which meet given criteria

    """

    if output is None and output_template is None:
        raise click.UsageError("must supply either --output or --output-template")

    if output is not None and output_template is not None:
        raise click.UsageError("can only supply --output or --output-template, not both")

    verbose = verbose or dry_run

    if output:
        batch_mode = True
    else:
        batch_mode = False

    api = PushshiftAPI(https_proxy=proxy)
    search_args = dict()

    query = ut.string_to_list(query)
    filter_ = ut.string_to_list(filter_)
    authors = ut.string_to_list(authors)
    subreddits = ut.string_to_list(subreddits)
    before = ut.string_to_epoch(before)
    after = ut.string_to_epoch(after)

    # use a dict to pass args to search function because certain parameters
    # don't have defaults (eg, passing filter=None returns no fields)
    search_args = ut.build_search_kwargs(
        search_args,
        q=query,
        subreddit=subreddits,
        author=authors,
        limit=limit,
        before=before,
        after=after,
        filter=filter_,
    )

    search_functions = {
        'comments': api.search_comments,
        'submissions': api.search_submissions,
    }[search_type]

    if verbose:
        click.echo("calling api with following arguments:")
        click.echo(pprint.pformat(search_args))

    things = search_functions(**search_args)
    thing, things = ut.peek_first_item(things)
    if thing is None:
        click.secho("no results found", err=True, bold=True)
        return

    fields, missing_fields = ut.validate_fields(thing, filter_)

    if missing_fields:
        missing_fields = sorted(missing_fields)
        click.secho("server did not return following fields: {}".format(missing_fields),
                    bold=True, err=True)

    writer_class = choose_writer_class(format, batch_mode)
    writer = writer_class(fields=fields, prettify=prettify)

    if batch_mode:
        save_to_single_file(things, output, writer=writer,
                            count=limit, verbose=verbose, dry_run=dry_run)
    else:
        if not no_output_template_check:
            validate_output_template(output_template)

        save_to_multiple_files(things, output_template, writer=writer,
                               count=limit, verbose=verbose, dry_run=dry_run)


def choose_writer_class(format, batch_mode):
    """
    Choose appropriate writer class

    :param format: str
    :param batch_mode: bool
    :return: Class

    """
    writer_cls = {
        ('json', False): wt.JsonWriter,
        ('json', True): wt.JsonBatchWriter,
        ('csv', False): wt.CsvWriter,
        ('csv', True): wt.CsvBatchWriter,
    }[(format, batch_mode)]

    return writer_cls


def save_to_single_file(things, output_file, writer, count,
                        verbose=False, dry_run=False):
    """
    Write all things to a single file

    :param things: iterable
    :param output_file: str
    :param writer: Writer
    :param count: int
    :param verbose: bool
    :param dry_run: bool

    """
    count = 0
    writer.open(output_file)
    writer.header()
    try:
        with click.progressbar(things, length=count) as things:
            for thing in things:
                if not dry_run:
                    writer.write(thing.d_)
                    count += 1
    finally:
        writer.footer()
        writer.close()

    if verbose:
        click.echo("wrote {} items to {}".format(count, output_file))


def save_to_multiple_files(things, output_template, writer, count,
                           verbose=False, dry_run=False):
    """
    Write things to a separate file per thing

    :param things: iterable
    :param output_template: str
        template should contain python str format codes, such as "{subreddit}.{id}.json"
    :param writer: Writer
    :param count: int
    :param verbose: bool
    :param dry_run: bool

    """
    if dry_run:
        progressbar = ut.DummyProgressBar(things)
    else:
        progressbar = click.progressbar(things, length=count)

    count = 0
    with progressbar as things:
        for thing in things:
            output_file = output_template.format(**thing.d_)
            p = Path(output_file)
            if not p.parent.exists():
                p.parent.mkdir(parents=True)

            if dry_run:
                click.echo("saving to: {}".format(output_file))
            else:
                try:
                    writer.open(output_file)
                    writer.header()
                    writer.write(thing.d_)
                    writer.footer()
                    count += 1
                finally:
                    writer.close()

    if verbose:
        click.echo("wrote {} items".format(count))


def validate_output_template(output_template):
    """
    Crude sanity check that output template looks reasonable

    :param output_template: str

    """
    if "{" not in output_template or "}" not in output_template:
        raise click.BadParameter("output_template must include python "
                                 "string formatting replacement fields\n"
                                 "use --no-output-template-check to override check")

    try:
        p = Path(output_template)
    except:
        raise click.BadParameter("output_template does not look like "
                                 "a valid path\n"
                                 "use --no-output-template-check to override check")

    if len(p.parts) > 4:
        raise click.BadParameter("output_template looks like it is going to "
                                 "create a lot of directories\n"
                                 "use --no-output-template-check to override check")


if __name__ == '__main__':
    cli()



