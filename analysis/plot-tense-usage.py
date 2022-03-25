#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Plot the tense usage statistics."""
from argparse import ArgumentParser
import pandas as pd
from pathlib import Path
import re

# Cele mai frecvente verbe
# top 10 cei mai promițători
# Statistici pe partide


def get_legislature_term(file_name):
    """Get legislature term from file name.

    Parameters
    ----------
    file_name: str, required
        The file name containing legislature info.

    Returns
    -------
    (start, end): tuple of int, int
        Start and end year of the legislature.
    """
    regex = r"(\d{4})-(\d{4})"
    match = re.search(regex, file_name)
    start, end = match.groups()
    return (int(start), int(end))


def get_name_parts(name_or_id):
    """Split the name or id into parts.

    Parameters
    ----------
    name_or_id: str, required
        The name or id of the speaker.

    Returns
    -------
    name_parts: set
        The set of canonical name parts.
    """
    name = name_or_id.replace('#', '')
    name = name.replace('Ș', 's')
    name = name.replace('ș', 's')
    name = name.replace('Ț', 't')
    name = name.replace('ț', 't')
    name = name.replace('-', ' ').lower()
    name_parts = name.split()
    return set(name_parts)


def load_legislature_data(data_directory):
    """Iterate over CSV files in directory and load data.

    Parameters
    ----------
    data_directory: str, required
        The path of parent directory containing legislature data.
    """
    names = {'Name': [], 'Start': [], 'End': [], 'Party': []}
    name_parts = {}
    data_dir = Path(data_directory)
    for file_path in data_dir.glob('*.csv'):
        df = pd.read_csv(str(file_path))
        start, end = get_legislature_term(file_path.stem)

        for _, tup in df.iterrows():
            name = tup[0]
            party = str(tup[1]).strip()
            names['Name'].append(name)
            names['Start'].append(start)
            names['End'].append(end)
            names['Party'].append(party)
            if name not in name_parts:
                name_parts[name] = get_name_parts(name)
    return (name_parts, pd.DataFrame(names))


def find_name(speaker_id, names):
    """Lookup speaker name based on the id.

    Parameters
    ----------
    speaker_id: str, required
        The id of the speaker.
    names: dict of str, set
        The dictionary of names and name parts.

    Returns
    -------
    name: str
        Speaker name if found; otherwise None.
    """
    name_parts = get_name_parts(speaker_id)
    print(name_parts)
    for name, parts in names.items():
        if name_parts == parts:
            return name
    return None


def plot_most_frequent_forms(args):
    """Create a plot of most frequent verb forms."""
    stats = pd.read_csv(args.statistics_file)
    grouped = stats.groupby(stats.Form)
    aggregated = grouped['Count'].sum().sort_values(ascending=False)
    aggregated = aggregated.head(args.N).to_frame()
    fig = aggregated.plot(kind='bar', legend=False, rot=90).get_figure()
    fig.tight_layout()
    fig.savefig(args.output_file)
    if args.save_plot_data:
        aggregated.to_csv(args.plot_data_file)
    print(aggregated)


def plot_top_speakers(args):
    """Create plot of top speakers."""
    name_parts, affiliations = load_legislature_data(args.legislatures)
    stats = pd.read_csv(args.statistics_file)
    grouped = stats.groupby(stats.Speaker)
    aggregated = grouped['UsageCount'].sum().sort_values(ascending=False)
    aggregated = aggregated.head(args.N).to_frame()
    speakers, usage_counts = [], []
    for speaker_id, row in aggregated.iterrows():
        speaker_name = find_name(speaker_id, name_parts)
        speakers.append(
            speaker_name if speaker_name is not None else speaker_id)
        usage_counts.append(row['UsageCount'])

    data = pd.DataFrame({'Speakers': speakers, 'UsageCount': usage_counts})
    data.set_index('Speakers', inplace=True)

    fig = data.plot(kind='bar',
                    title="Top {} speakers using future forms".format(args.N),
                    xlabel="Spearker name",
                    ylabel="Count of future forms used",
                    alpha=0.75,
                    rot=90).get_figure()
    fig.tight_layout()
    fig.savefig(args.output_file)
    print(data)
    if args.save_plot_data:
        data.to_csv(args.plot_data_file)


def parse_arguments():
    """Parse command-line arguments."""
    root_parser = ArgumentParser()
    root_parser.add_argument(
        '--legislatures',
        help="Directory path containing deputy info per legislature.",
        default='../data/legislatures')
    root_parser.add_argument(
        '--save-plot-data',
        help="When supplied instructs the script to store data shown in plot",
        action='store_true')
    root_parser.add_argument('--plot-data-file',
                             help="Path of the file where to store plot data.",
                             default='../data/plot-data.csv')
    subparsers = root_parser.add_subparsers()

    top_forms = subparsers.add_parser('top-forms',
                                      help="Plot most frequent N verb forms.")
    top_forms.set_defaults(func=plot_most_frequent_forms)
    top_forms.add_argument('-N',
                           help="Number of forms to plot.",
                           type=int,
                           default=100)
    top_forms.add_argument(
        '--statistics-file',
        help="Path of the file containing usage statistics.",
        default='../data/future-usage-per-form.csv')
    top_forms.add_argument('--output-file',
                           help="Path of the output file.",
                           default='../plots/top-verb-forms.png')

    top_speakers = subparsers.add_parser(
        'top-speakers', help="Plot most frequent N users of verb forms.")
    top_speakers.set_defaults(func=plot_top_speakers)
    top_speakers.add_argument('-N',
                              help="Number of speakers to plot.",
                              type=int,
                              default=10)
    top_speakers.add_argument(
        '--statistics-file',
        help="Path of the file containing usage statistics.",
        default='../data/future-usage-stats.csv')
    top_speakers.add_argument('--output-file',
                              help="Path of the output file",
                              default="../plots/top-speakers.png")

    return root_parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    args.func(args)
