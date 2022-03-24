#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Analyze ParlaMint-RO corpus."""
from argparse import ArgumentParser
import logging
import pandas
import re
from lxml import etree
from datetime import datetime
from pathlib import Path
from itertools import product
from joblib import Parallel, delayed

PRONOUNS = ['eu', 'tu', 'el/ea', 'noi', 'voi', 'ei/ele']


def get_future_forms(df):
    """Get the list of verb forms in the future tense from dataframe.

    Parameters
    ----------
    df: pandas.DataFrame, required
        The dataframe containing verb forms.

    Returns
    -------
    future_forms: list of str
        The verb forms in the future tense.
    """
    pattern = r"^\s*(eu|tu|el\/ea|noi|voi|ei\/ele)\s"
    future_forms = df.Viitor.map(lambda x: re.sub(
        pattern, '', x, 0, flags=re.IGNORECASE | re.MULTILINE))
    return [x.strip() for x in future_forms]


def get_ininitive_forms(df):
    """Get the list of verbs in the infinitive form from the dataframe.

    Parameters
    ----------
    df: pandas.DataFrame, required
        The dataframe containing verb forms.

    Returns
    -------
    infinitive_forms: list of str
        The verb forms in the infinitive.
    """
    mask = df.Infinitiv.notnull()
    infinitive_forms = [x.strip() for x in df[mask].Infinitiv]
    return infinitive_forms


def get_xml_root(file_path):
    """Parse XML from provided file and return the root node.

    Parameters
    ----------
    file_path: str, required
        The path of the XML file to parse.

    Returns
    -------
    xml_root: lxml.Element
        The root element of the XML tree.
    """
    parser = etree.XMLParser(remove_blank_text=True)
    xml_tree = etree.parse(file_path, parser)
    for elem in xml_tree.iter():
        elem.tail = None
    return xml_tree.getroot()


def get_usage_statistics(forms, file_name):
    """Get usage statistics.

    Parameters
    ----------
    forms: iterable of str, required
        The forms for which to count statistics.
    file_name: str, required
        The corpus file from which to count statistics.

    Returns
    -------
    (date, stats): tuple of date, dict
        The date and the statistics for that date.
    """
    xml = get_xml_root(file_name)
    date_elem, *_ = [
        elem for elem in xml.iterdescendants()
        if 'date' in elem.tag and 'setting' in elem.getparent().tag
    ]
    date = datetime.strptime(date_elem.get('when'), '%Y-%m-%d').date()
    stats = {}
    for u in xml.iterdescendants('{http://www.tei-c.org/ns/1.0}u'):
        speaker = u.get('who')
        text = ''.join(u.itertext())
        acc = stats[speaker] if speaker in stats else 0
        acc += sum([text.count(form) for form in forms])
        stats[speaker] = acc
    return date, stats


def iterate_corpus_files(root_file):
    """Iterate over corpus files included in root file.

    Parameters
    ----------
    root_file: str, required
        The path of the root file of the corpus.

    Returns
    -------
    file_generator: generator of str
        The generator of corpus files.
    """
    root_file_path = Path(root_file)
    for file_path in root_file_path.parent.glob('*.xml'):
        if file_path == root_file_path:
            continue
        if '.ana' in file_path.suffixes:
            continue

        file_name = str(file_path)
        logging.info("Extracting statistics from %s.", file_name)
        yield file_name


def main(args):
    """Where the magic happens."""
    logging.info("Loading verb forms.")
    df = pandas.read_csv(args.verb_forms_file)

    logging.info("Extracting future forms.")
    future_forms = get_future_forms(df)

    logging.info("Computing statistics.")

    data = Parallel(n_jobs=-1)(
        delayed(get_usage_statistics)(future_forms, f)
        for f in iterate_corpus_files(args.corpus_root_file))

    logging.info("Aggregating statistics.")
    global_stats = {}
    speakers = set()
    dates = set()
    for date, stats in data:
        dates.add(date)
        global_stats[date] = stats
        for speaker in stats.keys():
            speakers.add(speaker)

    result = {'Speaker': [], 'Date': [], 'UsageCount': []}
    for speaker, date in product(speakers, dates):
        result['Speaker'].append(speaker)
        result['Date'].append(date)
        session_stats = global_stats[date]
        usage_count = session_stats[
            speaker] if speaker in session_stats else None
        result['UsageCount'].append(usage_count)
    df_stats = pandas.DataFrame(result)
    output_file = args.statistics_file
    logging.info("Saving statistics to %s.", output_file)
    df_stats.to_csv(output_file)


def parse_arguments():
    """Parse command-line arguments."""
    parser = ArgumentParser(description='')
    parser.add_argument('--corpus-root-file',
                        help="The path of the ParlaMint corpus root file.",
                        default='../data/corpus/ParlaMint-RO.xml')
    parser.add_argument('--verb-forms-file',
                        help="The path of the CSV file containing verb forms.",
                        default='../data/verb-forms.csv')
    parser.add_argument(
        '--statistics-file',
        help="The path of the output CSV file containing statistics.",
        default='../data/future-usage-stats.csv')
    parser.add_argument(
        '-l',
        '--log-level',
        help="The level of details to print when running.",
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                        level=getattr(logging, args.log_level.upper()))
    main(args)
