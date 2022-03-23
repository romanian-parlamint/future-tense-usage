#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Script for crawling verb forms from https://conjugare.ro."""
import argparse
import logging
import pandas
import requests
from lxml import html
import time
import random

URL = 'https://conjugare.ro'


class Resources:
    """Wrapper class for magic strings."""

    VerbNotFound = "Verbul nu a fost găsit."
    FormNameTag = 'b'
    VerbFormTag = 'div'
    VerbFormCssClass = 'cont_conj'
    ContentBoxXPath = "//div[@class='box_conj']"


def load_verbs(input_file, delimiter=';'):
    """Read the verbs from the specified file and removes duplicates.

    Parameters
    ----------
    input_file : str, required
        The full path of the input CSV file.
    delimiter: str, optional
        The separator char of the input file.
        Default is ';'.

    Returns
    -------
    verb_list : list of str
        The list of verbs.
    """
    df = pandas.read_csv(input_file, sep=delimiter)
    return list(df[df.columns[0]].unique())


def is_form_name(element):
    """Check if the given element is a form name element.

    Parameters
    ----------
    element: etree.Element, required
        The tag to check.

    Returns
    -------
    is_form_name: bool
        Returns True if this is a form name tag; otherwise False.
    """
    return element.tag.lower() == Resources.FormNameTag


def is_verb_form(element):
    """Check if the given element is a verb form element.

    Parameters
    ----------
    element: etree.Element, required
        The tag to check.

    Returns
    -------
    is_verb_form: bool
        Returns True if this is a verb form tag; otherwise False.
    """
    name = element.tag.lower()
    if name != Resources.VerbFormTag:
        return False

    css_class = element.get('class')
    if css_class is not None:
        css_class = css_class.lower()
    return Resources.VerbFormCssClass in css_class


def parse_verb_form(form_content):
    """Parse the verb form from HTML element.

    Parameters
    ----------
    form_content: etree.Element, required
        The HTML element containing inflected verb forms.

    Returns
    -------
    (form_name, verb_forms): tuple of (str, list of str)
        The form name and inflected forms of the verb.
    """
    verb_forms = []
    form_name = ''
    for tag in form_content:
        if is_form_name(tag):
            form_name = tag.text
        if is_verb_form(tag):
            text = ''.join(tag.itertext()).strip()
            if text != '-':
                verb_forms.append(text)
    return (form_name, verb_forms)


def parse_page_contents(page):
    """Parse the contents of the page.

    Parameters
    ----------
    page : etree.Element, required
        The HTML markup of the page.

    Returns
    -------
    verb_forms : dict
        The verb forms extracted from the page.
        None if content cannot be parsed.
    """
    if Resources.VerbNotFound in page:
        return None

    verb_forms = {}
    content_boxes = page.xpath(Resources.ContentBoxXPath)
    for content_box in content_boxes:
        form_name, verbs = parse_verb_form(content_box)
        verb_forms[form_name] = verbs

    return verb_forms


def main(args):
    """Where the magic happens."""
    verbs = load_verbs(args.verbs_file)
    logging.info("Loaded %s distinct entries from verbs file.", len(verbs))
    result = None
    logging.info("Remaining verbs to process: %s", len(verbs))
    logging.info("Start scraping verb forms.")
    for verb in verbs:
        logging.info("Scrapping forms for %s.", verb)
        url = '{}/romana.php?conjugare={}'.format(URL, verb)
        page = requests.get(url)
        page_content = html.fromstring(page.content)
        data = parse_page_contents(page_content)

        if data is None or len(data.values()) == 0:
            logging.info("No data found for %s.", verb)
            continue

        max_len = max([len(col) for col in data.values()])
        for key, value in data.items():
            data[key] = value + ([None] * (max_len - len(value)))
        df = pandas.DataFrame.from_dict(data)
        result = df if result is None else pandas.concat([result, df])
        logging.info("Saving results to %s.", args.output_file)
        result.to_csv(args.output_file)

        sleep_interval = random.randint(0, 1)
        logging.info("Sleeping for %s seconds.", sleep_interval)
        time.sleep(sleep_interval)

    print(result)
    logging.info("That's all folks!")


def parse_arguments():
    """Parse the command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Script for crawling verb forms from https://conjugare.ro.'
    )
    parser.add_argument('--verbs-file',
                        help="Path to the CSV file containing verbs.",
                        default="./data/dex-entries.csv")
    parser.add_argument('--output-file',
                        help="Path to the output file.",
                        default='./data/verb-forms.csv')
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
