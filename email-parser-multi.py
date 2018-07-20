# coding: utf-8


from __future__ import print_function

from urllib.parse import urlsplit, urljoin, urlparse
from collections import deque, defaultdict
import re
import sys
import argparse
import os

import pandas as pd
import numpy as np
import turbodbc

from bs4 import BeautifulSoup
import requests
import requests.exceptions

from multiprocessing import Pool
from multiprocessing.managers import BaseManager, DictProxy


class MyManager(BaseManager):
    pass


MyManager.register('defaultdict', defaultdict, DictProxy)


def batch_(x, n):
    max_len = len(x)
    i = 0
    while i < max_len:
        batch = x[i:min(i + n, max_len)]
        i += n
        yield batch


def sql_to_dataframe(connection, query, *args, **kwargs):
    df = pd.read_sql(query, connection, *args, **kwargs)
    return df


def create_connection_turbo(server, database):
    options = turbodbc.make_options(prefer_unicode=True)
    constr = 'Driver={ODBC Driver 13 for SQL Server};Server=' + \
        server + ';Database=' + database + ';Trusted_Connection=yes;'
    con = turbodbc.connect(connection_string=constr, turbodbc_options=options)
    return con


def parse_emails(url, max_domains=10, max_iter=999):

    if not url.startswith("http"):
        url = "http://" + url

    if not url.endswith("/"):
        url = url + "/"
    # a queue of urls to be crawled
    new_urls = deque([url])
    # a set of urls that we have already crawled
    processed_urls = set()
    emails = defaultdict(set)
    domains = set()
    # avoid infity loop
    # process urls one by one until we exhaust the queue
    i = 0
    while len(new_urls):

        if i > max_iter:
            break

        # move next url from the queue to the set of processed urls
        url = new_urls.popleft()
        processed_urls.add(url)

        # extract base url to resolve relative links
        parts = urlsplit(url)
        base_url = "{0.scheme}://{0.netloc}".format(parts)
        path = url[:url.rfind('/') + 1] if '/' in parts.path else url

        if len(domains) > max_domains and parts.hostname not in domains:
            continue

        domains.update({parts.hostname})

        # get url's content
        try:
            response = requests.get(url, timeout=180)
            try:
                if not "text/html" in response.headers["content-type"]:
                    #print("Page is not HTML")
                    continue
            except (KeyError):
                # print("Page is not HTML")
                continue
            #print("Processing %s" % url)
        except Exception:
            # ignore pages with errors
            continue

        # extract all email addresses and add them into the resulting set
        new_emails = set(re.findall(
            r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", response.text, re.I))
        if new_emails:
            emails[parts.hostname].update(new_emails)

        # create a beutiful soup for the html document
        soup = BeautifulSoup(response.text, "html.parser")
        #soup = BeautifulSoup(response.text, "lxml")

        # find and process all the anchors in the document
        for anchor in soup.find_all("a"):
            # extract link url from the anchor
            link = anchor.attrs["href"] if "href" in anchor.attrs else ''
            # resolve relative links
            if link.startswith('/'):
                link = base_url + link
            elif not link.startswith('http'):
                link = path + link
            # add the new url to the queue if it was not enqueued nor processed yet
            if not link in new_urls and not link in processed_urls:
                new_urls.append(link)
        i += 1
    return emails


def nested_dict_to_dataframe(d):
    df = pd.DataFrame()
    for key, val in d.items():
        for url, mails in val.items():
            dft = pd.DataFrame()
            dft['emails'] = [mail for mail in mails]
            dft['url'] = url
            dft['key'] = key
            df = pd.concat([df, dft])
    return df[['key', 'url', 'emails']].reset_index(drop=True)


def get_mails_muti(multi_d, i, key, url, number_of_rows, *args):
    multi_d[key] = parse_emails(url, *args)
    print("Thread {} of {} Threads {}=======> {}".format(
        i + 1, number_of_rows, "\t", url))


def get_mails(df, key_column, url_column, *args):
    df = df.copy().reset_index(drop=True)

    key_index = df.columns.get_loc(key_column)
    url_index = df.columns.get_loc(url_column)

    with Pool(4) as p:
        mgr = MyManager()
        mgr.start()
        multi_d = mgr.defaultdict(defaultdict)

        number_of_rows = df.shape[0]

        for i, d in df.iterrows():
            p.apply_async(get_mails_muti, (multi_d,
                                           i, d[key_index],
                                          d[url_index],
                                          number_of_rows,
                                          *args))
        p.close()
        p.join()
    return multi_d


def query():
    return """
        SELECT Kundennummer = [Nr.], Name, Homepage
        FROM BDL.DES.Debitor d
        WHERE Homepage > '' AND Name NOT LIKE 'ยง%'
        AND EXISTS ( SELECT Kunde
                      FROM BDL.DES.[Kundenstamm CRHT] k
                      WHERE k.Kunde = d.[Nr.]
                    )
         """


if __name__ == "__main__":

    print("\n\n\n==============================================\n\n",
          "Simple Mail Parser",
          "\n\n""==============================================\n\n\n")

    parser = argparse.ArgumentParser(description='Parameter')
    parser.add_argument('-niter', type=int, default=10, dest="max_iter",
                        help='Number of max iterations')
    parser.add_argument('-maxd', type=int, default=10, dest="max_domains",
                        help='Number of max subdomains')

    para = parser.parse_args()

    path_ = os.path.join("\\\\CRHBUSADCS01",
                         "Data",
                         "PublicCitrix",
                         "084_Bern_Laupenstrasse",
                         "CM",
                         "Analysen",
                         "Software",
                         "Simple_Mail_Parser")

    con = create_connection_turbo("CRHBUSADWH51", 'Operations')
    customer_df = sql_to_dataframe(con, query())
    con.close()

    for i, d in enumerate(batch_(customer_df, 50)):
        try:
            dict_of_emails = get_mails(d, 'Kundennummer', 'Homepage',
                                       para.max_domains, para.max_iter)

            df = nested_dict_to_dataframe(dict_of_emails)

            df = df.merge(d, left_on="key", right_on="Kundennummer")
            df['Emails'] = df['emails']
            df = df[['Kundennummer', 'Name', 'Homepage', 'emails']]

            print("Writing files of batch process {}...".format(i))
            df.to_csv(os.path.join(path_,
                                   os.path.join(
                                   "Output", "File-" + str(i) + ".csv")),
                      sep="\t", encoding='utf-8')
        except Exception as e:
            print("Error in Batch {}. Error code: {}".format(i, e))
            continue
