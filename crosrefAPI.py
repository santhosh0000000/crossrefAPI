import csv
import threading
import requests
import time
import psycopg2
import logging
from crossref_commons.retrieval import get_publication_as_json

logging.basicConfig(filename='API.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


def establish_database_connection():

    try:
        conn = psycopg2.connect(database="xxx",
                                host="xxx",
                                user="xxx",
                                password="xxxx",
                                port="5432")
        return conn

    except Exception as e:
        logging.error("Error connecting to the database: %s", str(e))
        raise


def get_citation_count(doi):

    try:

        response = requests.get(f'https://opencitations.net/index/api/v1/citation-count/{doi}')

        if response.status_code == 200:
            citation_count = response.json()[0]['count']
            return citation_count
        else:
            return 'N/A'

    except Exception as e:
        logging.error("An error occurred while fetching citation count: %s", str(e))
        return 'N/A'


def get_citation_dois(doi):
    url = f'https://opencitations.net/index/api/v1/citations/{doi}'

    try:

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            return [mod['cited'] for mod in data] if data else []

        else:
            logging.info("Failed to fetch citation DOIs. Status code: %s", response.status_code)
            return []

    except Exception as e:
        logging.error("An error occurred while fetching citation DOIs: %s", str(e))
        return []

def process_row(row, writer):
    doi = str(row[1])
    doi_details = get_publication_as_json(doi)

    if isinstance(doi_details, dict):

        author_names = [f"{author.get('given', '')} {author.get('family', '')}" for author in
                        doi_details.get('author', [])]
        author_list = ", ".join(author_names)
        writer.writerow([row[0], row[1], row[2], doi_details.get('type', 'N/A'),
                         doi_details.get('container-title', 'N/A'),
                         doi_details.get('title', 'N/A'), doi_details.get('volume', 'N/A'),
                         doi_details.get('page', 'N/A'),
                         doi_details.get('published-online', {}).get('date-parts', [['N/A']])[0][0], author_list,
                         doi_details.get('publisher', 'N/A'),
                         doi_details.get('published-online', {}).get('date-parts', [['N/A']])[0],
                         get_citation_dois(doi), get_citation_count(doi)])

    else:
        writer.writerow(list(row) + ["N/A"] * 11)


def process_rows_in_threads(rows, writer):
    threads = []

    try:

        for row in rows:
            thread = threading.Thread(target=process_row, args=(row, writer))
            thread.start()
            threads.append(thread)
            time.sleep(0.01)

        for thread in threads:
            thread.join()

    except Exception as e:
        logging.error("An error occurred while threading: %s", str(e))


def main():
    conn = establish_database_connection()
    sql = """SELECT id, doi, confy_id FROM eudl.content WHERE doi_resolves = true"""

    cursor = conn.cursor()

    cursor.execute(sql)
    rows = cursor.fetchall()

    if rows:
        logging.info("Data fetched successfully.")
    else:
        logging.error("Failed to fetch data.")

    with open("citations_data_with_details_out.csv", mode='w', newline='', encoding='utf-8') as output_file:

        writer = csv.writer(output_file)

        writer.writerow(["ID", "DOI", "confy_id", "DOI Type", "Journal Title",
                         "Article Title", "Volume", "First Page", "Year", "Authors", "Publisher",
                         "Publication Date",
                         "Citation DOI_Num", "Citation Count"])
        process_rows_in_threads(rows, writer)

    logging.info("Data saved to citations_data_with_details.csv")


if __name__ == "__main__":
    main()
