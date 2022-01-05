import psycopg2
import requests
import re
import string
from unidecode import unidecode


def get_ror_api(text):
    try:
        r = requests.get('http://localhost:9292/organizations', params={'affiliation': text}).json()
    except:
        return None
    if len(r['items']) > 0:
        return r['items'][0]['organization']['id'], r['items'][0]['chosen']


def normalize(text):
    text = text.replace('.', '')
    text = text.replace('USA', 'United States')
    text = text.replace('US', 'United States').replace('UK', 'United Kingdom')
    text = ''.join([char for char in unidecode(text.lower()) if char in string.ascii_lowercase])
    return text


def get_ror_postgres(text, confident=True):
    text = text.replace('California,', 'California')
    names = [name.strip() for name in re.findall('[^^,]*(?:univers|institut|college|school|hospital|hôpital|foundation|centre|center|council|laboratory|agency|academy|association|society|pharma|service|system|campus|clinic)[^,]*', text, flags=re.IGNORECASE)]
    text_normalized = normalize(text)
    if names:
        if confident:
            exact = names[-1].split('(')[0].strip()
            if exact not in exact_cache:
                cur.execute('select ror, city, country from insts where name = %s limit 1', (exact,))
                exact_cache[exact] = cur.fetchone()
            rows = exact_cache[exact]
            if rows:
                ror, city, country = rows
                if normalize(country) in text_normalized and normalize(city) in text_normalized:
                    return ror, True
                else:
                    return get_ror_postgres(text, confident=False)
        query_inst = ('&' if confident else '|').join(names[-1].split('(')[0].strip().replace('&', ' ').replace('-', ' ').replace("\\'", '').replace(')', '').replace('|', ' ').replace('!', '').replace('#', '').replace('@', '').replace(':', ' ').split())
        if query_inst not in inst_cache:
            cur.execute('with ranks as (select ror, city, country, ts_rank(name_vec, q, 1) as rank from insts, to_tsquery(%s) as q where name_vec @@ q order by rank desc) select * from ranks where rank = (select max(rank) from ranks)',
                        (query_inst,))
            inst_cache[query_inst] = cur.fetchall()
        for rows in inst_cache[query_inst]:
            if not rows:
                return get_ror_postgres(text, confident=False)
            ror, city, country, rank = rows
            country = country.replace('South Korea', 'Korea')
            if (confident and normalize(country) in text_normalized and normalize(city) in text_normalized) or (not confident and (normalize(country) in text_normalized or normalize(city) in text_normalized)):
                return ror, confident
    if confident:
        return get_ror_postgres(text, confident=False)


inst_cache = {}
exact_cache = {}
conn = psycopg2.connect(dbname='postgres', user='postgres', password='testpass', port=5435)
cur = conn.cursor()
from time import time
f = open('results.txt', 'w')
postgres_time = 0
api_time = 0
for line in open('test_set.tsv', 'r', encoding='utf-8'):
    ror, text = line.split('	')
    t = time()
    postgres = get_ror_postgres(text.strip())
    postgres_time += time() - t
    t = time()
    api = get_ror_api(text.strip())
    api_time += time() - t
    if ',' not in ror:
        f.write(' '.join((ror if ror else 'NA', (postgres[0] if (postgres) else 'NA'), (api[0]))) + '\n')
print(postgres_time, api_time)