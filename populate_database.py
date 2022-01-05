import psycopg2
import json
from tqdm import tqdm


conn = psycopg2.connect(dbname='postgres', user='postgres', password='testpass', port=5435)
cur = conn.cursor()

ror = json.loads(open('../ror-data-7.1.json', 'r', encoding='utf-8').read())

for inst in tqdm(ror):
    names = [inst['name']]
    for alias in inst['aliases']:
        names.append(alias)
    for acronym in inst['acronyms']:
        names.append(acronym)
    for label in inst['labels']:
        names.append(label['label'])
    for name in names:
        cur.execute('insert into insts values (%s, %s, %s, %s)',
                    (name.split('(')[0].strip(), inst['addresses'][0]['city'], inst['country']['country_name'], inst['id']))
conn.commit()