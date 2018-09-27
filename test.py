from pathlib import Path
from typing import List, Dict
import csv

from google.cloud import datastore

def _chunk(l, n):
    """Yield successive n-sized chunks from l."""
    return [l[i:i + n] for i in range(0, len(l), n)]
    # for i in range(0, len(l), n):
    #     yield l[i:i + n]


def create_client(cred_json: Path = None) -> List:
    config = str(cred_json) if cred_json else 'aarhusiana-aedd4578c625.json'
    return datastore.Client.from_service_account_json(config)


def list_suggestions(client):
    query = client.query(kind='AutoToken_v2')
    query.add_filter('prefixTokens', '=', 'solvang')
    return list(query.fetch())


def format_entities(entities):
    lines = []
    if len(entities) == 0:
        return "no hits"
    for entity in entities:
        lines.append('{}: {}'.format(
            entity['display'], entity['tokenID']))
    return '\n'.join(lines)


def delete_entries(table: str, delete_file: Path):
    return {}


def update_entries(table: str, update_file: Path):
    """table is the kind in the aarhusiana-datastore

    update_file must be a csv or json-file and only include the updates
    to be applied, not the full entries. The syntax is as follows:

    {
        "entry_identifier": {
            "key_to_update": "new total value",
            "key_to_update": "new total value"
        },
        "entry_identifier": {
            "key_to_update": "new total value",
            "key_to_update": "new total value"
        }
    }
    """

    # if update_file.is_file():
    #     if update_file.suffix == 'csv':
    #     elif update_file.suffix == 'json':
    #     else:
    #         return {'errors': {'error': 'Update_file must be csv or json.'}}
    return True


def update_autosuggest(client, update_file: Path):
    """Update_file must be in csv-format and include these columns:
    'tokenID' (entityID)
    'tokenDomain' (entityType)

    Furthermore include a column for each property you want to update.
    Some properties
    """
    modified_entries = []
    with open(update_file) as ifile:
        for row in csv.reader(ifile):
            query = client.query(kind='AutoToken_v2')
            query.add_filter('tokenID', '=', int(row[0]))
            query.add_filter('tokenDomain', '=', row[1])

            result = list(query.fetch(limit=1))
            print("results: " + str(len(result)))
            for entity in result:
                if 2 in entity.get('autoGroup'):
                    continue

                autoGroup = set(entity.get('autoGroup') + [2])
                # print(autoGroup)
                entity.update({
                    'autoGroup': list(autoGroup)
                })
                print('updated entry: ' + row[0] + ' ' + entity.get('display'))
                print('subdisplay: ' + entity.get('subDisplay'))
                # print(sorted(entity.items()))
                try:
                    client.put(entity)
                except Exception as e:
                    print(e)
                print('uploaded entry: ' + row[0])
                modified_entries.append(entity)

    for chunk in _chunk(modified_entries, 50):
        try:
            client.put_multi(chunk)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    client = create_client()
    print("client created")
    update_autosuggest(client, 'csv-files/20180927_aarhusteater______people_from_ext_data.csv')
    print('finished')
    # print(format_entities(list_suggestions(client)))
