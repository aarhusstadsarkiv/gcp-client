from pathlib import Path
from typing import List, Dict
import csv

from google.cloud import datastore

def _chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


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


def update_autosuggest(client, update_file):
    """Update_file must be in csv-format and include these columns:
    'tokenID' (entityID)
    'tokenDomain' (entityType)

    Furthermore include a column for each property you want to update.
    Some properties
    """
    modified_entities = []
    with open(update_file) as ifile:
        for row in csv.reader(ifile[1:]):
            query = client.query(kind='AutoToken_v2')
            query.add_filter('tokenID', '=', row[0])
            query.add_filter('tokenDomain', '=', row[1])
            
            for entity in list(query.fetch(1)):
                entity.update({
                    'autoGroup': set(entity.autoGroup).union({2}) 
                })
                modified_entities.append(entity)

    for chunk in list(_chunks(modified_entities, 50)):
        client.put_multi(chunk)


if __name__ == '__main__':
    client = create_client()
    print(format_entities(list_suggestions(client)))
