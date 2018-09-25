from pathlib import Path
from typing import List

from google.cloud import datastore


def create_client(cred_json: Path = None) -> List:
    config = str(cred_json) if cred_json else 'aarhusiana-aedd4578c625.json'
    return datastore.Client.from_service_account_json(config)


def list_suggestions(client):
    query = client.query(kind='AutoToken_v2')
    query.add_filter('prefixTokens', '=', 'solvang')
    # query.order = ['display']

    return list(query.fetch())


def format_entities(entities):
    lines = []
    if len(entities) == 0:
        return "no hits"
    for entity in entities:
        lines.append('{}: {}'.format(
            entity['display'], entity['tokenID']))
    return '\n'.join(lines)


if __name__ == '__main__':
    client = create_client()
    print(format_entities(list_suggestions(client)))
