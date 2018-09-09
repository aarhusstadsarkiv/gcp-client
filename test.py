from google.cloud import datastore


def create_client(project_id):
    return datastore.Client(project_id)


def list_suggestions(client):
    query = client.query(kind='AutoToken_v2')
    query.add_filter('prefixTokens', '=', 'Solvangsvej')
    query.order = ['display']

    return list(query.fetch())


def format_entities(entities):
    lines = []
    for entity in entities:
        lines.append('{}: {}'.format(
            entity['display'], entity['tokenID']))
    return '\n'.join(lines)


if __name__ == '__main__':
    client = create_client('aarhusiana')
    print(format_entities(list_suggestions(client)))
