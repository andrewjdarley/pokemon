from requests import request
import json, re


def get_replay(url, output_path):
    """
    url: Pass in https://replay.pokemonshowdown.com/gen8doublesubers-1097585496.json or gen8doublesubers-1097585496.json
    """
    if not url.startswith("https://replay.pokemonshowdown.com/"):
        url = "https://replay.pokemonshowdown.com/" + url

    response = request("GET", url)

    if response.status_code == 200:
        data_dict = response.json()
        # filename = url.split("/")[-1]
        # with open(filename, 'w') as file:
        #     json.dump(data_dict, file, indent=4)
        # print("Successfully downloaded the JSON data")
    else:
        print(f"Failed to download. Status code: {response.status_code}")


    # Regular expressions to capture the relevant data from the 'log'
    gen_re = re.compile(r'\|gen\|(\d+)')
    tier_re = re.compile(r'\|tier\|([\w\s\[\]]+)')
    poke_re = re.compile(r'\|poke\|(\w+)\|([\w\s,\-*]+)')
    rule_re = re.compile(r'\|rule\|([\w\s,:\-]+)')
    winner_re = re.compile(r'\|win\|([\w\s\*]+)')

    # Storage for parsed data
    parsed_data = {
        'id': data_dict.get('id'),
        'format': data_dict.get('format'),
        'players': data_dict.get('players'),
        'gen': None,
        'tier': None,
        'poke': {
            'p1': [],
            'p2': []
        },
        'winner': None,
        'rules': [],
        'metadata': {},
        'full_log': ''
    }

    # Extract log content
    log_content = data_dict.get('log', '')
    parsed_data['full_log'] = log_content

    # Extract generation
    gen_match = gen_re.search(log_content)
    if gen_match:
        parsed_data['gen'] = int(gen_match.group(1))

    # Extract tier
    tier_match = tier_re.search(log_content)
    if tier_match:
        parsed_data['tier'] = tier_match.group(1).strip()

    # Extract pokemons for each player
    for poke_match in poke_re.finditer(log_content):
        player = poke_match.group(1)
        pokemon = poke_match.group(2).strip()
        if player == 'p1':
            parsed_data['poke']['p1'].append(pokemon)
        elif player == 'p2':
            parsed_data['poke']['p2'].append(pokemon)

    # Extract rules
    for rule_match in rule_re.finditer(log_content):
        rule = rule_match.group(1).strip()
        parsed_data['rules'].append(rule)


    # Extract winner
    winner_match = winner_re.search(log_content)
    if winner_match:
        parsed_data['winner'] = winner_match.group(1).strip()

    # Add other metadata fields
    parsed_data['metadata'] = {
        'views': data_dict.get('views'),
        'uploadtime': data_dict.get('uploadtime'),
        'rating': data_dict.get('rating'),
        'private': data_dict.get('private'),
        'formatid': data_dict.get('formatid')
    }

    # Write parsed data to a JSON file
    json_output_path = output_path
    with open(json_output_path, 'w', encoding='utf-8') as json_file:
        json.dump(parsed_data, json_file, indent=4)

    # print(f"Parsed data successfully written to {json_output_path}")

    return parsed_data
