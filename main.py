import json

import psycopg2
import requests

import hidden


def _create_table(cur):
    cur.execute('CREATE TABLE IF NOT EXISTS pokeapi (id SERIAL PRIMARY KEY, body JSONB);')
    print("pokeapi table created")


def _clean_table(cur):
    cur.execute('DELETE FROM pokeapi;')
    print("pokeapi table cleaned")


def _insert_into_database(cur, json_data):
    # print("inserting ", json)
    json_as_string = json.dumps(json_data)
    cur.execute("INSERT INTO pokeapi (body) VALUES (%s);", (json_as_string,))


def _fetch_and_persist():
    response = requests.get(API_BASE_URL + "pokemon/" + str(n))
    if response.status_code == 200:
        _insert_into_database(cur, response.json())
        print(f"Inserted pokemon {n}")
    else:
        print("Error: Failed to retrieve data from API. HTTP status code: ", response.status_code)


def _fetch_pokemons(number_of_pokemons):
    for n in range(1, number_of_pokemons):
        try:
            _fetch_and_persist()
            if n % 25 == 0:
                conn.commit()
        except requests.exceptions.RequestException as e:
            print("Error: Request failed:", e)
        except KeyboardInterrupt:
            print('')
            print('Program interrupted by user...')
            break


# Load the secrets
secrets = hidden.secrets()
API_BASE_URL = "https://pokeapi.co/api/v2/"
conn = psycopg2.connect(host=secrets['host'],
                        port=secrets['port'],
                        database=secrets['database'],
                        user=secrets['user'],
                        password=secrets['pass'],
                        connect_timeout=3)

cur = conn.cursor()

_create_table(cur)
_clean_table(cur)
_fetch_pokemons(100)

print('Closing database connection...')
conn.commit()
cur.close()
