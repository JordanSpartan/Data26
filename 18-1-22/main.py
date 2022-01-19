import pymongo
import requests


client = pymongo.MongoClient()
db = client['starwars']
db.starships.drop()
db.create_collection('starships')


def url_to_pilot_id(documents):
    for item in documents:
        if item['pilots']:
            pilot_lst = []
            for link in item['pilots']:
                pilot_id = get_pilot_id(link)
                pilot_lst.append(pilot_id)
            item['pilots'] = pilot_lst
            db.starships.insert_one(item)
        else:
            db.starships.insert_one(item)


def get_pilot_name(url):
    r = requests.get(url).json()
    return r['name']


def get_pilot_id(url):
    name = get_pilot_name(url)
    pilot_id = db.characters.find_one({"name": name}, {"_id": 1})
    return pilot_id.get('_id')


def extract_to_db():
    page = 0
    state = True
    while state:
        page += 1
        response = requests.get("https://swapi.dev/api/starships/?page="+str(page)).json()

        if response['next'] is not None:
            results = response['results']
            url_to_pilot_id(results)

        if response['next'] is None:
            results = response['results']
            url_to_pilot_id(results)
            state = False


extract_to_db()
