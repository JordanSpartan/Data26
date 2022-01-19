import pymongo
import requests


client = pymongo.MongoClient()
db = client['starwars']
db.starships.drop()
db.create_collection('starships')


# Takes in the results list from the json response
# loops from each document in list
# If the pilots list is not empty, for each pilot get the id and append to list
# pilots list = ObjectIds list instead of url, then insert to db collection
# If pilots list contain no pilots, just insert to db collection
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


# Takes a character url and returns the name of that character
def get_pilot_name(url):
    r = requests.get(url).json()
    return r['name']


# Gets the pilot name from url
# Matches that name to the name in db and gets the id
# returns the ObjectId
def get_pilot_id(url):
    name = get_pilot_name(url)
    pilot_id = db.characters.find_one({"name": name}, {"_id": 1})
    return pilot_id.get('_id')


# while true state, loop through starship pages
# if response has next page, do function
# if page does not have next page, do function and set state to false, ending while
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
