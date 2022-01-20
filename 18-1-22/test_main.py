import unittest
import main


class MyTestCase(unittest.TestCase):

    def test_name(self):
        self.assertEqual(main.get_pilot_name("https://swapi.dev/api/people/13/"), "Chewbacca")

    def test_pilot_id(self):
        self.assertEqual(str(main.get_pilot_id("https://swapi.dev/api/people/13/")), "61e58b229fe487deaa5f69b8")

    def test_upload_in_db(self):
        death_star = main.db.starships.find_one({"name": "Death Star"}, {"name": 1, "_id": 0})
        self.assertEqual("Death Star", death_star.get("name"))

    def test_num_pilots_in_db(self):
        x_wing = main.db.starships.find_one({"name": "X-wing"}, {"name": 1, "_id": 0, "pilots": 1})
        self.assertEqual(4, len(x_wing.get("pilots")))

    def test_pilots_in_db(self):
        x_wing = main.db.starships.find_one({"name": "X-wing"}, {"name": 1, "_id": 0, "pilots": 1})
        self.assertEqual("[ObjectId('61e58b361bcef3692b6653dc'), ObjectId('61e58b1fb35ad1f9b4e03166'), ObjectId('61e58b4ff3d24136fa3378bc'), ObjectId('61e58b31423057453901b37d')]", str(x_wing.get("pilots")))

    def test_document_count(self):
        total_documents = main.db.starships.count_documents({})
        self.assertEqual(36, total_documents)

if __name__ == '__main__':
    unittest.main()
