# This is the main class that defines player attributes and functionalities

class Player:
    def __init__(self, skill_film, popularity, budget):
        self.skill_film = skill_film
        self.popularity = popularity
        self.budget = budget

    def produce_film(self):
        self.skill_film += 1
        self.budget -= 1000

    def live_stream(self):
        self.popularity += 1
        self.budget += 500