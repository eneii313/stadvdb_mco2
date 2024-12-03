from locust import HttpUser, task, between, constant_pacing

class GameUser(HttpUser):
    host = "http://127.0.0.1:3000" 
    #wait_time = between(1, 3)  # Wait time between each task
    # 200 milliseconds
    wait_time = constant_pacing(0.2) 
    # 10.2.0.126
    @task
    def view_game(self):
        # Simulate accessing a game with a random AppID
        appid = 413150  
        self.client.get(f"/view_game/{appid}")


# To run the locust test, run the command: locust -f test_read.py
