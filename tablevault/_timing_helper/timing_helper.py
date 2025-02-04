import time


class Timer:
    def __init__(self):
        self.start_time = time.perf_counter()

    def catch_time(self, step):
        self.end_time = time.perf_counter()
        delta_time = self.end_time - self.start_time
        print(f"{step} Time Interval: {delta_time}")
        self.start_time = time.perf_counter()
