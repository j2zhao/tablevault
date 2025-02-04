import time
from collections import defaultdict
from pprint import pprint

class BasicTimer:
    def __init__(self):
        self.start_time = time.p()

    def end_time(self, step):
        end_time = time.perf_counter()
        delta_time = end_time - self.start_time
        print(f"{step} Time Interval: {delta_time}")
        self.start_time = time.perf_counter()

class StepsTimer:
    def __init__(self):
        self.step_times = defaultdict(float)
        # For each step, we'll store start times in a dictionary keyed by an index.
        self.step_temps = defaultdict(dict)
        # This dictionary keeps track of the next available index for each step.
        self.next_index = defaultdict(int)
    
    def start_step(self, step: str) -> int:
        """
        Start timing for a given step.
        
        Returns an index that must be passed to stop_step to match this start.
        """
        idx = self.next_index[step]
        self.step_temps[step][idx] = time.perf_counter()
        self.next_index[step] += 1
        return idx

    def stop_step(self, step: str, index: int):
        """
        Stop timing for a given step using the index returned by start_step.
        Accumulates the elapsed time into step_times.
        """
        if step not in self.step_temps or index not in self.step_temps[step]:
            raise ValueError("Step not started or invalid index provided.")
        
        # Use the same high-precision timer
        start_time = self.step_temps[step].pop(index)
        end_time = time.perf_counter()
        self.step_times[step] += end_time - start_time

    def print_results(self):
        """
        Print the total elapsed times for all steps.
        Raises an error if any step has an ongoing (not stopped) timing.
        """
        # Check for any ongoing (unfinished) steps
        for step, start_dict in self.step_temps.items():
            if start_dict:  # if the dict is not empty, there are unfinished timings.
                raise RuntimeError(f"Step '{step}' has ongoing timing that hasn't been stopped.")
        
        # All steps are stopped; print the accumulated times.
        pprint(dict(self.step_times))

        
        