import threading
import time
import random


class ConditionBarrier:
    def __init__(self, parties):
        self.parties = parties
        self.arrived = 0
        self.generation = 0
        self.condition = threading.Condition()

    def wait(self):
        with self.condition:
            generation = self.generation
            self.arrived += 1

            if self.arrived == self.parties:
                self.arrived = 0
                self.generation += 1
                self.condition.notify_all()
            else:
                while generation == self.generation:
                    self.condition.wait()


barrier = ConditionBarrier(4)


def worker(worker_id):
    for phase in range(1, 4):
        print(f"Поток {worker_id}: начал фазу {phase}")
        time.sleep(random.uniform(0.1, 0.5))
        print(f"Поток {worker_id}: закончил фазу {phase}")

        barrier.wait()

        print(f"Поток {worker_id}: перешёл к фазе {phase + 1}"
              if phase < 3 else
              f"Поток {worker_id}: все фазы завершены")


threads = [
    threading.Thread(target=worker, args=(i,))
    for i in range(1, 5)
]

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

print("Барьер: все потоки завершили три фазы.")
