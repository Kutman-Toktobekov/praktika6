import logging
import threading
import time

BUFFER_SIZE = 5
TOTAL_ITEMS = 1000
PRODUCERS = 3
CONSUMERS = 2

logging.basicConfig(
    filename="violations.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s",
)


class RingBuffer:
    """Кольцевой буфер фиксированного размера."""

    def __init__(self, capacity):
        self.data = [None] * capacity
        self.capacity = capacity
        self.head = 0
        self.tail = 0
        self.count = 0

    def put(self, item):
        if self.count >= self.capacity:
            logging.error("Переполнение буфера при добавлении %s", item)
            raise RuntimeError("Переполнение кольцевого буфера")

        self.data[self.tail] = item
        self.tail = (self.tail + 1) % self.capacity
        self.count += 1

    def get(self):
        if self.count <= 0:
            logging.error("Попытка извлечь элемент из пустого буфера")
            raise RuntimeError("Буфер пуст")

        item = self.data[self.head]
        self.data[self.head] = None
        self.head = (self.head + 1) % self.capacity
        self.count -= 1
        return item


def run_with_semaphores():
    """Вариант 1: два семафора и мьютекс."""
    buffer = RingBuffer(BUFFER_SIZE)
    empty = threading.Semaphore(BUFFER_SIZE)
    full = threading.Semaphore(0)
    mutex = threading.Lock()

    consumed = []
    consumed_lock = threading.Lock()
    next_item = 0
    item_lock = threading.Lock()
    errors = []

    def producer():
        nonlocal next_item

        while True:
            with item_lock:
                if next_item >= TOTAL_ITEMS:
                    return
                item = next_item
                next_item += 1

            empty.acquire()
            try:
                with mutex:
                    buffer.put(item)
            except Exception as exc:
                errors.append(str(exc))
                return
            finally:
                full.release()

    def consumer():
        while True:
            full.acquire()
            try:
                with mutex:
                    item = buffer.get()
            except Exception as exc:
                errors.append(str(exc))
                return
            finally:
                empty.release()

            if item is None:
                return

            with consumed_lock:
                consumed.append(item)

    producers = [
        threading.Thread(target=producer)
        for _ in range(PRODUCERS)
    ]
    consumers = [
        threading.Thread(target=consumer)
        for _ in range(CONSUMERS)
    ]

    for thread in consumers + producers:
        thread.start()

    for thread in producers:
        thread.join()

    # Маркеры завершения для потребителей.
    for _ in consumers:
        empty.acquire()
        with mutex:
            buffer.put(None)
        full.release()

    for thread in consumers:
        thread.join()

    return check_results("Семафоры", consumed, errors, buffer)


def run_with_condition():
    """Вариант 2: мьютекс и условная переменная."""
    buffer = RingBuffer(BUFFER_SIZE)
    condition = threading.Condition()
    consumed = []
    errors = []
    next_item = 0
    item_lock = threading.Lock()
    producers_done = False

    def producer():
        nonlocal next_item

        while True:
            with item_lock:
                if next_item >= TOTAL_ITEMS:
                    return
                item = next_item
                next_item += 1

            with condition:
                while buffer.count == buffer.capacity:
                    condition.wait()

                try:
                    buffer.put(item)
                except Exception as exc:
                    errors.append(str(exc))
                    return

                condition.notify_all()

    def consumer():
        while True:
            with condition:
                while buffer.count == 0 and not producers_done:
                    condition.wait()

                if buffer.count == 0 and producers_done:
                    return

                try:
                    item = buffer.get()
                except Exception as exc:
                    errors.append(str(exc))
                    return

                condition.notify_all()

            if item is not None:
                consumed.append(item)

    producers = [
        threading.Thread(target=producer)
        for _ in range(PRODUCERS)
    ]
    consumers = [
        threading.Thread(target=consumer)
        for _ in range(CONSUMERS)
    ]

    for thread in consumers + producers:
        thread.start()

    for thread in producers:
        thread.join()

    with condition:
        producers_done = True
        condition.notify_all()

    for thread in consumers:
        thread.join()

    return check_results("Condition", consumed, errors, buffer)


def check_results(name, consumed, errors, buffer):
    expected = set(range(TOTAL_ITEMS))
    actual = set(consumed)
    duplicates = len(consumed) - len(actual)
    missing = expected - actual
    unexpected = actual - expected

    ok = (
        len(consumed) == TOTAL_ITEMS
        and not duplicates
        and not missing
        and not unexpected
        and not errors
        and 0 <= buffer.count <= buffer.capacity
    )

    if not ok:
        message = (
            f"{name}: consumed={len(consumed)}, "
            f"duplicates={duplicates}, missing={len(missing)}, "
            f"unexpected={len(unexpected)}, errors={errors}, "
            f"buffer_count={buffer.count}"
        )
        logging.error(message)

    print(f"\n=== {name} ===")
    print("Потреблено элементов:", len(consumed))
    print("Дубликаты:", duplicates)
    print("Пропущено элементов:", len(missing))
    print("Неожиданные элементы:", len(unexpected))
    print("Ошибки:", errors)
    print("Остаток в буфере:", buffer.count)
    print("Результат:", "УСПЕХ" if ok else "ОШИБКА")

    return ok


if __name__ == "__main__":
    run_with_semaphores()
    run_with_condition()
