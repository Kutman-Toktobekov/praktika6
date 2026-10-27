import threading

condition = threading.Condition()
items = 0


def consumer_wrong():
    global items

    with condition:
        print("Потребитель: проверяет буфер")
        if items == 0:
            print("Потребитель: ожидает уведомления")
            condition.wait()

        # Ошибка: после пробуждения условие не перепроверяется циклом.
        if items == 0:
            print("ОШИБКА: поток проснулся, но буфер пуст!")
        else:
            items -= 1
            print("Потребитель: забрал элемент")


def notifier():
    with condition:
        print("Другой поток: отправляет уведомление без элемента")
        condition.notify()


consumer = threading.Thread(target=consumer_wrong)
consumer.start()

# Дожидаемся, чтобы потребитель действительно начал ожидание.
with condition:
    # Освобождаем блокировку после того, как потребитель вызовет wait().
    pass

# Даём потребителю время перейти в wait().
import time
time.sleep(0.2)

notifier_thread = threading.Thread(target=notifier)
notifier_thread.start()

consumer.join()
notifier_thread.join()

print("Демонстрация завершена.")
