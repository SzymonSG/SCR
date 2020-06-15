# Ta biblioteka jest przeznaczona do wielowątkowości
import threading
import queue
#Pozwala rejestrować rzeczy
import logging
#Pomocne moduły
import time
import random
import string
import hashlib

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("main")
logger_p = logging.getLogger("Producent")
logger_c = logging.getLogger("Konsument")

RUNNING = True

#Tworzymy naszą skrzynkę podstawową
class SimBase(threading.Thread):

    def __init__(self, q, rate):
        self.running = True
        self.q = q
        self.rate = rate
        self.count = 0
        super(SimBase, self).__init__()

#Tworzymy skrzynke producenta
class SimProducer(SimBase):
    def __init__(self, q, rate):
        super(SimProducer, self).__init__(q, rate)

    def _generate_message(self):
        return str(random.randint(0, 1000))

    def run(self):
        while RUNNING:
            message = self._generate_message() 
            if not self.q.full():
                logger_p.info("{} Tworzę {}".format(threading.current_thread().name, message))
                self.q.put(message)
                logger_p.debug("{} Q = {}".format(threading.current_thread().name, self.q.queue))
                self.count += 1
            else:
                logger_p.warn("{} Kolejka Q pełna! Wypróżniam... {}".format(threading.current_thread().name, message))

            # Czas między zdarzeniami z rozkładu Poissona jest wykładniczy
            wait = random.expovariate(self.rate)
            logger_p.debug("{} Zamrażanie dla = {}".format(threading.current_thread().name, wait))
            time.sleep(wait)
        logger_p.debug("{} Gotowe!".format(threading.current_thread().name))


class DistributedPOW(SimBase):
    def __init__(self, q, rate, difficulty):
        self.difficulty = difficulty
        super(DistributedPOW, self).__init__(q, rate)

    def _pow(self, message):

        h = None
        nonce = 0
        while True:
            message = "{}{}".format(message, nonce )
            message = message.encode("utf-8")
            h = hashlib.sha256(message).hexdigest()
            nonce += 1
            if self.valid_prefix(h):
                break
        return (h, nonce)

    def _send(self, message, h, nonce):
        # Symulujemy oczekiwanie na zasób
        # Dodawanie wątków nie zwiększa przepustowości, ponieważ
        # wątki nie są podzielone na rdzenie. Jednak tutaj nie jesteśmy zablokowani
        # zużywamy dowolne zasoby, a zatem wykorzystujemy dowolny wątek obliczający klasę POW
        # ustawiamy dostęp do procesora
        wait = random.expovariate(self.rate)
        logger_c.debug("{} Zamrażanie dla = {}".format(threading.current_thread().name, wait))
        time.sleep(wait)

    def valid_prefix(self, hash):
        """Sprawdź, czy skrót jest poprzedzony odpowiednią liczbą zer"""
        return hash[:self.difficulty] == "".join(['0'] * self.difficulty)

    def run(self):
        while RUNNING:
            # Jeśli tutaj zablokujemy, możemy dojść do impasu
            try:
                message = self.q.get(block=False)
                logger_c.info("{} Wykorzystuję... {}".format(threading.current_thread().name, message))
                (h, nonce) = self._pow(message)
                self._send(message, h, nonce)
                logger_c.info("{} Tworzę POW ({}, {}, {})".format(threading.current_thread().name, message, nonce, h))
                self.count += 1

            # Według dokumentów, jeśli zwraca prawdę, to nie gwarantuje
            # get nie będzie próbował pobrać z pustej kolejki, więc po prostu
            # zignoruj ​​to, jeśli tak się stanie
            except queue.Empty:
                pass
        
        logger_c.debug("{} Gotowe!".format(threading.current_thread().name))



if __name__ == "__main__":
    random.seed(3)

    BUFF_SIZE = 10
    SIM_TIME = 10.0
    difficulty = 3
    q = queue.Queue(BUFF_SIZE)
    num_consumers = 10 
    consumers = []

    # Jeśli mamy naprawdę szybkiego producenta, kolejka się przepełni
    # abyśmy mogli zrobić jedną lub dwie rzeczy, musimy zwiększyć pracowników
    # lub wydłużyć czas przetwarzania
    producer_rate = 1#0.1
    consumer_rate = 1#3.0

    # inicjujemy producenta 
    producer = SimProducer(q, 1.0/producer_rate)
    for i in range(num_consumers):
        consumers.append(DistributedPOW(q, 1.0/consumer_rate, difficulty))

    # uruchamiamy producenta
    producer.start()
    for w in consumers:
        w.start()

    # uruchamiany procedure
    start_time = time.time()
    end_time = start_time + SIM_TIME
    while True:
        if time.time() > end_time:
            RUNNING = False
            break
        time.sleep(1)

    # czyszczenie i obliczanie tput
    # Możemy tutaj wykazać, że chociaż wątki mogą się nie zwiększać
    # paramilizacja w niektórych aplikacjach może zwiększyć przepustowość
    # Jeśli bawimy się liczbą klientów i stawką, możemy to pokazać
    producer.join()
    produce_count = producer.count
    consumer_count = 0
    for w in consumers:
        w.join()
        consumer_count += w.count

    logger.info("Tput P={} C={}".format(produce_count/SIM_TIME, consumer_count/SIM_TIME))
