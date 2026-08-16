from abc import ABC, abstractmethod
from typing import Any


def _is_str_dict(data: Any) -> bool:
    return (isinstance(data, dict)
            and all(isinstance(key, str) for key in data)
            and all(isinstance(value, str) for value in data.values()))


class DataProcessor(ABC):
    def __init__(self, name: str = "Processor") -> None:
        self._storage: list[tuple[int, str]] = []
        self._counter = 0
        self._name = name

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        return self._storage.pop(0)

    def stats(self) -> str:
        return (f"{self._name}: total {self._counter} items processed, "
                f"remaining {len(self._storage)} on processor")

    def _store(self, item: str) -> None:
        self._storage.append((self._counter, item))
        self._counter += 1


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__("Numeric Processor")

    def validate(self, data: Any) -> bool:
        if type(data) is list:
            return all(type(item) is int or type(item) is float
                       for item in data)
        return type(data) is int or type(data) is float

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Invalid Data")
        items = data if isinstance(data, list) else [data]
        for item in items:
            self._store(str(item))


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__("Text Processor")

    def validate(self, data: Any) -> bool:
        if type(data) is list:
            return all(isinstance(item, str) for item in data)
        return isinstance(data, str)

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Wrong Type")
        items = data if isinstance(data, list) else [data]
        for item in items:
            self._store(item)


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__("Log Processor")

    def validate(self, data: Any) -> bool:
        if type(data) is list:
            return all(_is_str_dict(item) for item in data)
        return _is_str_dict(data)

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Invalid Data")
        items = data if isinstance(data, list) else [data]
        for item in items:
            self._store(str(item))


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    break
            else:
                print("DataStream error - Can't process element "
                      f"in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            print(proc.stats())


# --------------------------------------------------------------------
# Test scenario
# --------------------------------------------------------------------

def consume(proc: DataProcessor, count: int) -> None:
    for _ in range(count):
        try:
            proc.output()
        except IndexError:
            break


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    batch: list[Any] = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO',
          'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five'],
    ]

    print("\nRegistering Numeric Processor")
    stream.register_processor(numeric)
    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nRegistering other data processors")
    stream.register_processor(text)
    stream.register_processor(log)
    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nConsume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    consume(numeric, 3)
    consume(text, 2)
    consume(log, 1)
    stream.print_processors_stats()