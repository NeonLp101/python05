from abc import ABC, abstractmethod
from typing import Any, Protocol


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
            raise ValueError("Improper numeric data")
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
            raise ValueError("Improper text data")
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
            raise ValueError("Improper log data")
        items = data if isinstance(data, list) else [data]
        for item in items:
            self._store(": ".join(item.values()))


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    @staticmethod
    def _escape(value: str) -> str:
        if any(char in value for char in ',"\n'):
            return '"' + value.replace('"', '""') + '"'
        return value

    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        print(",".join(self._escape(value) for _, value in data))


class JSONExportPlugin:
    @staticmethod
    def _escape(value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        escaped = escaped.replace("\n", "\\n").replace("\t", "\\t")
        return escaped

    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        entries = [f'"item_{rank}": "{self._escape(value)}"'
                   for rank, value in data]
        print("{" + ", ".join(entries) + "}")


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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            data: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    data.append(proc.output())
                except IndexError:
                    break
            if data:
                plugin.process_output(data)

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

if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")
    print("\nInitialize Data Stream...")
    stream = DataStream()
    print()
    stream.print_processors_stats()

    print("\nRegistering Processors")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())

    first_batch: list[Any] = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO',
          'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five'],
    ]
    print(f"\nSend first batch of data on stream: {first_batch}")
    stream.process_stream(first_batch)
    print()
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, CSVExportPlugin())
    print()
    stream.print_processors_stats()

    second_batch: list[Any] = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR',
          'log_message': '500 server crash'},
         {'log_level': 'NOTICE',
          'log_message': 'Certificate expires in 10 days'}],
        [32, 42, 64, 84, 128, 168],
        'World hello',
    ]
    print(f"\nSend another batch of data: {second_batch}")
    stream.process_stream(second_batch)
    print()
    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, JSONExportPlugin())
    print()
    stream.print_processors_stats()

    print("\nAn unhandled type is reported by the stream:")
    stream.process_stream([True, {'a': 1}])
