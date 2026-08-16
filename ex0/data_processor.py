from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[tuple[int, str]] = []
        self._counter = 0
        self._name = "Processor"

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        first_item = self._storage.pop(0)
        return first_item


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self._name = "Numeric Processor"
    def validate(self, data: Any) -> bool:
        if type(data) is list:
            if all(type(item) is int or type(item) is float for item in data):
                return True
            else:
                return False
        elif type(data) is int or type(data) is float:
            return True
        else:
            return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Invalid Data")
        if not isinstance(data, list):
            data = [data]
        for item in data:
            self._storage.append((self._counter, str(item)))
            self._counter += 1


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self._name = "Text Processor"
    def validate(self, data: Any) -> bool:
        if type(data) is list:
            if all(isinstance(item, str) for item in data):
                return True
            else:
                return False
        elif isinstance(data, str):
            return True
        else:
            return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Wrong Type")
        if not isinstance(data, list):
            data = [data]
        for item in data:
            self._storage.append((self._counter, item))
            self._counter += 1


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self._name = "Log Processor"
    def validate(self, data: Any) -> bool:
        def is_dict_valid(data: dict) -> bool:
            if (all(isinstance(key, str) for key in data.keys())
            and all(isinstance(values, str) for values in data.values())):
                return True
            else:
                return False

        if type(data) is list:
            if all(is_dict_valid(item) for item in data):
                return True
            else:
                return False
        elif type(data) is dict:
            if (is_dict_valid(data)):
                return True
            else:
                return False
        else:
            return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Invalid Data")
        if not isinstance(data, list):
            data = [data]
        for item in data:
            self._storage.append((self._counter, str(item)))
            self._counter += 1
    

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def section(title: str) -> None:
    print(f"\n=== {title} ===")


if __name__ == "__main__":
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    # --- validate: valid and invalid data per processor ---
    section("NumericProcessor.validate")
    print("5              ->", numeric.validate(5))
    print("3.14           ->", numeric.validate(3.14))
    print("[1, 2.5, 3]    ->", numeric.validate([1, 2.5, 3]))
    print("'hello'        ->", numeric.validate("hello"))
    print("[1, 'two']     ->", numeric.validate([1, "two"]))

    section("TextProcessor.validate")
    print("'hello'        ->", text.validate("hello"))
    print("['a', 'b']     ->", text.validate(["a", "b"]))
    print("5              ->", text.validate(5))
    print("['a', 1]       ->", text.validate(["a", 1]))

    section("LogProcessor.validate")
    print("{'a': 'b'}                 ->", log.validate({"a": "b"}))
    print("[{'a': 'b'}, {'c': 'd'}]   ->", log.validate([{"a": "b"}, {"c": "d"}]))
    print("{'a': 1}                   ->", log.validate({"a": 1}))
    print("'not a dict'               ->", log.validate("not a dict"))

    # --- ingest without prior validation on invalid data -> must raise ---
    section("ingest without validate on invalid data (expect exceptions)")
    try:
        numeric.ingest("this is not numeric")
    except ValueError as e:
        print("NumericProcessor raised:", e)

    try:
        text.ingest(123)
    except ValueError as e:
        print("TextProcessor raised:", e)

    try:
        log.ingest("not a log entry")
    except ValueError as e:
        print("LogProcessor raised:", e)

    # --- ingest valid data, then extract with output ---
    section("NumericProcessor ingest/output")
    numeric.ingest(42)
    numeric.ingest([1, 2.5, 3])
    while True:
        try:
            print(numeric.output())
        except IndexError:
            break

    section("TextProcessor ingest/output")
    text.ingest("hello")
    text.ingest(["foo", "bar"])
    while True:
        try:
            print(text.output())
        except IndexError:
            break

    section("LogProcessor ingest/output")
    log.ingest({"level": "error", "msg": "disk full"})
    log.ingest([{"level": "info", "msg": "startup"}, {"level": "warn", "msg": "low memory"}])
    while True:
        try:
            print(log.output())
        except IndexError:
            break