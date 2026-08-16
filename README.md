# python05

42 Heilbronn — Python module 05: object-oriented design.

The same `DataProcessor` abstraction rebuilt three times, growing each round.
Concrete processors validate and ingest different data types behind one
interface.

| | Topic |
|---|---|
| `ex0` | `DataProcessor` ABC — abstract `validate` and `ingest`, concrete `output` |
| `ex1` | Adds `stats()` and a shared `_store()` helper; subclasses pass their name up through `super().__init__()` |
| `ex2` | Introduces `Protocol` alongside `ABC` — structural versus nominal typing |

## Running

```
python3 ex2/data_pipeline.py
```
