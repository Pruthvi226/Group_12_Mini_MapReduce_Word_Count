"""Pure MapReduce stages and a local process-based execution helper.

Token policy: whitespace defines chunk boundaries; Unicode alphanumeric runs
define words. Punctuation (including apostrophes, hyphens, and underscores)
separates words. Numbers are counted. Case is normalized with str.lower().
"""

from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
import os
import re
from time import perf_counter


def tokenize(text: str) -> list[str]:
    """Return lowercase Unicode letters/digits, excluding underscores."""
    return re.findall(r"[^\W_]+", text.lower(), flags=re.UNICODE)


def split_text(text: str, num_chunks: int) -> list[str]:
    """Balance whitespace-delimited tokens without losing or cutting any token.

    Return only nonempty chunks, capped at the number of input tokens.
    Whitespace formatting is normalized; punctuation is preserved at this stage.
    """
    if isinstance(num_chunks, bool) or not isinstance(num_chunks, int) or num_chunks < 1:
        raise ValueError("Number of chunks must be a positive integer.")
    words = text.split()
    if not words:
        return []
    count = min(num_chunks, len(words))
    size, extra = divmod(len(words), count)
    chunks = []
    start = 0
    for index in range(count):
        end = start + size + (index < extra)
        chunks.append(" ".join(words[start:end]))
        start = end
    return chunks


def map_chunk(chunk: str) -> dict[str, int]:
    """Count normalized words in one chunk without mutating external state."""
    return dict(Counter(tokenize(chunk)))


def shuffle_results(map_outputs: list[dict[str, int]]) -> dict[str, list[int]]:
    """Collect each mapper's local count for each word, in mapper order."""
    grouped = defaultdict(list)
    for output in map_outputs:
        for word, count in output.items():
            grouped[word].append(count)
    return dict(sorted(grouped.items()))


def reduce_counts(grouped_data: dict[str, list[int]]) -> dict[str, int]:
    """Sum the intermediate counts and return words in alphabetical order."""
    return {word: sum(grouped_data[word]) for word in sorted(grouped_data)}


@dataclass
class MapRun:
    outputs: list[dict[str, int]]
    seconds: float
    mode: str
    workers: int
    fallback_reason: str | None = None


def run_mappers(chunks: list[str], parallel: bool = False,
                max_workers: int | None = None) -> MapRun:
    """Time mapping, including process startup/shutdown for parallel execution.

    If a process pool fails, re-run all chunks sequentially. The reported time
    includes the failed attempt; callers can distinguish fallback from parallel.
    """
    if max_workers is not None and (
        isinstance(max_workers, bool) or not isinstance(max_workers, int) or max_workers < 1
    ):
        raise ValueError("Worker count must be a positive integer.")
    start = perf_counter()
    if not parallel or not chunks:
        outputs = [map_chunk(chunk) for chunk in chunks]
        return MapRun(outputs, perf_counter() - start, "sequential", 1 if chunks else 0)
    workers = min(len(chunks), max_workers or min(os.cpu_count() or 1, 4))
    try:
        # Workers live in this importable module, never inside the Streamlit app.
        with ProcessPoolExecutor(max_workers=workers) as executor:
            outputs = list(executor.map(map_chunk, chunks))
        return MapRun(outputs, perf_counter() - start, "parallel", workers)
    except Exception as error:
        outputs = [map_chunk(chunk) for chunk in chunks]
        return MapRun(outputs, perf_counter() - start, "sequential fallback", 1,
                      f"{type(error).__name__}: {error}")
