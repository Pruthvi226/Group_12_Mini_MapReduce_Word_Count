import pytest

import mapreduce
from mapreduce import map_chunk, reduce_counts, run_mappers, shuffle_results, split_text, tokenize


@pytest.mark.parametrize("text,expected", [
    ("", {}), ("  \n\t", {}), ("Apple", {"apple": 1}),
    ("apple apple apple", {"apple": 3}),
    ("Apple APPLE apple", {"apple": 3}),
    ("Hello, world! Hello?", {"hello": 2, "world": 1}),
    ("one   two\n one\tthree", {"one": 2, "two": 1, "three": 1}),
    ("can't re-enter foo_bar", {"can": 1, "t": 1, "re": 1, "enter": 1, "foo": 1, "bar": 1}),
    ("Café CAFÉ 42...", {"café": 2, "42": 1}), ("! _ --", {}),
])
def test_mapper(text, expected):
    assert map_chunk(text) == expected


def test_split_exact():
    assert split_text("apple banana apple orange banana apple", 3) == [
        "apple banana", "apple orange", "banana apple"]
    assert split_text("a b c d e", 3) == ["a b", "c d", "e"]
    assert split_text("a b", 7) == ["a", "b"]
    assert split_text("", 3) == []


@pytest.mark.parametrize("count", [1, 2, 3, 10, 32])
def test_split_preserves_tokens(count):
    text = "One,  two-three\nFOUR\tfive !"
    chunks = split_text(text, count)
    assert " ".join(chunks).split() == text.split()
    assert max(map(lambda chunk: len(chunk.split()), chunks)) - min(
        map(lambda chunk: len(chunk.split()), chunks)) <= 1


@pytest.mark.parametrize("invalid", [0, -1, 1.5, True])
def test_invalid_chunk_count(invalid):
    with pytest.raises(ValueError):
        split_text("a b", invalid)


def test_shuffle_and_reduce():
    outputs = [{"apple": 1, "banana": 1}, {"apple": 1, "orange": 1},
               {"banana": 1, "apple": 1}]
    grouped = shuffle_results(outputs)
    assert grouped == {"apple": [1, 1, 1], "banana": [1, 1], "orange": [1]}
    assert reduce_counts(grouped) == {"apple": 3, "banana": 2, "orange": 1}
    assert shuffle_results([]) == {}
    assert reduce_counts({}) == {}


def test_real_parallel_matches_sequential():
    chunks = split_text("apple banana apple orange banana apple", 3)
    sequential = run_mappers(chunks)
    parallel = run_mappers(chunks, parallel=True, max_workers=2)
    # Require a real pool here; fallback must not make this test pass silently.
    assert parallel.mode == "parallel", parallel.fallback_reason
    assert parallel.workers == 2
    assert parallel.outputs == sequential.outputs
    assert reduce_counts(shuffle_results(parallel.outputs)) == {"apple": 3, "banana": 2, "orange": 1}
    assert parallel.seconds >= 0 and sequential.seconds >= 0


def test_fallback(monkeypatch):
    def unavailable(**kwargs):
        raise OSError("process creation disabled")
    monkeypatch.setattr(mapreduce, "ProcessPoolExecutor", unavailable)
    result = run_mappers(["apple apple", "banana"], parallel=True)
    assert result.mode == "sequential fallback"
    assert result.outputs == [{"apple": 2}, {"banana": 1}]
    assert "process creation disabled" in result.fallback_reason


@pytest.mark.parametrize("text", ["", "Hi HI! there.", "can't a-b 123 café\n!", "one"])
def test_total_word_count_consistency(text):
    outputs = run_mappers(split_text(text, 8)).outputs
    final = reduce_counts(shuffle_results(outputs))
    assert final == map_chunk(text)
    assert sum(final.values()) == len(tokenize(text))


def test_empty_parallel_and_invalid_workers():
    assert run_mappers([], parallel=True).outputs == []
    with pytest.raises(ValueError):
        run_mappers(["a"], parallel=True, max_workers=0)


def test_reduce_returns_alphabetical_output():
    grouped = {"orange": [1], "apple": [1, 1, 1], "banana": [1, 1]}
    assert list(reduce_counts(grouped).items()) == [
        ("apple", 3), ("banana", 2), ("orange", 1)]


def test_large_text_and_total_consistency():
    text = (" ".join(["Alpha", "beta", "ALPHA", "gamma"] * 250)
            + "\nDelta, delta! epsilon")
    chunks = split_text(text, 32)
    outputs = run_mappers(chunks).outputs
    final = reduce_counts(shuffle_results(outputs))
    assert len(chunks) == 32
    assert sum(final.values()) == len(tokenize(text))
    assert final == map_chunk(text)
