"""Generate Searchlight's parity fixtures from scrape-bot's Python, the source of truth.

Run from the repository root (``make parity``)::

    uv run --project E:/code/scrape_bot python tools/parity/gen.py

It exports ``src/`` and ``tests/support/`` of a scrape-bot ref (``SCRAPE_BOT_REF``, default
``origin/main``) from the scrape-bot repository (``SCRAPE_BOT``, default
``E:/code/scrape_bot``) into a temporary directory with ``git archive``, imports
``scrape_bot.conditions``, ``scrape_bot.search_index`` and ``tests.support.search_oracle``
from there (never from the checkout, whose branch may be anything), and writes:

* ``testdata/parity/normalize.json``: ``normalize`` and ``clean_text`` of edge and random
  strings, plus every code point's ``casefold`` and Python's whitespace set;
* ``testdata/parity/words.json``: ``words_of``, plus Python's ``\\w`` set and NFKC per code
  point;
* ``testdata/parity/entries.json``: ``list_entries`` and the index's entry terms;
* ``testdata/parity/numbers.json``: ``number`` and ``as_text`` of JSON literals and of
  Python values JSON cannot spell (NaN, infinities);
* ``testdata/parity/similarity.json``: pg_trgm trigrams and ``trigram_similarity``, plus
  every code point's ``lower``;
* ``testdata/parity/match.json``: random documents and queries over every op of the
  Searchlight spec section 4, with the oracle's verdicts and scrape-bot's index documents;
* ``internal/analysis/tables.go``: the Unicode tables the Go analysis reads (Python's
  whitespace, ``\\w``, ``casefold``, ``lower`` and the Final_Sigma context classes), so Go
  folds exactly as this Python does, whatever Unicode version Go's own tables carry.

Everything is seeded, so a run over the same scrape-bot commit writes the same bytes.
"""

from __future__ import annotations

import io
import json
import math
import os
import random
import re
import struct
import subprocess
import sys
import tarfile
import tempfile
import unicodedata
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any

sys.dont_write_bytecode = True

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "testdata" / "parity"
TABLES = ROOT / "internal" / "analysis" / "tables.go"
SCRAPE_BOT = Path(os.environ.get("SCRAPE_BOT", "E:/code/scrape_bot"))
REF = os.environ.get("SCRAPE_BOT_REF", "origin/main")
SEED = 20261002
MAX_RUNE = 0x110000


def _is_surrogate(code: int) -> bool:
    return 0xD800 <= code <= 0xDFFF


def _code_points() -> Iterator[int]:
    return (code for code in range(MAX_RUNE) if not _is_surrogate(code))


# --------------------------------------------------------------------------- import


def _export(tree: Path) -> str:
    """Export the scrape-bot ref's sources into ``tree``; returns the commit."""
    commit = subprocess.run(
        ["git", "-C", str(SCRAPE_BOT), "rev-parse", f"{REF}^{{commit}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    archive = subprocess.run(
        ["git", "-C", str(SCRAPE_BOT), "archive", "--format=tar", commit, "src", "tests/support"],
        check=True,
        capture_output=True,
    ).stdout
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(tree, filter="data")
    return commit


# --------------------------------------------------------------------------- inputs

EDGE_STRINGS: list[str] = [
    "",
    " ",
    "\x00",
    "a\x00b",
    "\x00\x00 \x00",
    "ß",
    "Straße",
    "STRASSE",
    "strasse",
    "ẞ",
    "İ",
    "İstanbul",
    "ISTANBUL",
    "ı",
    "ﬁ",
    "ﬀ",
    "ﬃ",
    "ﬄ",
    "ﬅ",
    "ﬆ",
    "ﬓ",
    "Ǆ",
    "ǅ",
    "ǆ",
    "ŉ",
    "ǰ",
    "ΐ",
    "ᾈ",
    "ᾳ",
    "\u0345",
    "Ꭰ",
    "ꭰ",
    "ᏸ",
    "ΟΔΟΣ",
    "ΣΑΣ",
    "Σ",
    "aΣ",
    "aΣ.",
    "a.Σ",
    "aΣ'b",
    "ὈΔΥΣΣΕΎΣ",
    "x" * 500,
    ("Straße İstanbul ﬁsh " * 30)[:500],
    "é" * 500,
    "\u3000".join(["日本"] * 166)[:500],
    "😀" * 500,
    "a\u0301",
    "e\u0301\u0302",
    "\u0301",
    "\u0301abc",
    "a\u0332b",
    "Ａｂｃ１２３",
    "①②③",
    "²³",
    "½",
    "Ⅻ",
    "㎏",
    "㍿",
    "…",
    "ℌ",
    "\u2126",
    "\u212a",
    "\u212b",
    "\U0001d400\U0001d401",
    "日本語のテキスト",
    "中文 字符",
    "한국어",
    "\u1100\u1161\u11a8",
    "😀",
    "👍🏽",
    "👨\u200d👩\u200d👧",
    "🇨🇦",
    "❤\ufe0f",
    "a\u200db",
    "a\u200bb",
    "\ufeffbom",
    "a\u00adb",
    "a\tb\nc\rd\x0be\x0cf",
    "a\x1cb\x1dc\x1ed\x1ff",
    "a\x85b",
    "a\xa0b",
    "a\u1680b",
    "a\u2000b\u200ac",
    "a\u2028b\u2029c",
    "a\u202fb\u205fc\u3000d",
    "a\u180eb",
    "a\u200bb\u2060c",
    "  leading and trailing  ",
    "multiple   spaces\t\ttabs",
    "\t\n\x0b\x0c\r\x1c\x1d\x1e\x1f \x85\xa0",
    "\ufffd",
    "a\ufffdb",
    "under_score",
    "_",
    "__a__",
    "a_b-c",
    "hyphen-ated words",
    "can't won't",
    "it\u2019s",
    "e-mail@example.com",
    "https://Example.com/Path?Q=1",
    "C++ / C#",
    "$12.99",
    "!!",
    "...",
    "12",
    "12.50",
    "-0",
    "1e400",
    "NaN",
    "inf",
    "true",
    "None",
    "\U0002ebf0\U0002ebf1",
    "\U0002ee5d",
    "\U00020000",
    "\u2ffc\u31ef",
    "क्षि",
    "ภาษาไทย",
    "שָׁלוֹם",
    "مرحبا",
    "Ǳ ǲ ǳ",
    "ǈ",
    "ϐ ϑ ϕ ϖ ϰ ϱ ϵ",
    "ſ",
    "K",
    "ᲀ",
    "ﬗ",
]

POOL_ASCII = [chr(c) for c in range(0x20, 0x7F)]
POOL_SPACE = [chr(c) for c in _code_points() if chr(c).isspace()]
POOL_LATIN = [chr(c) for c in range(0xC0, 0x250)]
POOL_MARKS = [chr(c) for c in range(0x300, 0x370)] + ["\u0345", "\u20d0", "\u0332"]
POOL_GREEK = [chr(c) for c in range(0x370, 0x400) if unicodedata.category(chr(c)) != "Cn"]
POOL_CHEROKEE = [chr(c) for c in range(0x13A0, 0x13FE)] + [chr(c) for c in range(0xAB70, 0xABC0)]
POOL_SPECIAL = list("ßẞİıŉǰﬀﬁﬂﬃﬄﬅﬆﬓﬔﬕﬖﬗΐΰᾀᾈᾳᾼῌῼſK\u2126\u212bΣσς")
POOL_CJK = [chr(c) for c in range(0x4E00, 0x4E40)] + [chr(c) for c in range(0x2EBF0, 0x2EC10)]
POOL_HANGUL = [chr(c) for c in range(0xAC00, 0xAC20)] + [chr(c) for c in range(0x1100, 0x1120)]
POOL_COMPAT = (
    [chr(c) for c in range(0xFF01, 0xFF5F)]
    + list("²³¹½¼¾ⅠⅡⅢⅫⅰ①②㎏㍿ℌℍ…™℃㈱")
    + [chr(c) for c in range(0x1D400, 0x1D410)]
)
POOL_EMOJI = list("😀👍🏽❤🇨🇦👨👩👧") + ["\u200d", "\ufe0f"]
POOL_FORMAT = ["\u200b", "\u200c", "\u200d", "\ufeff", "\u00ad", "\u200e", "\u2060"]
POOL_MISC = ["\x00", "\ufffd", "_", "'", "\u2019", ".", ":", ",", ", ", "-", "/"]
POOLS: list[tuple[int, list[str]]] = [
    (30, POOL_ASCII),
    (10, POOL_SPACE),
    (6, POOL_LATIN),
    (5, POOL_MARKS),
    (5, POOL_GREEK),
    (3, POOL_CHEROKEE),
    (5, POOL_SPECIAL),
    (5, POOL_CJK),
    (3, POOL_HANGUL),
    (5, POOL_COMPAT),
    (3, POOL_EMOJI),
    (3, POOL_FORMAT),
    (6, POOL_MISC),
]


def _random_string(rng: random.Random, longest: int = 40) -> str:
    weights = [weight for weight, _ in POOLS]
    pools = [pool for _, pool in POOLS]
    length = rng.randint(0, longest)
    return "".join(rng.choice(rng.choices(pools, weights)[0]) for _ in range(length))


def _random_strings(rng: random.Random, count: int) -> list[str]:
    return [_random_string(rng) for _ in range(count)]


# ------------------------------------------------------------------------- fixtures


def _ranges(codes: Iterable[int]) -> list[list[int]]:
    """Sorted code points as inclusive ``[lo, hi]`` runs."""
    runs: list[list[int]] = []
    for code in codes:
        if runs and runs[-1][1] == code - 1:
            runs[-1][1] = code
        else:
            runs.append([code, code])
    return runs


def _normalize_fixture(conditions: Any, search_index: Any, rng: random.Random) -> dict[str, Any]:
    inputs = EDGE_STRINGS + _random_strings(rng, 600)
    cases = [
        {"in": text, "normalize": conditions.normalize(text), "clean": search_index.clean_text(text)}
        for text in inputs
    ]
    casefold = {
        str(code): chr(code).casefold() for code in _code_points() if chr(code).casefold() != chr(code)
    }
    whitespace = [code for code in _code_points() if chr(code).isspace()]
    return {"cases": cases, "casefold": casefold, "whitespace": whitespace}


_WORD = re.compile(r"\w")


def _words_fixture(conditions: Any, rng: random.Random) -> dict[str, Any]:
    inputs = EDGE_STRINGS + _random_strings(rng, 600)
    cases = [{"in": text, "words": conditions.words_of(text)} for text in inputs]
    word = _ranges(code for code in _code_points() if _WORD.match(chr(code)))
    nfkc = {
        str(code): unicodedata.normalize("NFKC", chr(code))
        for code in _code_points()
        if unicodedata.normalize("NFKC", chr(code)) != chr(code)
    }
    return {"cases": cases, "word": word, "nfkc": nfkc}


ENTRY_STRINGS: list[str] = [
    "",
    ", ",
    " , ",
    ",",
    "a",
    "a, b, c",
    "a,b",
    "a , b",
    "a,  b",
    "a, , b",
    ", a, ",
    "a, b, a",
    "A, a, A ",
    "  Leading, trailing  ",
    "x\x00, y",
    "\x00, \x00",
    "Straße, STRASSE, strasse",
    "İ, i\u0307, I",
    "ﬁ, fi, FI",
    "a,\tb",
    "a,\nb, c",
    "a, \u3000, b",
    "a\u3000, b\xa0",
    "a, \x1c, b",
    "日本, 中国, 한국",
    "😀, 👍🏽, 😀",
    "S, ",
    "one",
    "  ",
    ", , , ",
    "a,, b",
    "a, b,",
    "Ꭰ, ꭰ",
    ", ".join(["x" * 100] * 5),
    ", ".join(f"Size {n}" for n in range(1, 60)),
    "red, Red , RED,  red",
    "10, 10.0, 1e1",
]


def _entries_fixture(conditions: Any, search_index: Any, rng: random.Random) -> list[Any]:
    inputs = list(ENTRY_STRINGS)
    for _ in range(400):
        parts = [_random_string(rng, 8) for _ in range(rng.randint(0, 6))]
        seps = [", ", ",", " , ", ",  ", ", , "]
        text = ""
        for index, part in enumerate(parts):
            text += part if index == 0 else rng.choice(seps) + part
        inputs.append(text)
    return [
        {
            "in": text,
            "entries": conditions.list_entries(text),
            "terms": sorted(
                {
                    search_index.clean_text(conditions.normalize(entry))
                    for entry in conditions.list_entries(text)
                }
            ),
        }
        for text in inputs
    ]


NUMBER_LITERALS: list[str] = [
    "0",
    "-0",
    "1",
    "-1",
    "12",
    "12.5",
    "12.50",
    "1.0",
    "-0.0",
    "0.0",
    "1E2",
    "1e2",
    "1e+2",
    "1E+16",
    "1e-2",
    "0.1",
    "0.0001",
    "0.00001",
    "0.000123",
    "1e15",
    "1e16",
    "1e17",
    "123456789012345.6",
    "1234567890123456",
    "1234567890123456.0",
    "12345678901234567",
    "12345678901234567.0",
    "9999999999999998",
    "1e22",
    "1e23",
    "1e-7",
    "123e-20",
    "4.35",
    "0.30000000000000004",
    "3.141592653589793",
    "1.5e300",
    "1e308",
    "1.7976931348623157e308",
    "1.7976931348623158e308",
    "1.7976931348623159e308",
    "1e400",
    "-1e400",
    "1e-400",
    "-1e-400",
    "5e-324",
    "2.5e-324",
    "2.4703282292062328e-324",
    "2.2250738585072014e-308",
    "9007199254740993",
    "9007199254740993.0",
    "123456789012345678901234567890",
    str(2**53),
    str(2**53 + 1),
    str(2**63 - 1),
    str(2**63),
    str(-(2**63)),
    str(-(2**63) - 1),
    str(2**64),
    str(2**64 + 1),
    str(10**400),
    str(-(10**400)),
    str(2**1024 - 2**970 - 1),
    str(2**1024 - 2**970),
    str(2**1024),
    "true",
    "false",
    "null",
    '"12"',
    '""',
    '"1e400"',
    '"NaN"',
    '" 12 "',
    "[]",
    "[1]",
    "{}",
    '{"a": 1}',
]


def _bits(value: float) -> str:
    return struct.pack(">d", value).hex()


def _number_case(conditions: Any, value: Any) -> dict[str, Any]:
    found = conditions.number(value)
    return {
        "number": found,
        "bits": None if found is None else _bits(found),
        "text": conditions.as_text(value),
    }


def _numbers_fixture(conditions: Any, rng: random.Random) -> list[Any]:
    literals = list(NUMBER_LITERALS)
    for _ in range(300):
        value = struct.unpack(">d", rng.getrandbits(64).to_bytes(8, "big"))[0]
        if math.isfinite(value):
            literals.append(repr(value))
    for _ in range(100):
        literals.append(str(rng.randint(-(10**30), 10**30) // (10 ** rng.randint(0, 29))))
    for _ in range(100):
        mantissa = rng.randint(0, 10**rng.randint(1, 20))
        literals.append(f"{mantissa}e{rng.randint(-330, 330)}")
        literals.append(f"{mantissa}.{rng.randint(0, 999)}E{rng.choice(['+', '-', ''])}{rng.randint(0, 30)}")
    cases: list[dict[str, Any]] = []
    for literal in literals:
        cases.append({"json": literal, **_number_case(conditions, json.loads(literal))})
    for name, value in (("nan", math.nan), ("inf", math.inf), ("-inf", -math.inf)):
        cases.append({"float": name, **_number_case(conditions, value)})
    return cases


def _similarity_fixture(conditions: Any, rng: random.Random) -> dict[str, Any]:
    texts = EDGE_STRINGS + _random_strings(rng, 300)
    vocabulary = [
        "word",
        "words",
        "sword",
        "Word",
        "wordy",
        "hello world",
        "Hello, World!",
        "world hello",
        "Straße",
        "strasse",
        "İstanbul",
        "istanbul",
        "ΟΔΟΣ",
        "οδος",
        "ab",
        "a",
        "abc",
        "abd",
        "under_score",
        "under score",
        "nike air max 90",
        "Nike Air Max 270",
        "air jordan 1 retro",
        "jordan",
        "日本語",
        "日本",
        "12345",
        "1234",
    ]
    trigrams = [{"in": text, "trigrams": sorted(conditions._trigrams(text))} for text in texts]
    pairs: list[dict[str, Any]] = []
    for left in vocabulary:
        for right in vocabulary:
            pairs.append({"a": left, "b": right})
    for _ in range(600):
        pairs.append({"a": rng.choice(texts), "b": rng.choice(texts)})
    for _ in range(200):
        left = rng.choice(texts)
        pairs.append({"a": conditions.normalize(left), "b": conditions.normalize(rng.choice(texts))})
    for pair in pairs:
        pair["similarity"] = conditions.trigram_similarity(pair["a"], pair["b"])
    lower = {str(code): chr(code).lower() for code in _code_points() if chr(code).lower() != chr(code)}
    return {"trigrams": trigrams, "pairs": pairs, "lower": lower}


# ---------------------------------------------------------------------------- match

MAPPING: dict[str, str] = {
    "name": "text",
    "note": "text",
    "brand": "keyword",
    "color": "keyword",
    "tags": "keyword_list",
    "sizes": "keyword_list",
    "price": "number",
    "qty": "number",
    "active": "bool",
    "seen": "date",
}
KIND: dict[str, str] = {
    "text": "text",
    "keyword": "text",
    "keyword_list": "list",
    "number": "number",
    "date": "number",
    "bool": "bool",
}
OPS: dict[str, list[str]] = {
    "keyword": [
        "eq",
        "ne",
        "in",
        "exists",
        "contains",
        "contains_any",
        "contains_all",
        "starts_with",
        "similar",
    ],
    "keyword_list": ["has", "has_any", "has_all", "empty", "nonempty", "exists"],
    "number": ["eq", "ne", "in", "lt", "lte", "gt", "gte", "between", "exists"],
    "date": ["lt", "lte", "gt", "gte", "between", "exists"],
    "bool": ["eq", "ne", "exists"],
}
OPS["text"] = [*OPS["keyword"], "words_all", "words_any"]

WORDS = [
    "Nike",
    "nike",
    "NIKE",
    "Air",
    "Max",
    "air max",
    "Jordan",
    "Retro",
    "Straße",
    "STRASSE",
    "strasse",
    "İstanbul",
    "istanbul",
    "ﬁsh",
    "fish",
    "FISH",
    "Ｆｕｌｌ",
    "full",
    "naïve",
    "nai\u0308ve",
    "café",
    "CAFÉ",
    "日本",
    "日本語",
    "😀",
    "a_b",
    "x\x00y",
    "ΟΔΟΣ",
    "οδος",
    "Ꭰ",
    "ꭰ",
    "red",
    "Red",
    "blue",
    "Blue Moon",
    "10",
    "10.5",
    "½",
    "Ⅻ",
    "can't",
    "e-mail",
    "C++",
    "XL",
    "xl",
    "M",
    "\U0002ebf0",
]
SEPARATORS = [" ", " ", " ", "  ", "\t", "\n", "\xa0", "\u3000", "\x1c", ", ", "-", "/", "!! ", "_"]
NUMBER_POOL = [
    "0",
    "-0",
    "1",
    "1.0",
    "2",
    "2.5",
    "10",
    "10.0",
    "1E1",
    "1e1",
    "99.99",
    "100",
    "1e2",
    "-5",
    "-5.5",
    "0.1",
    "0.30000000000000004",
    "1e16",
    "10000000000000000",
    "10000000000000001",
    "100000000000000000000",
    "1e20",
    "1e400",
    "-1e400",
    "1" + "0" * 400,
    "5e-324",
]
NUMBER_QUERY_POOL = [
    0,
    1,
    1.0,
    2,
    2.5,
    10,
    10.0,
    99.99,
    100,
    -5,
    -5.5,
    0.1,
    0.30000000000000004,
    1e16,
    10000000000000000,
    10000000000000001,
    100000000000000000000,
    1e20,
    5e-324,
    0.0,
    -0.0,
    50,
    3,
]
OTHER_VALUES = ["null", "[]", "[1, 2]", '["a", "b"]', "{}", '{"v": 1}']


def _text_value(rng: random.Random) -> str:
    roll = rng.random()
    if roll < 0.1:
        return rng.choice(EDGE_STRINGS)
    if roll < 0.15:
        return _random_string(rng)
    if roll < 0.18:
        return (" ".join(rng.choice(WORDS) for _ in range(120)))[:500]
    parts = [rng.choice(WORDS) for _ in range(rng.randint(1, 5))]
    text = parts[0]
    for part in parts[1:]:
        text += rng.choice(SEPARATORS) + part
    if rng.random() < 0.2:
        text = rng.choice(["  ", "\t", " "]) + text + rng.choice(["  ", "\n", ""])
    return text


def _list_value(rng: random.Random) -> str:
    entries = [rng.choice(WORDS + ["S", "M", "L", "XL", "10", "10.5", " ", ""]) for _ in range(rng.randint(0, 5))]
    text = ""
    for index, entry in enumerate(entries):
        text += entry if index == 0 else rng.choice([", ", ", ", ", ", ",", " , "]) + entry
    return text


def _literal(rng: random.Random, kind: str) -> str | None:
    """A field value as JSON text; ``None``: leave the field out."""
    roll = rng.random()
    if roll < 0.12:
        return None
    if roll < 0.2:
        # A JSON array is a keyword_list's entries in Searchlight but no list at all in
        # scrape-bot (whose lists are strings), so a list field never holds one here.
        others = [value for value in OTHER_VALUES if not (kind == "keyword_list" and value.startswith("["))]
        return rng.choice(others)
    if roll < 0.27:  # a value of the wrong sort
        wrong = rng.choice(["text", "number", "bool"])
        if wrong == "text" and kind != "date":
            return json.dumps(rng.choice(["12", "10", "true", "", " ", rng.choice(WORDS)]))
        if wrong == "number":
            return rng.choice(NUMBER_POOL)
        return rng.choice(["true", "false"])
    if kind in ("text", "keyword"):
        return json.dumps(_text_value(rng))
    if kind == "keyword_list":
        return json.dumps(_list_value(rng))
    if kind == "number":
        return rng.choice(NUMBER_POOL)
    if kind == "date":
        return rng.choice(["1700000000000", "1700000000000.5", "1600000000000", "0", "-1", "1e13"])
    return rng.choice(["true", "false"])


def _doc_id(rng: random.Random, index: int) -> str:
    stem = rng.choice(["doc", "Doc", "DOC", "ﬁle", "Straße", "item", "İtem", "日本", "a b", "x_y"])
    return f"{stem}-{index:03d}"


def _make_docs(rng: random.Random, count: int) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for index in range(count):
        members = []
        for field, kind in MAPPING.items():
            literal = _literal(rng, kind)
            if literal is not None:
                members.append(f"{json.dumps(field)}: {literal}")
        rng.shuffle(members)
        body = "{" + ", ".join(members) + "}"
        docs.append({"id": _doc_id(rng, index), "body": body})
    return docs


def _sample_text(rng: random.Random, attrs: list[dict[str, Any]], field: str) -> str:
    """A string some document holds in ``field``, or a vocabulary word."""
    found = [attr[field] for attr in attrs if isinstance(attr.get(field), str) and attr[field].strip()]
    if found and rng.random() < 0.7:
        return rng.choice(found)
    return rng.choice(WORDS)


def _vary(rng: random.Random, text: str) -> str:
    roll = rng.random()
    if roll < 0.2:
        return text.upper()
    if roll < 0.35:
        return text.lower()
    if roll < 0.45:
        return "  " + text.replace(" ", "\t ") + " "
    return text


def _clean_query_text(text: str) -> str | None:
    text = text.replace("\x00", "").replace("\ufffd", "")
    text = text[:500]
    return text if text.strip() else None


def _substring(rng: random.Random, text: str) -> str:
    if len(text) <= 1:
        return text
    start = rng.randrange(len(text))
    return text[start : start + rng.randint(1, 6)]


def _text_query_value(rng: random.Random, attrs: list[dict[str, Any]], field: str, op: str) -> Any:
    def one() -> str | None:
        source = _sample_text(rng, attrs, field)
        if op.startswith("contains"):
            source = _substring(rng, source) if rng.random() < 0.7 else source
        elif op == "starts_with":
            source = source.lstrip()[: rng.randint(1, 8)] if rng.random() < 0.7 else source
        elif op.startswith("words"):
            words = source.split()
            if words and rng.random() < 0.7:
                start = rng.randrange(len(words))
                source = " ".join(words[start : start + rng.randint(1, 2)])
        return _clean_query_text(_vary(rng, source))

    if op in ("eq", "ne", "contains", "starts_with"):
        return one()
    if op == "similar":
        text = one()
        if text is None:
            return None
        return {"text": text, "min": rng.choice([0.05, 0.1, 0.2, 0.3, 0.333, 0.5, 0.8, 1, 1.0])}
    values = [one() for _ in range(rng.randint(1, 4))]
    return [value for value in values if value is not None] or None


def _id_query_value(rng: random.Random, ids: list[str], op: str) -> Any:
    def one() -> str | None:
        source = rng.choice(ids) if rng.random() < 0.8 else rng.choice(WORDS)
        if op.startswith("contains"):
            source = _substring(rng, source)
        elif op == "starts_with":
            source = source[: rng.randint(1, 6)]
        return _clean_query_text(_vary(rng, source))

    if op in ("eq", "ne", "contains", "starts_with"):
        return one()
    if op == "similar":
        text = one()
        return None if text is None else {"text": text, "min": rng.choice([0.1, 0.3, 0.5, 1])}
    values = [one() for _ in range(rng.randint(1, 4))]
    return [value for value in values if value is not None] or None


def _list_query_value(rng: random.Random, conditions: Any, attrs: list[dict[str, Any]], field: str, op: str) -> Any:
    def one() -> str | None:
        entries = [
            entry
            for attr in attrs
            if isinstance(attr.get(field), str)
            for entry in conditions.list_entries(attr[field])
        ]
        source = rng.choice(entries) if entries and rng.random() < 0.75 else rng.choice(WORDS)
        source = source.replace(",", "")
        return _clean_query_text(_vary(rng, source))

    if op == "has":
        return one()
    values = [one() for _ in range(rng.randint(1, 4))]
    return [value for value in values if value is not None] or None


def _number_query_value(rng: random.Random, kind: str, op: str) -> Any:
    def one() -> Any:
        if kind == "date":
            return rng.choice([0, 1600000000000, 1700000000000, 1700000000000.5, 1e13, -1])
        return rng.choice(NUMBER_QUERY_POOL)

    if op == "between":
        low, high = sorted([one(), one()])
        return [low, high]
    if op == "in":
        return [one() for _ in range(rng.randint(1, 4))]
    return one()


def _leaf(rng: random.Random, conditions: Any, attrs: list[dict[str, Any]], ids: list[str]) -> dict[str, Any] | None:
    field = rng.choice([*MAPPING, "_id"])
    kind = "keyword" if field == "_id" else MAPPING[field]
    extra = ["words_all", "words_any"] * 3 if kind == "text" else []  # rarer otherwise
    op = rng.choice(OPS[kind] + extra)
    if op in ("empty", "nonempty"):
        return {"field": field, "op": op}
    if op == "exists":
        roll = rng.random()
        if roll < 0.3:
            return {"field": field, "op": op}
        return {"field": field, "op": op, "value": roll < 0.65}
    if kind == "bool":
        value: Any = rng.choice([True, False])
    elif kind in ("number", "date"):
        value = _number_query_value(rng, kind, op)
    elif kind == "keyword_list":
        value = _list_query_value(rng, conditions, attrs, field, op)
    elif field == "_id":
        value = _id_query_value(rng, ids, op)
    else:
        value = _text_query_value(rng, attrs, field, op)
    if value is None:
        return None
    problem = conditions.value_problem(op, value, kind=KIND[kind], field=field)
    if problem is not None:
        return None
    return {"field": field, "op": op, "value": value}


class _Budget:
    def __init__(self) -> None:
        self.leaves = 0
        self.nodes = 0


def _node(
    rng: random.Random,
    make_leaf: Callable[[], dict[str, Any] | None],
    budget: _Budget,
    depth: int,
) -> dict[str, Any] | None:
    """A random node; ``depth`` is the all/any groups already above it."""
    roll = rng.random()
    if depth >= 4 or budget.nodes >= 90 or budget.leaves >= 45 or roll < 0.55:
        leaf = make_leaf()
        if leaf is not None:
            budget.leaves += 1
            budget.nodes += 1
        return leaf
    if roll < 0.65:
        child = _node(rng, make_leaf, budget, depth)
        if child is None:
            return None
        budget.nodes += 1
        return {"not": child}
    budget.nodes += 1
    children = [_node(rng, make_leaf, budget, depth + 1) for _ in range(rng.randint(1, 4))]
    kept = [child for child in children if child is not None]
    if not kept:
        return None
    return {rng.choice(["all", "any"]): kept}


def _for_oracle(node: Any) -> Any:
    """The query as scrape-bot spells it: ``_id`` is its ``key`` pseudo-field."""
    if isinstance(node, dict):
        return {
            key: ("key" if key == "field" and value == "_id" else _for_oracle(value))
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [_for_oracle(child) for child in node]
    return node


def _match_fixture(
    conditions: Any, search_index: Any, oracle: Any, search_query: Any, rng: random.Random
) -> dict[str, Any]:
    groups: list[dict[str, Any]] = []
    op_counts: dict[str, int] = {}
    hits = 0
    evaluations = 0
    for _ in range(8):
        docs = _make_docs(rng, 60)
        attrs = [json.loads(doc["body"]) for doc in docs]
        ids = [doc["id"] for doc in docs]
        items = [
            {"title": None, "url": None, "external_key": doc["id"], "attributes": attr}
            for doc, attr in zip(docs, attrs, strict=True)
        ]
        for doc, item in zip(docs, items, strict=True):
            doc["index"] = search_index.index_doc(item, doc["id"], None).document["f"]
        queries: list[dict[str, Any]] = []
        while len(queries) < 80:
            if rng.random() < 0.03:
                query: dict[str, Any] | None = {"all": []}
            else:
                query = _node(rng, lambda: _leaf(rng, conditions, attrs, ids), _Budget(), 0)
            if query is None:
                continue
            spec = search_query.SearchSpec.model_validate({"query": _for_oracle(query)})
            matched = [
                doc["id"]
                for doc, item in zip(docs, items, strict=True)
                if oracle.matches(spec, item, frozenset())
            ]
            for leaf in _leaves(query):
                op_counts[leaf["op"]] = op_counts.get(leaf["op"], 0) + 1
            hits += len(matched)
            evaluations += len(docs)
            queries.append({"query": query, "matches": matched})
        groups.append({"docs": docs, "queries": queries})
    print(f"match: {evaluations} evaluations, {hits} hits, ops {sorted(op_counts.items())}")
    missing = {op for ops in OPS.values() for op in ops} - set(op_counts)
    if missing:
        raise SystemExit(f"match fixture misses ops: {sorted(missing)}")
    return {"mapping": {"dynamic": False, "fields": MAPPING}, "groups": groups}


def _leaves(node: Any) -> Iterator[dict[str, Any]]:
    if "op" in node:
        yield node
    elif "not" in node:
        yield from _leaves(node["not"])
    else:
        for child in node.get("all", node.get("any", [])):
            yield from _leaves(child)


# --------------------------------------------------------------------------- tables


def _go_string(text: str) -> str:
    out = ['"']
    for char in text:
        code = ord(char)
        if char in '"\\':
            out.append("\\" + char)
        elif 0x20 <= code < 0x7F:
            out.append(char)
        elif code <= 0xFFFF:
            out.append(f"\\u{code:04x}")
        else:
            out.append(f"\\U{code:08x}")
    out.append('"')
    return "".join(out)


def _go_range_table(name: str, doc: str, codes: list[int]) -> str:
    runs = _ranges(codes)
    r16 = [run for run in runs if run[1] <= 0xFFFF]
    r32 = [run for run in runs if run[0] > 0xFFFF]
    split = [run for run in runs if run[0] <= 0xFFFF < run[1]]
    for low, high in split:
        r16.append([low, 0xFFFF])
        r32.insert(0, [0x10000, high])
    latin = sum(1 for run in r16 if run[1] <= 0xFF)
    lines = [f"// {doc}", f"var {name} = &unicode.RangeTable{{"]
    if r16:
        lines.append("\tR16: []unicode.Range16{")
        lines.extend(f"\t\t{{Lo: 0x{low:04x}, Hi: 0x{high:04x}, Stride: 1}}," for low, high in r16)
        lines.append("\t},")
    if r32:
        lines.append("\tR32: []unicode.Range32{")
        lines.extend(f"\t\t{{Lo: 0x{low:x}, Hi: 0x{high:x}, Stride: 1}}," for low, high in r32)
        lines.append("\t},")
    if latin:
        lines.append(f"\tLatinOffset: {latin},")
    lines.append("}")
    return "\n".join(lines)


def _go_map(name: str, doc: str, mapping: dict[int, str]) -> str:
    lines = [f"// {doc}", f"var {name} = map[rune]string{{"]
    lines.extend(f"\t0x{code:04x}: {_go_string(text)}," for code, text in sorted(mapping.items()))
    lines.append("}")
    return "\n".join(lines)


def _sigma(text: str, index: int) -> str:
    return text.lower()[index]


def _tables(commit: str) -> str:
    space = [code for code in _code_points() if chr(code).isspace()]
    word = [code for code in _code_points() if _WORD.match(chr(code))]
    fold = {code: chr(code).casefold() for code in _code_points() if code >= 0x80 and chr(code).casefold() != chr(code)}
    lower = {code: chr(code).lower() for code in _code_points() if code >= 0x80 and chr(code).lower() != chr(code)}
    # Final_Sigma probes. Python lowers U+03A3 to U+03C2 when the nearest non-case-ignorable
    # character before it is cased and the nearest after it is not. "aΣxb" gives sigma
    # unless x is neither ignorable nor cased; " xΣ" gives final sigma only when x is cased
    # and not ignorable. Whether an ignorable character is cased never matters: it is
    # skipped before the cased test.
    ignorable: list[int] = []
    cased: list[int] = []
    for code in _code_points():
        char = chr(code)
        ignorable_or_cased = _sigma(f"aΣ{char}b", 1) == "σ"
        cased_not_ignorable = (" " + char + "Σ").lower()[-1] == "ς"
        if ignorable_or_cased and not cased_not_ignorable:
            ignorable.append(code)
        if cased_not_ignorable:
            cased.append(code)
    blocks = [
        _go_range_table("pySpace", "pySpace is str.isspace: what str.split() and str.strip() split on.", space),
        _go_range_table("pyWord", "pyWord is the re module's \\w for str patterns (str.isalnum() and '_').", word),
        _go_range_table(
            "caseIgnorable",
            "caseIgnorable is Python's Case_Ignorable, as str.lower() reads it for Final_Sigma.",
            ignorable,
        ),
        _go_range_table(
            "casedNotIgnorable",
            "casedNotIgnorable is Python's Cased, less the case-ignorable characters (which\n// Final_Sigma skips before it asks whether a character is cased).",
            cased,
        ),
        _go_map("foldMap", "foldMap is str.casefold() of every non-ASCII code point it changes.", fold),
        _go_map("lowerMap", "lowerMap is str.lower() of every non-ASCII code point it changes (U+03A3 out of context).", lower),
    ]
    header = (
        "// Code generated by tools/parity/gen.py from scrape-bot "
        f"{commit[:12]}; DO NOT EDIT.\n\n"
        "package analysis\n\n"
        'import "unicode"\n\n'
        "// UnicodeVersion is the Unicode version of the Python these tables were generated by.\n"
        f'const UnicodeVersion = "{unicodedata.unidata_version}"\n'
    )
    return header + "\n" + "\n\n".join(blocks) + "\n"


# ----------------------------------------------------------------------------- main


def _write(name: str, meta: dict[str, Any], payload: Any) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    document = {"generator": meta, "data": payload}
    text = json.dumps(document, ensure_ascii=True, indent=1) + "\n"
    (OUT / name).write_text(text, encoding="utf-8", newline="\n")
    print(f"wrote {OUT / name} ({len(text)} bytes)")


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="searchlight-parity-") as temp:
        tree = Path(temp)
        commit = _export(tree)
        sys.path[:0] = [str(tree / "src"), str(tree)]
        from scrape_bot import conditions, search_index, search_query  # noqa: PLC0415
        from tests.support import search_oracle  # noqa: PLC0415

        for module in (conditions, search_index, search_query, search_oracle):
            origin = Path(module.__file__ or "").resolve()
            if not origin.is_relative_to(tree.resolve()):
                raise SystemExit(f"{module.__name__} came from {origin}, not the exported tree")

        meta = {
            "tool": "tools/parity/gen.py",
            "scrape_bot_ref": REF,
            "scrape_bot_commit": commit,
            "python": sys.version.split()[0],
            "unicode": unicodedata.unidata_version,
            "seed": SEED,
        }
        _write("normalize.json", meta, _normalize_fixture(conditions, search_index, random.Random(SEED)))
        _write("words.json", meta, _words_fixture(conditions, random.Random(SEED + 1)))
        _write("entries.json", meta, _entries_fixture(conditions, search_index, random.Random(SEED + 2)))
        _write("numbers.json", meta, _numbers_fixture(conditions, random.Random(SEED + 3)))
        _write("similarity.json", meta, _similarity_fixture(conditions, random.Random(SEED + 4)))
        _write(
            "match.json",
            meta,
            _match_fixture(conditions, search_index, search_oracle, search_query, random.Random(SEED + 5)),
        )
        TABLES.parent.mkdir(parents=True, exist_ok=True)
        TABLES.write_text(_tables(commit), encoding="utf-8", newline="\n")
        print(f"wrote {TABLES}")
        for name in [name for name in sys.modules if name.split(".")[0] in ("scrape_bot", "tests")]:
            del sys.modules[name]


if __name__ == "__main__":
    main()
