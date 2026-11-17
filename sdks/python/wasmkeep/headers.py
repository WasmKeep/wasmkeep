"""Headers as the wire carries them: an ordered list of (name, value) pairs
in which a name may repeat. Names compare without case. Pure: nothing here
imports the host."""

from typing import Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union

Pairs = List[Tuple[str, str]]
HeadersInit = Union[Mapping[str, Union[str, Sequence[str]]], Iterable[Tuple[str, str]], "Headers"]


def pairs_of(init: Optional[HeadersInit]) -> Pairs:
    """`init` as pairs in order (a copy); a mapping's list value is one pair
    per value. Names are kept as given."""
    if init is None:
        return []
    if isinstance(init, Headers):
        return init.items()
    if isinstance(init, Mapping):
        out: Pairs = []
        for name, value in init.items():
            if isinstance(value, str):
                out.append((name, value))
            else:
                out.extend((name, v) for v in value)
        return out
    return [(n, v) for n, v in init]


class Headers:
    """An ordered list of header pairs; a name may repeat and compares
    without case. `get` is the first value, `get_all` every value in order,
    `append` adds, `set` replaces every value. A Set-Cookie is never joined."""

    def __init__(self, init: Optional[HeadersInit] = None):
        self._list: Pairs = pairs_of(init)

    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        lower = name.lower()
        for n, v in self._list:
            if n.lower() == lower:
                return v
        return default

    def get_all(self, name: str) -> List[str]:
        lower = name.lower()
        return [v for n, v in self._list if n.lower() == lower]

    getAll = get_all

    def has(self, name: str) -> bool:
        return self.get(name) is not None

    def append(self, name: str, value: str) -> "Headers":
        self._list.append((name, value))
        return self

    def set(self, name: str, value: str) -> "Headers":
        """Replaces every value of `name`, at the place of its first."""
        lower = name.lower()
        at = next((i for i, (n, _) in enumerate(self._list) if n.lower() == lower), None)
        if at is None:
            return self.append(name, value)
        self._list = [p for i, p in enumerate(self._list) if i <= at or p[0].lower() != lower]
        self._list[at] = (name, value)
        return self

    def delete(self, name: str) -> "Headers":
        lower = name.lower()
        self._list = [p for p in self._list if p[0].lower() != lower]
        return self

    def items(self) -> Pairs:
        """Every pair, in order (a copy)."""
        return list(self._list)

    def __iter__(self) -> Iterator[Tuple[str, str]]:
        return iter(self.items())

    def __len__(self) -> int:
        return len(self._list)

    def __repr__(self) -> str:
        return f"Headers({self._list!r})"


def last_values(pairs: Iterable[Tuple[str, str]]) -> Dict[str, str]:
    """The older dict view: by name as sent, of a repeat the last value."""
    return {k: v for k, v in pairs}


def response_pairs(headers: Optional[HeadersInit], more: Optional[HeadersInit] = None) -> Pairs:
    """The headers of a response (`headers`, then `more`): names in lower
    case, repeats kept in order, and `content-type: text/plain; charset=utf-8`
    unless one is given."""
    given = [(n.lower(), v) for n, v in pairs_of(headers) + pairs_of(more)]
    if any(n == "content-type" for n, _ in given):
        return given
    return [("content-type", "text/plain; charset=utf-8")] + given
