"""Stands in for the bindings componentize-py generates, so `wasmkeep`
imports under plain CPython. Only `actors.call` does anything: tests
`install` a function that records each call and answers it (or fails it, as
the host would, with `fail(kind, value)`). Import this before `wasmkeep`."""

import os
import sys
import types


class _Stub(types.ModuleType):
    """A module whose every missing attribute is a placeholder class."""

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        cls = type(name, (), {"__init__": lambda self, *a, **k: None})
        setattr(self, name, cls)
        return cls


def _module(name, **attrs):
    m = _Stub(name)
    m.__path__ = []  # a package, so submodules import
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    parent, _, child = name.rpartition(".")
    if parent:
        setattr(sys.modules[parent], child, m)
    return m


class Err(Exception):
    def __init__(self, value):
        super().__init__(value)
        self.value = value


class Ok:
    def __init__(self, value):
        self.value = value


class Request:
    def __init__(self, method, path, headers, body):
        self.method = method
        self.path = path
        self.headers = headers
        self.body = body


class Response:
    """What the host's `actors.call` returns."""

    def __init__(self, status, body=b"", headers=()):
        self.status = status
        self.headers = list(headers)
        self.body = body


_answer = None


def install(answer):
    """Every call goes to `answer(address, request)`, which returns a
    `Response` or raises `fail(...)`."""
    global _answer
    _answer = answer


def fail(kind, value=None):
    """The host's error for a failed call: `kind` is the WIT case
    (invalid-address, reentrant, too-large, timeout, failed)."""
    name = "CallError_" + "".join(p.title() for p in kind.split("-"))
    case = type(name, (), {})()
    if value is not None:
        case.value = value
    return Err(case)


def _call(address, request):
    return _answer(address, request)


_module("componentize_py_types", Err=Err, Ok=Ok)
_module("wit_world")
_module("wit_world.exports")
_module("wit_world.imports")
_module("wit_world.imports.actors", Request=Request, Response=Response, call=_call)
for _name in ("alarms", "env", "storage", "websockets", "streams", "wasi_http_types", "wasmkeep_actor_types"):
    _module("wit_world.imports." + _name)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
