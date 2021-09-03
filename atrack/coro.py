
from typing import NamedTuple


class Change(NamedTuple):
    id_: str


class Event(NamedTuple):
    id_: str


def dispatch_event(evt):
    yield Change(id_=evt.id_+"_change") 


def dispatch_change(change):
    yield Change(id_=change.id_+"_change")

def dispatch(evt):

    def _dispatch_recursive():
        while True:
            change = yield
            yield from dispatch_change(change)

    change = next(dispatch_event(evt))
    yield change
    dispatch_gen = _dispatch_recursive()
    i = 0
    while change:
        next(dispatch_gen)
        change = dispatch_gen.send(change)
        yield change
        i += 1
        if i == 10:
            break

for change in dispatch(Event(id_="evt1")):
    print("foo", change)

# def dispatch():
#     change = None
#     while True:
#         change = (yield change)
#         d = dispatch_change(change)
#         if not d:
#             break
#         change = next(d)
#         print("dispatch", change)


# for change in dispatch_event(Event(id_="evt1")):
#     i = 0
#     coro = dispatch()
#     coro.send(None)
#     print(f"change {i}", change)
#     while change:
#         i += 1
#         try:
#             change = coro.send(change)
#             print(f"change {i}", change)
#         except StopIteration:
#             break
#         if i==10:
#             break
