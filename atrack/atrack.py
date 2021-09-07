"""Module for tracking the states of a tree of state machines.
"""
from collections import deque
import io
import json
import asyncio
from types import SimpleNamespace
from typing import Any, Dict, Generator, List, NamedTuple, Optional, Type, Union

from json import JSONEncoder
from copy import deepcopy


class _Encoder(JSONEncoder):
    def default(self, o):
        return o.__dict__


class _State:
    def __init__(self, name: str, data: Optional[dict] = None) -> None:
        self.name = name
        self.data = data

    def __repr__(self) -> str:
        repr_s = f"State<{self.name}"
        if self.data:
            repr_s += f" with data {self.data}"
        repr_s += ">"
        return repr_s

    def __eq__(self, other: object):
        if isinstance(other, _State):
            return self.name == other.name and (
                # if either has data, compare it
                (not other.data and not self.data)
                or other.data == self.data
            )
        return self == other

    def with_data(self, data: Optional[dict]) -> "_State":
        """Add data to a copy of this state."""
        if data is None:
            return self

        s_copy = deepcopy(self)
        s_copy.data = data
        return s_copy


UNKNOWN = _State("UNKNOWN")
RUNNING = _State("RUNNING")
SUCCESS = _State("SUCCESS")
FAILURE = _State("FAILURE")


# _Event is to be replaced by a CloudEvent
class _Event:  # pylint: disable=too-few-public-methods
    def __init__(self, src: str, type_: str, data: Optional[dict] = None) -> None:
        self.src = src
        self.type_ = type_
        self.data = data

    def __repr__(self) -> str:
        repr_s = f"Event<{self.type_}@{self.src}"
        if self.data:
            repr_s += f" with data {self.data}"
        repr_s += ">"
        return repr_s


class _Transition(NamedTuple):
    from_state: _State
    to_state: _State

    def comparable_to(self, from_state: _State, to_state: _State) -> bool:
        """Disregarding node, are the two transitions roughly comparable."""
        return (
            self.from_state.name == from_state.name
            and self.to_state.name == to_state.name
        )

    def __repr__(self) -> str:
        return f"Transition<{self.from_state} -> {self.to_state}>"


class _StateChange(NamedTuple):
    transition: _Transition
    node: "_FSM"

    def __repr__(self) -> str:
        return f"StateChange<{self.transition}@{self.node}>"

    def __lt__(self, other) -> bool:
        return self.node.src < other.node.src

    @property
    def src(self) -> str:
        return self.node.src

    @staticmethod
    def changeset_to_partial(changes: List["_StateChange"]) -> dict:
        """Create a partial from a list of state changes."""
        partial: Dict[str, Any] = {}

        # Each key represents an entity which is mapped to from iterating over
        # the path of the change. So given /foo/bar/baz/qux, foo is None, qux
        # maps to "jobs".
        partial_key_hierarchy: List[str] = [None, "reals", "steps", "jobs"]

        sub_partial = partial
        for change in changes:
            parts = change.node.src.split("/")[1:]

            # Iteratively build a partial dict while looping over the parts of
            # the path e.g. /foo/bar/baz/qux. Given that path, qux is the
            # terminal part, and foo represents the "reals", qux the "jobs",
            # etc.
            # TODO: handle multiple changes for e.g. one job
            for pos, part in enumerate(parts):
                is_terminal_part = pos == len(parts) - 1
                key = partial_key_hierarchy[pos]

                if key and key not in sub_partial:
                    sub_partial[key] = {part: {}}
                    sub_partial = sub_partial[key][part]
                elif key in sub_partial:
                    if not part in sub_partial[key]:
                        sub_partial[key][part] = {}
                    sub_partial = sub_partial[key][part]

                if is_terminal_part:
                    sub_partial["status"] = change.transition.to_state.name
                    if change.transition.to_state.data:
                        sub_partial["data"] = change.transition.to_state.data
                    sub_partial = partial

        return partial


class IllegalTransition(Exception):
    """Represents an illegal transition."""

    def __init__(self, error: str, node: "_FSM", transition: _Transition) -> None:
        super().__init__(error)
        self.node = node
        self.transition = transition
        self.src = node.src


_TransitionResult = Union[_StateChange, IllegalTransition]
_TransitionTrigger = Union[_Event, _StateChange]


class _FSM:
    def __init__(self, branch: str, id_: str, parent: Optional["_FSM"] = None) -> None:
        self._id = id_
        self._branch = branch
        self._parent: Optional[_FSM] = parent
        self._children: List[_FSM] = []
        if self._parent:
            self._parent._children.append(self)

        self._state: _State = UNKNOWN
        self._transitions: List[_Transition] = []

    def __repr__(self) -> str:
        return f"Node<{self.__class__.__name__}@{self.src}>"

    @property
    def id_(self) -> str:
        """The id property."""
        return self._id

    @property
    def children(self) -> List["_FSM"]:
        """The children property."""
        return self._children

    @property
    def src(self) -> str:
        """Return the source of this node, which is a textual presentation of
        this nodes position in the tree in the form of /foo/bar/baz.
        """
        return self._branch + self._id

    @property
    def path(self) -> str:
        """Return the path of this node, which is the src property plus a
        trailing slash.
        """
        return self.src + "/"

    @property
    def state(self) -> _State:
        """The state property"""
        return self._state

    @state.setter
    def state(self, state: _State) -> None:
        """Set the state of this node."""
        prev_data = self._state.data
        self._state = state.with_data(prev_data)

    def transition(self, to_state: _State) -> Generator[_TransitionResult, None, None]:
        """Transition this node to a new state."""
        for trans in self._transitions:
            if trans.comparable_to(self.state, to_state):

                # these states are equal even considering data, thus no-op
                if trans.from_state == to_state:
                    break

                self.state = to_state
                yield _StateChange(
                    transition=_Transition(trans.from_state, to_state), node=self
                )
                break
        else:
            yield IllegalTransition(
                f"no transition for {self} from {self.state} -> {to_state}",
                self,
                _Transition(self.state, to_state),
            )

    def add_transition(self, transition: _Transition) -> None:
        """Add a transition."""
        self._transitions.append(transition)

    def is_applicable(self, obj: _TransitionTrigger) -> bool:
        """Return whether or not the event is applicable to this node."""
        return obj.src.startswith(self.src)

    def _dispatch_event(
        self, event: _Event
    ) -> Generator[_TransitionResult, None, None]:
        if self.children:
            for child in self.children:
                yield from child.dispatch(event)
        else:
            yield from ()

    def _dispatch_state_change(
        self, state_change: _StateChange
    ) -> Generator[_TransitionResult, None, None]:
        if self.children:
            for child in self.children:
                yield from child.dispatch(state_change)
        else:
            yield from ()

    def _dispatch_illegal_transition(
        self, illegal_transition: IllegalTransition
    ) -> Generator[_TransitionResult, None, None]:
        """Should not be called at all if the illegal_transition has been
        handled."""
        if self.children:
            for child in self.children:
                yield from child.dispatch(illegal_transition)
        else:
            raise illegal_transition

    def dispatch(
        self, obj: _TransitionTrigger
    ) -> Generator[_TransitionResult, None, None]:
        """Dispatch something that triggers changes."""
        if not self.is_applicable(obj):
            return
        if isinstance(obj, _Event):
            yield from self._dispatch_event(obj)
        elif isinstance(obj, _StateChange):
            yield from self._dispatch_state_change(obj)
        elif isinstance(obj, IllegalTransition):
            yield from self._dispatch_illegal_transition(obj)
        else:
            raise TypeError(f"cannot dispatch {type(obj)}")


class _Job(_FSM):
    def __init__(self, id_: str, parent: _FSM) -> None:
        super().__init__(parent.path, id_, parent=parent)
        self.add_transition(_Transition(UNKNOWN, RUNNING))
        self.add_transition(_Transition(RUNNING, RUNNING))
        self.add_transition(_Transition(RUNNING, SUCCESS))
        self.add_transition(_Transition(UNKNOWN, FAILURE))
        self.add_transition(_Transition(RUNNING, FAILURE))

    def _dispatch_event(
        self, event: _Event
    ) -> Generator[_TransitionResult, None, None]:
        if event.type_ == "JOB_STARTED":
            yield from self.transition(RUNNING.with_data(event.data))
        elif event.type_ == "JOB_RUNNING":
            yield from self.transition(RUNNING.with_data(event.data))
        elif event.type_ == "JOB_SUCCESS":
            yield from self.transition(SUCCESS.with_data(event.data))
        elif event.type_ == "JOB_FAILURE":
            yield from self.transition(FAILURE.with_data(event.data))

        yield from super()._dispatch_event(event)


class _Step(_FSM):
    def __init__(self, id_: str, parent: "_FSM") -> None:
        super().__init__(parent.path, id_, parent=parent)
        self.add_transition(_Transition(UNKNOWN, RUNNING))
        self.add_transition(_Transition(RUNNING, RUNNING))
        self.add_transition(_Transition(RUNNING, SUCCESS))

    def _dispatch_event(
        self, event: _Event
    ) -> Generator[_TransitionResult, None, None]:
        if event.type_ == "STEP_STARTED":
            yield from self.transition(RUNNING.with_data(event.data))
        elif event.type_ == "STEP_RUNNING":
            yield from self.transition(RUNNING.with_data(event.data))
        elif event.type_ == "STEP_SUCCESS":
            yield from self.transition(SUCCESS.with_data(event.data))

        yield from super()._dispatch_event(event)

    def _dispatch_state_change(
        self, state_change: _StateChange
    ) -> Generator[_TransitionResult, None, None]:
        if state_change.node in self.children:
            if state_change.transition.to_state == RUNNING:
                yield from self.transition(RUNNING)
            elif state_change.transition.to_state == SUCCESS:
                # are all jobs succeeding?
                for job in self._children:
                    if job.state != SUCCESS:
                        break
                else:
                    yield from self.transition(SUCCESS)

        yield from super()._dispatch_state_change(state_change)

    def _dispatch_illegal_transition(
        self, illegal_transition: IllegalTransition
    ) -> Generator[_TransitionResult, None, None]:
        if illegal_transition.node in self.children:
            if illegal_transition.transition.to_state == SUCCESS:
                # Assume it, and the jobs preceding it, are SUCCESS
                i = self._children.index(illegal_transition.node)
                while i >= 0:
                    old_state = self._children[i].state
                    self._children[i].state = SUCCESS
                    yield _StateChange(
                        transition=_Transition(old_state, self._children[i].state),
                        node=self._children[i],
                    )
                    i -= 1

                # since it was handled here, stop propagating it downwards
                return

        yield from super()._dispatch_illegal_transition(illegal_transition)


class _Realization(_FSM):
    def __init__(self, id_: str, parent: _FSM) -> None:
        super().__init__(parent.path, id_, parent=parent)
        self.add_transition(_Transition(UNKNOWN, RUNNING))
        self.add_transition(_Transition(RUNNING, RUNNING))
        self.add_transition(_Transition(RUNNING, SUCCESS))

    def _dispatch_state_change(
        self, state_change: _StateChange
    ) -> Generator[_TransitionResult, None, None]:
        if state_change.node in self.children:
            if state_change.transition.to_state == RUNNING:
                yield from self.transition(RUNNING)
            elif state_change.transition.to_state == SUCCESS:
                # are all steps succeeding?
                for step in self._children:
                    if step.state != SUCCESS:
                        break
                else:
                    yield from self.transition(SUCCESS)

        yield from super()._dispatch_state_change(state_change)


class _Ensemble(_FSM):
    def __init__(self, id_: str) -> None:
        super().__init__("/", id_, parent=None)
        self.add_transition(_Transition(UNKNOWN, RUNNING))
        self.add_transition(_Transition(RUNNING, RUNNING))
        self.add_transition(_Transition(RUNNING, SUCCESS))
        self.add_transition(_Transition(SUCCESS, SUCCESS))

    def snapshot(self) -> dict:
        """Return a snapshot representing the state of this and all other
        descendant nodes."""
        snapshot: Dict[str, Any] = {"status": self.state.name, "reals": {}}
        for real in self.children:
            snapshot["reals"][real.id_] = {"status": real.state.name, "steps": {}}
            if real.state.data:
                snapshot["reals"][real.id_]["data"] = real.state.data

            for step in real.children:
                step_d: Dict[str, Any] = {
                    "status": step.state.name,
                    "jobs": {},
                }
                if step.state.data:
                    step_d["data"] = step.state.data
                snapshot["reals"][real.id_]["steps"][step.id_] = step_d
                for job in step.children:
                    job_d: Dict[str, Any] = {"status": job.state.name}
                    if job.state.data:
                        job_d["data"] = job.state.data
                    snapshot["reals"][real.id_]["steps"][step.id_]["jobs"][
                        job.id_
                    ] = job_d
        return snapshot

    def _dispatch_event(
        self, event: _Event
    ) -> Generator[_TransitionResult, None, None]:
        if event.type_ == "ENS_STARTED":
            yield from self.transition(RUNNING.with_data(event.data))

        if event.type_ == "ENS_SUCCESS":
            yield from self.transition(SUCCESS.with_data(event.data))

        yield from super()._dispatch_event(event)

    def _dispatch_state_change(
        self, state_change: _StateChange
    ) -> Generator[_TransitionResult, None, None]:
        if state_change.node in self.children:
            if state_change.transition.to_state == RUNNING:
                yield from self.transition(RUNNING)
            elif state_change.transition.to_state == SUCCESS:
                # are all reals succeeding?
                for real in self.children:
                    if real.state != SUCCESS:
                        break
                else:
                    yield from self.transition(SUCCESS)

        yield from super()._dispatch_state_change(state_change)

    def _recursive_dispatch(
        self, change: _TransitionTrigger
    ) -> Generator[_TransitionResult, None, None]:
        deck = deque([change])
        max_iterations = 1000
        iterations = 0
        while len(deck):
            iterations += 1
            trigger = deck.pop()
            changes = list(super().dispatch(trigger))
            deck.extendleft(changes)
            yield from changes

            if iterations > max_iterations:
                raise RecursionError(f"{change} caused > {max_iterations} changes")

    def dispatch(
        self, obj: _TransitionTrigger
    ) -> Generator[_TransitionResult, None, None]:
        """Dispatch something that triggers a change, but recurse over the tree
        for all changes until there are no more."""
        if not self.is_applicable(obj):
            return
        if not isinstance(obj, _Event) and not isinstance(obj, IllegalTransition):
            raise TypeError(f"cannot dispatch {type(obj)}")
        for change in self._dispatch_event(obj):
            yield change
            yield from self._recursive_dispatch(change)

    @staticmethod
    def from_ert_trivial_graph_format(tgf: str) -> "_Ensemble":
        """Takes ERT Trivial Graph Format (ERTGRAF) and converts it into an _Ensemble
        FSM. The ERTGRAF is defined to be directed, acyclical and formatted as follows:
            <type of node> <source>
            …
            <type of node> <source>

        Where the type of node can be one of Ensemble, Realization, Step and
        Job. Source is the path of the node, e.g. /foo/bar/baz/qux. A trivial
        example is
            Ensemble /0
            Realization /0/0
            Realization /0/1
            Step /0/0/0
            Job /0/0/0/0
        """
        type_map: Dict[str, Any] = {
            "Ensemble": _Ensemble,
            "Realization": _Realization,
            "Step": _Step,
            "Job": _Job,
        }
        source_to_type: Dict[str, Any] = {}
        s = io.StringIO(tgf)
        for line in s.readlines():
            line = line.strip()
            if not line:
                continue
            wanted_type, source = line.split(" ")
            if wanted_type not in type_map:
                raise TypeError(f"unexpected type {wanted_type}")
            source_to_type[source] = wanted_type

        source_to_instance: Dict[str, _FSM] = {}
        root = None
        for source in sorted(
            source_to_type.keys(), key=lambda source: len(source.split("/"))
        ):
            parts = source.split("/")[1:]
            if len(parts) - 1 == 0:  # root
                source_to_instance[source] = type_map[source_to_type[source]](
                    parts[len(parts) - 1]
                )
                root = source_to_instance[source]
            else:
                parent = source_to_instance["/" + "/".join(parts[:-1])]
                source_to_instance[source] = type_map[source_to_type[source]](
                    parts[len(parts) - 1], parent
                )
        return root


async def main():
    ens = _Ensemble("0")
    real_1 = _Realization("r1", ens)
    real_2 = _Realization("r2", ens)

    step_1 = _Step("s1", real_1)
    step_2 = _Step("s2", real_2)

    _Job("j1", step_1)
    _Job("j2", step_1)

    _Job("j1", step_2)
    _Job("j2", step_2)

    for evt in [
        _Event("/0", "ENS_STARTED"),
        _Event("/0/r1/s1", "STEP_STARTED"),
        _Event("/0/r1/s1/j1", "JOB_STARTED"),
        _Event("/0/r1/s1/j1", "JOB_RUNNING", {"max_mem": 100}),
        _Event("/0/r1/s1/j2", "JOB_SUCCESS"),
        _Event("/0/r1/s1", "STEP_SUCCESS"),
        _Event("/0/r2/s2", "STEP_STARTED"),
        _Event("/0/r2/s2/j1", "JOB_STARTED"),
        _Event("/0/r2/s2/j1", "JOB_RUNNING"),
        _Event("/0/r2/s2/j1", "JOB_SUCCESS"),
        _Event("/0/r2/s2/j2", "JOB_STARTED"),
        _Event("/0/r2/s2/j2", "JOB_SUCCESS"),
        _Event("/0/r2/s2/j2", "JOB_RUNNING"),
        _Event("/0/r2/s2", "STEP_SUCCESS"),
        _Event("/0", "ENS_SUCCESS"),
    ]:
        changes: List[_StateChange] = []
        for change in ens.dispatch(evt):
            if not isinstance(change, _StateChange):
                continue
            changes.append(change)
        partial = _StateChange.changeset_to_partial(sorted(changes))
        print(evt, changes, partial)

    print(json.dumps(ens.snapshot(), sort_keys=True, indent=4, cls=_Encoder))


if __name__ == "__main__":
    asyncio.run(main())
