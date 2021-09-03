from typing import Generator, Union
import pytest
from atrack.atrack import (
    _Ensemble,
    _StateChange,
    _FSM,
    UNKNOWN,
    _Transition,
    RUNNING,
    IllegalTransition,
)


def test_transition():
    n: _FSM = _FSM("", "/0")
    transition = _Transition(UNKNOWN, RUNNING)
    n.add_transition(transition)

    change = next(n.transition(RUNNING))
    assert isinstance(change, _StateChange)
    assert change.transition == transition
    assert change.src == "/0"


def test_orphaned_illegal_transition():
    n: _FSM = _FSM("/", "0")
    with pytest.raises(IllegalTransition) as e:
        next(n.transition(RUNNING))
    assert e.value.transition == _Transition(UNKNOWN, RUNNING)
    assert e.value.node == n


def test_unhandled_illegal_transition_in_child():
    class _parent(_FSM):
        def child_transitioned(
            self, child: "_FSM", transition: Union[_Transition, IllegalTransition]
        ) -> Generator[_StateChange, None, None]:
            raise transition

    p = _parent("/", "0")
    n = _FSM(p.path, "0", p)
    with pytest.raises(IllegalTransition) as e:
        next(n.transition(RUNNING))
    assert e.value.transition == _Transition(UNKNOWN, RUNNING)
    assert e.value.node == n


def test_handled_illegal_transition_in_child():
    class _parent(_FSM):
        def child_transitioned(
            self, child: "_FSM", transition: Union[_Transition, IllegalTransition]
        ) -> Generator[_StateChange, None, None]:
            yield from ()

    p = _parent("/", "0")
    n = _FSM(p.path, "0", p)
    list(n.transition(RUNNING))


def test_child_transition_propagation():
    """Test that a transition in a child that triggers a transition in a
    parent, produces multiple changes."""

    class _parent(_FSM):
        def child_transitioned(
            self, child: "_FSM", transition: Union[_Transition, IllegalTransition]
        ) -> Generator[_StateChange, None, None]:
            yield from self.transition(RUNNING)

    transition = _Transition(UNKNOWN, RUNNING)

    p = _parent("/", "0")
    p.add_transition(transition)

    n: _FSM = _FSM(p.path, "0", p)
    n.add_transition(transition)

    changes = list(n.transition(RUNNING))
    assert len(changes) == 2
    assert sorted(changes)[0].src == "/0"
    assert sorted(changes)[1].src == "/0/0"


def test_multiple_child_transitions():
    """Test that multiple transitions in children trigger multiple
    child_transitioned in the parent."""
    i = 0

    class _parent(_FSM):
        def child_transitioned(
            self, child: "_FSM", transition: Union[_Transition, IllegalTransition]
        ) -> Generator[_StateChange, None, None]:
            nonlocal i
            i += 1
            yield from ()

    transition = _Transition(UNKNOWN, RUNNING)

    p = _parent("/", "0")

    n_0: _FSM = _FSM(p.path, "0", p)
    n_0.add_transition(transition)
    n_1: _FSM = _FSM(p.path, "1", p)
    n_1.add_transition(transition)

    changes = list(n_0.transition(RUNNING)) + list(n_1.transition(RUNNING))
    assert i == 2
    assert len(changes) == 2


@pytest.mark.parametrize("tgf", [(
    """Ensemble /0
    Realization /0/0
    """
)])
def test_tgf(tgf):
    ens = _Ensemble.from_ert_trivial_graph_format(tgf)
    print(ens.children)

# @pytest.mark.parametrize("tree,event,expected_partial", [
#     (, )
# ])
# def test_foo(tree, event, expected_partial):
#     pass