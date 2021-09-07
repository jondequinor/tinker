import json
from collections import deque
from typing import Generator, Union

import pytest

from atrack.atrack import (
    _FSM,
    FAILURE,
    RUNNING,
    SUCCESS,
    UNKNOWN,
    IllegalTransition,
    _Encoder,
    _Ensemble,
    _Event,
    _Realization,
    _State,
    _StateChange,
    _Transition,
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
    e = next(n.transition(RUNNING))
    assert e.transition == _Transition(UNKNOWN, RUNNING)
    assert e.node == n


def test_handled_illegal_transition():
    tgf = """Ensemble /0
    Realization /0/0
    Step /0/0/0
    Job /0/0/0/0
    """
    ens = _Ensemble.from_ert_trivial_graph_format(tgf)

    # UNKNOWN -> SUCCESS is handled by the step
    gen = ens.dispatch(_Event("/0/0/0/0", "JOB_SUCCESS"))

    illegal_transition = next(gen)

    assert isinstance(illegal_transition, IllegalTransition)
    assert illegal_transition.node == ens.children[0].children[0].children[0]
    assert illegal_transition.transition.from_state == UNKNOWN
    assert illegal_transition.transition.to_state == SUCCESS

    change = next(gen)
    assert isinstance(change, _StateChange)
    assert change.node == ens.children[0].children[0].children[0]
    assert change.transition.from_state == UNKNOWN
    assert change.transition.to_state == SUCCESS

    # will cause a step to transition illegally
    # TODO: maybe just handle this in the step

    illegal_transition = next(gen)
    assert isinstance(illegal_transition, IllegalTransition)
    assert illegal_transition.node == ens.children[0].children[0]
    assert illegal_transition.transition.from_state == UNKNOWN
    assert illegal_transition.transition.to_state == SUCCESS

    with pytest.raises(StopIteration):
        next(gen)


def test_unhandled_illegal_transition():
    tgf = """Ensemble /0
    Realization /0/0
    Step /0/0/0
    Job /0/0/0/0
    """
    ens = _Ensemble.from_ert_trivial_graph_format(tgf)

    # move job to RUNNING
    deque(ens.dispatch(_Event("/0/0/0/0", "JOB_STARTED")), maxlen=0)
    deque(ens.dispatch(_Event("/0/0/0/0", "JOB_SUCCESS")), maxlen=0)

    # SUCCESS -> FAILURE is not handled by anyone
    gen = ens.dispatch(_Event("/0/0/0/0", "JOB_FAILURE"))

    illegal_transition = next(gen)
    assert isinstance(illegal_transition, IllegalTransition)
    assert illegal_transition.node == ens.children[0].children[0].children[0]
    assert illegal_transition.transition.from_state == SUCCESS
    assert illegal_transition.transition.to_state == FAILURE

    with pytest.raises(IllegalTransition):
        next(gen)


def test_cascading_changes():
    # test that completion of job, completes step, …, completes ens
    tgf = """Ensemble /0
    Realization /0/0
    Step /0/0/0
    Job /0/0/0/0
    """
    ens = _Ensemble.from_ert_trivial_graph_format(tgf)
    all_changes = list(ens.dispatch(_Event("/0/0/0/0", "JOB_STARTED")))
    all_changes += list(ens.dispatch(_Event("/0/0/0/0", "JOB_SUCCESS")))
    partial = json.dumps(
        _StateChange.changeset_to_partial(all_changes),
        sort_keys=True,
        indent=4,
        cls=_Encoder,
    )
    assert (
        partial
        == """{
    "reals": {
        "0": {
            "status": "SUCCESS",
            "steps": {
                "0": {
                    "jobs": {
                        "0": {
                            "status": "SUCCESS"
                        }
                    },
                    "status": "SUCCESS"
                }
            }
        }
    },
    "status": "SUCCESS"
}"""
    )


@pytest.mark.parametrize(
    "a,b,equal",
    [
        (_State("foo"), _State("bar"), False),
        (_State("foo"), _State("foo"), True),
        (_State("bar"), _State("foo"), False),
        (_State("foo", {}), _State("foo", {}), True),
        (_State("bar", {}), _State("foo", {}), False),
        (_State("foo", {"a": 0}), _State("foo", {"a": 0}), True),
        (_State("foo", {}), _State("foo", {"a": 0}), False),
        (_State("foo", {"a": 0}), _State("foo", {}), False),
    ],
)
def test_state_equality(a, b, equal):
    assert (a == b) == equal


def test_equal_states_yield_no_change():
    tgf = """Ensemble /0
    Realization /0/0
    Step /0/0/0
    """
    ens = _Ensemble.from_ert_trivial_graph_format(tgf)
    gen = ens.dispatch(_Event("/0", "ENS_STARTED"))
    ens_running = next(gen)

    assert ens_running.node == ens
    assert ens_running.transition.from_state == UNKNOWN
    assert ens_running.transition.to_state == RUNNING

    with pytest.raises(StopIteration):
        next(gen)

    for change in ens.dispatch(_Event("/0/0/0", "STEP_STARTED")):
        assert (
            change.transition.from_state != change.transition.to_state
        ), "transitioned to same state"


def test_equal_states_but_with_data_changes_yields_change():
    tgf = """Ensemble /0
    Realization /0/0
    Step /0/0/0
    """
    ens = _Ensemble.from_ert_trivial_graph_format(tgf)
    list(ens.dispatch(_Event("/0/0/0", "STEP_STARTED")))

    gen = ens.dispatch(_Event("/0", "ENS_STARTED", {"foo": "bar"}))
    ens_running = next(gen)

    assert ens_running.node == ens
    assert ens_running.transition.from_state == RUNNING
    assert ens_running.transition.to_state == RUNNING.with_data({"foo": "bar"})

    with pytest.raises(StopIteration):
        next(gen)


@pytest.mark.parametrize(
    "tgf",
    [
        (
            """Ensemble /0
    Realization /0/0
    """
        )
    ],
)
def test_tgf(tgf):
    ens = _Ensemble.from_ert_trivial_graph_format(tgf)
    assert isinstance(ens.children[0], _Realization)
