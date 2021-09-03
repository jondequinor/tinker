from typing import Any, Callable, Dict, List, Optional, Set, Union


class IllegalTransition(Exception):
    pass


class _Transition:
    def __init__(
        self,
        from_: Union["State", "StateMachine"],
        to: Union["State", "StateMachine"],
        trigger: str,
    ) -> None:
        self.trigger: str = trigger
        self.from_: Union["State", "StateMachine"] = from_
        self.to: Union["State", "StateMachine"] = to


class State:
    def __init__(self, name: str) -> None:
        self._name: str = name

    def __repr__(self) -> str:
        return self._name


class StateMachine:
    def __init__(
        self, name: str, initial_state: Union["State", "StateMachine"]
    ) -> None:
        self._children: Set["StateMachine"] = set()
        self._name: str = name
        self._transitions: List = []
        self._state: Union["State", "StateMachine"] = initial_state

    def __repr__(self) -> str:
        return self._name

    def handle(self, trigger):
        trigger_found = False
        for transition in self._transitions:
            if transition.trigger == trigger:
                trigger_found = True
                if transition.from_ == self._state:
                    if self._state != transition.to:
                        print(
                            f"transition {self._name}: {transition.from_} -> {transition.to}"
                        )
                    self._state = transition.to
                    break
        else:
            if trigger_found:
                raise IllegalTransition(
                    f"trigger {trigger} has no transition for {self._name} in state {self._state}"
                )

        for child in self._children:
            child.handle(trigger)

    def add_transition(
        self,
        trigger: str,
        from_: Union["State", "StateMachine"],
        to: Union["State", "StateMachine"],
    ) -> "StateMachine":
        for transition in self._transitions:
            if trigger == transition.trigger and to != transition.to:
                raise RuntimeError(
                    f"trigger {trigger} would ambiguously transition to both {to} and {transition.to} in {self._name}"
                )
        self._transitions.append(_Transition(from_, to, trigger))
        return self

    def add_substate(self, *states: "StateMachine") -> "StateMachine":
        for state in states:
            self._children.add(state)
        return self


job_0_pending = State("job_0_pending")
job_0_running = State("job_0_running")
job_0_success = State("job_0_success")

job_0 = (
    StateMachine("job_0", job_0_pending)
    .add_transition("JOB_0_START", job_0_pending, job_0_running)
    .add_transition("JOB_0_RUNNING", job_0_pending, job_0_running)
    .add_transition("JOB_0_RUNNING", job_0_running, job_0_running)
    .add_transition("JOB_0_SUCCESS", job_0_running, job_0_success)
)

job_1_pending = State("job_1_pending")
job_1_running = State("job_1_running")
job_1_success = State("job_1_success")

job_1 = (
    StateMachine("job_1", job_1_pending)
    .add_transition("JOB_1_START", job_1_pending, job_1_running)
    .add_transition("JOB_1_RUNNING", job_1_pending, job_1_running)
    .add_transition("JOB_1_RUNNING", job_1_running, job_1_running)
    .add_transition("JOB_1_SUCCESS", job_1_running, job_1_success)
)

step_0_pending = State("step_0_pending")
step_0_running = StateMachine("step_0_running", job_0)
step_0_success = State("step_0_success")
step_0_running.add_substate(job_0, job_1)

step_0 = StateMachine("step_0", step_0_pending)
step_0.add_substate(step_0_running)

step_0.add_transition("STEP_0_START", step_0_pending, step_0_pending).add_transition(
    "STEP_0_RUNNING", step_0_pending, step_0_running
).add_transition("STEP_0_RUNNING", step_0_running, step_0_running).add_transition(
    "JOB_1_SUCCESS", step_0_running, step_0_success
).add_transition(
    "STEP_1_SUCCESS", step_0_running, step_0_success
).add_transition(
    "STEP_1_SUCCESS", step_0_success, step_0_success
).add_substate(
    step_0_running
)

step_0.handle("STEP_0_START")
step_0.handle("STEP_0_RUNNING")
step_0.handle("JOB_0_START")
step_0.handle("JOB_0_RUNNING")
step_0.handle("JOB_0_SUCCESS")
step_0.handle("STEP_0_RUNNING")
step_0.handle("JOB_1_START")
step_0.handle("JOB_1_RUNNING")
step_0.handle("JOB_1_SUCCESS")
step_0.handle("STEP_1_SUCCESS")
