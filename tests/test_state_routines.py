from python_project.backbone.community_routines import StateRoutines

from tests.mocking.community import FakeRoutines


class FakeStateRounties(StateRoutines, FakeRoutines):
    pass


def test_sign_verify_state():
    f = FakeStateRounties()

    test_blob = b"state_blob"

    state_vote = f.sign_state(test_blob)
    assert f.verify_state_vote(state_vote)
