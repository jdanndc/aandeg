from aandeg.util.config import Config


def test_aandeg_config():
    c = Config()
    assert(c is not None)
    assert(len(c.get_args()) == 5)
    assert(c.get_args()[0] is not None)
    assert(c.get_args()[1] is not None)
    assert(c.get_args()[2] is not None)
    assert(c.get_args()[3] is not None)
    assert(c.get_args()[4] is not None)
