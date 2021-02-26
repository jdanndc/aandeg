from aandeg.util import aandeg_config

def test_aandeg_config():
    c = aandeg_config()
    assert(c is not None)
    assert(len(c.get_args()) == 5)
    t = c.get_args()
    assert(c.get_args()[0] is not None)
    assert(c.get_args()[1] is not None)
    assert(c.get_args()[2] is not None)
    assert(c.get_args()[3] is not None)
    assert(c.get_args()[4] is not None)
