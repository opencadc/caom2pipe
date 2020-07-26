# this supports execution of
# test_execute_composable.py


from caom2utils import fits2caom2


def to_caom2():
    args = fits2caom2.get_gen_proc_arg_parser().parse_args()
    assert args is not None, 'expect args'
