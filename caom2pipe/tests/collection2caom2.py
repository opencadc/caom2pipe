# this supports execution of test_execute_composable.py


from caom2utils import fits2caom2


def to_caom2_with_client(ingore):
    args = fits2caom2.get_no_visit_gen_proc_arg_parser().parse_args()
    assert args is not None, 'expect args'
