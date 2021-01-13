from caom2utils import fits2caom2


def update(observation, **kwargs):
    # if something breaks during the execution of an 'update', that call
    # should return None
    return None


def to_caom2():
    bp = fits2caom2.ObsBlueprint()
    bp.configure_time_axis(1)
    bp.set('Plane.dataProductType', 'image')
    bp.set('Plane.calibrationLevel', 1)
    blueprints = {'ad:TEST/test_obs_id.fits.gz': bp}
    args = fits2caom2.get_gen_proc_arg_parser().parse_args()
    return fits2caom2.gen_proc(args, blueprints)
