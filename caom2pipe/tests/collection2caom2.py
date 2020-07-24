# this supports execution of
# test_execute_composable.py::test_meta_update_observation_direct


from caom2utils import fits2caom2


def to_caom2():
    import logging
    import sys
    logging.error(sys.argv)
    xml_fqn = '/usr/src/app/caom2pipe/caom2pipe/tests/test_obs_id/' \
              'test_obs_id.fits.xml'
    args = fits2caom2.get_gen_proc_arg_parser().parse_args()
    assert args is not None, 'expect args'
    assert args.in_obs_xml.name == xml_fqn, \
        f'wrong in value {args.in_obs_xml.name}'
    assert args.out_obs_xml == xml_fqn, f'wrong out value {args.out_obs_xml}'
    assert args.lineage == ['VLASS1.2.T07t14.J084202-123000.quicklook.v1/'
                            'ad:VLASS/VLASS1.2.ql.T07t14.J084202-123000.10.'
                            '2048.v1.I.iter1.image.pbcor.tt0.subim.fits',
                            'VLASS1.2.T07t14.J084202-123000.quicklook.v1/'
                            'ad:VLASS/VLASS1.2.ql.T07t14.J084202-123000.10.'
                            '2048.v1.I.iter1.image.pbcor.tt0.rms.subim.fits'], \
        f'wrong lineage value {args.lineage}'
    assert args.external_url is None, \
        f'wrong external urls value {args.external_url}'
