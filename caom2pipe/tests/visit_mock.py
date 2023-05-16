from caom2 import Algorithm, SimpleObservation


def visit(observation, **kwargs):
    if observation is None:
        observation = SimpleObservation(collection='OMM', observation_id='test_obs_id', algorithm=Algorithm(name='exposure'))
    return observation

