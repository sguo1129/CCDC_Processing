"""
Random Forest Classification training

This file should just be concerned with handling arrays of data
"""
import numpy as np
from sklearn.ensemble import RandomForestClassifier


start_time = '1999-01-01'
end_time = '2001-12-31'


def separate_fmask(fmask):
    """
    Generate arrays based on FMask values for stats generation

    FMask values are as noted:
    0 clear
    1 water
    2 shadow
    3 snow
    4 cloud
    255 fill

    :param fmask: numpy array
    :return: numpy array
    """
    ret = np.zeros(shape=((5,) + fmask.shape))

    ret[0][fmask < 255] = 1
    for i in range(1, len(ret)):
        ret[i][fmask == i] = 1

    return ret


def create_model():
    pass


def train_model(indep_arr, dep_arr, model_func, kwargs):
    """
    Train a model on the given data
    This assumes there is a fit method on the object returned by the model_func

    :param indep_arr: array of independent variables
    :param dep_arr: array of target values
    :param model_func: type of model to use
    :param kwargs: kwargs passed to the model_func
    :return: fitted model
    """
    model = model_func(**kwargs)

    return model.fit(indep_arr, dep_arr)
