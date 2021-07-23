# if you have a possoint distribution apply an anscombe transformation to make it normal
import os
import numpy as np
import pandas as pd

from sklearn.metrics import roc_auc_score, r2_score, accuracy_score
from sklearn.tree import DecisionTreeClassifier

import tensorflow as tf

# Hide GPU from visible devices
#tf.config.set_visible_devices([], 'GPU')

import keras
from keras import backend as K
from tensorflow.keras import Sequential
from tensorflow.keras.layers import Masking, LSTM, TimeDistributed, Dense, Dropout
from tensorflow.compat.v1.keras.layers import CuDNNLSTM
from tensorflow.keras.callbacks import EarlyStopping

print(f'CUDA GPU AVAILABLE: {tf.test.is_gpu_available(cuda_only=True)}')

def mean_absolute_percentage_error(y_true, y_pred):
    return np.mean((y_pred - y_true) / y_true)
    
N_SPLITS = 5
DATA_DIR = 'processed_data_20'

completion_acc = []
completion_auc = []
problems_mpe = []
problems_r2 = []
split_point = []

for i in range(N_SPLITS):

    training_input = np.load(f'{DATA_DIR}/training_input_{i}.npy')
    completion_training_target = np.load(f'{DATA_DIR}/completion_training_target_{i}.npy')
    problems_training_target = np.load(f'{DATA_DIR}/problems_training_target_{i}.npy')
    testing_input = np.load(f'{DATA_DIR}/testing_input_{i}.npy')
    completion_testing_target = np.load(f'{DATA_DIR}/completion_testing_target_{i}.npy')
    problems_testing_target = np.load(f'{DATA_DIR}/problems_testing_target_{i}.npy') 
    
    print(training_input.shape, testing_input.shape)
    
    # Clear session so models don't pile up
    keras.backend.clear_session()
    
    # Train model to predict completion
    completion_model = Sequential()
    completion_model.add(CuDNNLSTM(32, return_sequences=False, input_shape=(training_input[0].shape)))
    completion_model.add(Dropout(0.5))
    completion_model.add(Dense(completion_training_target.shape[1], activation='sigmoid'))
    completion_model.compile(optimizer='adam', loss='binary_crossentropy')
    
    es = [EarlyStopping(monitor='val_loss', patience=10, min_delta=0, restore_best_weights=True)]
    completion_model.fit(training_input, completion_training_target, epochs=1000, validation_split=0.2, callbacks=es, verbose=1)
    
    completion_training_output = completion_model.predict(training_input)
    dtc = DecisionTreeClassifier(criterion='gini', splitter='best', max_depth=1).fit(completion_training_output, completion_training_target)
    split_point.append(dtc.tree_.threshold[0])
    
    # Train model to predict problems to mastery when assignment is completed
    problems_training_input = training_input[completion_training_target.flatten() == 1]
    problems_training_target = problems_training_target[completion_training_target.flatten() == 1]

    problems_model = Sequential()
    problems_model.add(CuDNNLSTM(32, return_sequences=False, input_shape=(training_input[0].shape)))
    problems_model.add(Dropout(0.5))
    problems_model.add(Dense(problems_training_target.shape[1], activation='linear'))
    problems_model.compile(optimizer='adam', loss='mse')

    es = [EarlyStopping(monitor='val_loss', patience=10, min_delta=0, restore_best_weights=True)]
    problems_model.fit(problems_training_input, problems_training_target, epochs=1000, validation_split=0.2, callbacks=es, verbose=1)
    
    # Measure the quality of the 
    completion_testing_output = completion_model.predict(testing_input)
    completion_auc.append(roc_auc_score(completion_testing_target, completion_testing_output))
    completion_acc.append(accuracy_score(completion_testing_target, completion_testing_output > split_point[-1]))

    problems_testing_output = problems_model.predict(testing_input[completion_testing_output.flatten() > split_point[-1]])
    problems_testing_target = problems_testing_target[completion_testing_output.flatten() > split_point[-1]]
    problems_mpe.append(mean_absolute_percentage_error(problems_testing_target, problems_testing_output))
    problems_r2.append(r2_score(problems_testing_target, problems_testing_output))

print(f'split points: {split_point}, mean split point: {np.mean(split_point)}')
print(f'completion auc: {np.mean(completion_auc)}, completion accuracy: {np.mean(completion_acc)}')
print(f'problems to mastery mean percent error: {np.mean(problems_mpe)}, problems to mastery r^2: {np.mean(problems_r2)}')
print(f'target majority class frequency: {np.sum(completion_testing_target) / len(completion_testing_target)}')
print(f'output majority class frequency: {np.sum(completion_testing_output) / len(completion_testing_output)}')
