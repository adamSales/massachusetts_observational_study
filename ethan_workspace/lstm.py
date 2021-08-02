import os
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score, r2_score, accuracy_score
from sklearn.tree import DecisionTreeClassifier
import keras
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.compat.v1.keras.layers import CuDNNLSTM
from tensorflow.keras.layers import Input, Masking, LSTM, Activation, Dense, Dropout, BatchNormalization
from tensorflow.keras.callbacks import EarlyStopping

# Hide GPU from visible devices
# tf.config.set_visible_devices([], 'GPU')
print(f'CUDA GPU AVAILABLE: {tf.test.is_gpu_available(cuda_only=True)}')


def mean_absolute_percentage_error(y_true, y_pred):
    return np.mean((y_pred - y_true) / y_true)


N_SPLITS = 5
DATA_DIR = 'data/processed_data_20'

completion_acc = []
completion_auc = []
problem_count_mpe = []
problem_count_r2 = []

for i in range(N_SPLITS):

    training_input = np.load(f'{DATA_DIR}/training_input_{i}.npy')
    completion_training_target = np.load(f'{DATA_DIR}/completion_training_target_{i}.npy')
    problem_count_training_target = np.load(f'{DATA_DIR}/problems_training_target_{i}.npy')
    testing_input = np.load(f'{DATA_DIR}/testing_input_{i}.npy')
    completion_testing_target = np.load(f'{DATA_DIR}/completion_testing_target_{i}.npy')
    problem_count_testing_target = np.load(f'{DATA_DIR}/problems_testing_target_{i}.npy')
    
    print(training_input.shape, testing_input.shape)
    
    # Clear session so models don't pile up
    keras.backend.clear_session()

    # Construct the neural network
    input_layer = Input(shape=training_input[0].shape)
    model = Masking(mask_value=0.0)(input_layer) # BEST SO FAR
    # model = LSTM(units=64, return_sequences=False, activation='tanh', dropout=0.5, recurrent_dropout=0.5)(model) # BEST SO FAR
    model = LSTM(units=128, return_sequences=False, activation='tanh', dropout=0.5, recurrent_dropout=0.5)(model)
    # model = CuDNNLSTM(units=128, return_sequences=True)(input_layer)
    # model = BatchNormalization()(model)
    # model = CuDNNLSTM(units=64, return_sequences=True)(input_layer)
    # model = BatchNormalization()(model)
    # Just this one below had best performance of all the cudNN configurations
    # model = CuDNNLSTM(units=32, return_sequences=False)(input_layer)
    # model = BatchNormalization()(model)
    completion_output_layer = Dense(units=1, activation='sigmoid', name='completion')(model)
    problem_count_output_layer = Dense(units=1, activation='linear', name='problem_count')(model)
    combined_model = Model(input_layer, [completion_output_layer, problem_count_output_layer])
    combined_model.compile(optimizer='adam', loss={'completion': 'binary_crossentropy', 'problem_count': 'mse'})

    # Train the neural network
    es = [EarlyStopping(monitor='val_loss', patience=10, min_delta=0, restore_best_weights=True)]
    weights = {'completion': np.ones_like(completion_training_target), 'problem_count': completion_training_target}
    combined_model.fit(x=training_input,
                       y={'completion': completion_training_target, 'problem_count': problem_count_training_target},
                       epochs=1000,
                       validation_split=0.25,
                       callbacks=es,
                       sample_weight=weights,
                       verbose=1)

    # Measure the quality of the model
    completion_testing_output, problem_count_testing_output = combined_model.predict(testing_input)
    completion_auc.append(roc_auc_score(completion_testing_target, completion_testing_output))
    completion_acc.append(accuracy_score(completion_testing_target, completion_testing_output > 0.5))

    problem_count_testing_output = problem_count_testing_output[completion_testing_target.flatten() == 1]
    problem_count_testing_target = problem_count_testing_target[completion_testing_target.flatten() == 1]
    problem_count_mpe.append(mean_absolute_percentage_error(problem_count_testing_target, problem_count_testing_output))
    problem_count_r2.append(r2_score(problem_count_testing_target, problem_count_testing_output))

print(f'completion auc: {np.mean(completion_auc)}')
print(f'completion accuracy: {np.mean(completion_acc)}')
print(f'problems to mastery mean absolute percent error: {np.mean(problem_count_mpe)}')
print(f'problems to mastery r^2: {np.mean(problem_count_r2)}')
