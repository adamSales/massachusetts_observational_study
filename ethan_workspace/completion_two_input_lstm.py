import os
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score, r2_score, accuracy_score
import keras
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.compat.v1.keras.layers import CuDNNLSTM
from tensorflow.keras.layers import Input, Masking, LSTM, Dense, Dropout, Concatenate
from tensorflow.keras.callbacks import EarlyStopping

# Hide GPU from visible devices
tf.config.set_visible_devices([], 'GPU')
print(f'CUDA GPU AVAILABLE: {tf.test.is_gpu_available(cuda_only=True)}')

N_SPLITS = 5
DATA_DIR = 'data/processed_data'

completion_acc = []
completion_auc = []

for i in range(N_SPLITS):

    recurrent_training_input = np.load(f'{DATA_DIR}/recurrent_training_input_{i}.npy')
    prior_training_input = np.load(f'{DATA_DIR}/prior_training_input_{i}.npy')
    completion_training_target = np.load(f'{DATA_DIR}/completion_training_target_{i}.npy')
    recurrent_testing_input = np.load(f'{DATA_DIR}/recurrent_testing_input_{i}.npy')
    prior_testing_input = np.load(f'{DATA_DIR}/prior_testing_input_{i}.npy')
    completion_testing_target = np.load(f'{DATA_DIR}/completion_testing_target_{i}.npy')

    # Clear session so models don't pile up
    keras.backend.clear_session()

    # Create model
    recurrent_input_layer = Input(shape=recurrent_training_input[0].shape, name='recurrent')
    recurrent_model = Masking(mask_value=0.0)(recurrent_input_layer)
    recurrent_model = LSTM(units=64, return_sequences=False, activation='tanh', dropout=0.5, recurrent_dropout=0.5)(recurrent_model)
    prior_input_layer = Input(shape=prior_training_input[0].shape, name='prior')
    prior_model = Dense(units=64, activation='tanh')(prior_input_layer)
    prior_model = Dropout(rate=0.5)(prior_model)
    model = Concatenate()([recurrent_model, prior_model])
    completion_output_layer = Dense(units=1, activation='sigmoid', name='completion')(model)
    
    combined_model = Model([recurrent_input_layer, prior_input_layer], completion_output_layer)
    combined_model.compile(optimizer='adam', loss='binary_crossentropy')

    # Train model
    es = [EarlyStopping(monitor='val_loss', patience=10, min_delta=0, restore_best_weights=True)]
    combined_model.fit(x={'recurrent': recurrent_training_input, 'prior': prior_training_input},
                       y=completion_training_target,
                       epochs=1000,
                       validation_split=0.25,
                       callbacks=es,
                       verbose=1)

    # Measure the quality of the model
    completion_testing_output = combined_model.predict({'recurrent': recurrent_testing_input, 'prior': prior_testing_input})
    completion_auc.append(roc_auc_score(completion_testing_target, completion_testing_output))
    completion_acc.append(accuracy_score(completion_testing_target, completion_testing_output > 0.5))

print(f'completion auc: {np.mean(completion_auc)}')
print(f'completion accuracy: {np.mean(completion_acc)}')

