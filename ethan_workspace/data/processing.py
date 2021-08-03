import numpy as np
import pandas as pd

from sklearn.mixture import GaussianMixture
from sklearn.model_selection import GroupKFold
from sklearn.preprocessing import StandardScaler, OneHotEncoder


def mean_absolute_percentage_error(y_true, y_pred):
    return np.mean((y_pred - y_true) / y_true)


input_data = pd.read_csv('raw_data/all_raw_remnant_input.csv')
modeling_data = pd.read_csv('raw_data/all_raw_remnant_target.csv')

recurrent_categorical_features = ['directory_1',
                                  'directory_2',
                                  'directory_3',
                                  'is_skill_builder',
                                  'has_due_date',
                                  'assignment_completed']

recurrent_continuous_features = ['time_since_last_assignment_start',
                                 'session_count_raw',
                                 'session_count_normalized',
                                 'session_count_class_percentile',
                                 'day_count_raw',
                                 'day_count_normalized',
                                 'day_count_class_percentile',
                                 'completed_problem_count_raw',
                                 'completed_problem_count_normalized',
                                 'completed_problem_count_class_percentile',
                                 'median_ln_problem_time_on_task_raw',
                                 'median_ln_problem_time_on_task_normalized',
                                 'median_ln_problem_time_on_task_class_percentile',
                                 'median_ln_problem_first_response_time_raw',
                                 'median_ln_problem_first_response_time_normalized',
                                 'median_ln_problem_first_response_time_class_percentile',
                                 'average_problem_attempt_count',
                                 'average_problem_attempt_count_normalized',
                                 'average_problem_attempt_count_class_percentile',
                                 'average_problem_answer_first',
                                 'average_problem_answer_first_normalized',
                                 'average_problem_answer_first_class_percentile',
                                 'average_problem_correctness',
                                 'average_problem_correctness_normalized',
                                 'average_problem_correctness_class_percentile',
                                 'average_problem_hint_count',
                                 'average_problem_hint_count_normalized',
                                 'average_problem_hint_count_class_percentile',
                                 'average_problem_answer_given',
                                 'average_problem_answer_given_normalized',
                                 'average_problem_answer_given_class_percentile',
                                 'time_since_last_assignment_start_cluster']

prior_categorical_features = ['target_sequence',
                              'student_prior_completed_at_least_five_assignments']

prior_continuous_features = ['student_prior_assignments_started',
                             'student_prior_assignments_percent_completed',
                             'student_prior_median_ln_assignment_time_on_task',
                             'student_prior_average_problems_per_assignment',
                             'student_prior_median_ln_problem_time_on_task',
                             'student_prior_median_ln_problem_first_response_time',
                             'student_prior_average_problem_correctness',
                             'student_prior_average_problem_attempt_count',
                             'student_prior_average_problem_hint_count']

# Create additional features and target variables

# Inverse mastery speed
modeling_data['inverse_mastery_speed'] = modeling_data.apply(lambda x: 1 / x['problems_completed'] if x['assignment_completed'] else 0, axis=1)

# An explicit feature for which cluster the time_since_last_assignment_start falls into
clusters = 4
times = input_data['time_since_last_assignment_start'].values.reshape(-1, 1)
input_data['time_since_last_assignment_start_cluster'] = GaussianMixture(n_components=clusters).fit(times).predict(times)
recurrent_categorical_features.append('time_since_last_assignment_start_cluster')

# A feature for whether or not there is a folder path
input_data['custom_assignment'] = input_data['directory_1'].isna().astype(int)
recurrent_categorical_features.append('custom_assignment')

# A feature for whether or not there is any problem level data
input_data['no_problem_statistics'] = input_data['median_ln_problem_time_on_task_raw'].isna().astype(int)
recurrent_categorical_features.append('no_problem_statistics')

# Replace NaN categorical features
input_data[recurrent_categorical_features] = input_data[recurrent_categorical_features].fillna(-1)
modeling_data[prior_categorical_features] = modeling_data[prior_categorical_features].fillna(-1)

# Add the previous assignments to the training data
modeling_data['previous_assignments'] = None
for i, row in modeling_data.iterrows():
    row_input = input_data[(input_data['assignment_start_time'] < row['assignment_start_time']) & (
            input_data['student_id'] == row['student_id'])].sort_values('assignment_start_time')
    if len(row_input) > 0:
        modeling_data.at[i, 'previous_assignments'] = row_input
modeling_data = modeling_data[~modeling_data['previous_assignments'].isna()]


def process_recurrent_input(df_list, max_sequence_length, one_hot_encoder=None, normalizer=None):
    if one_hot_encoder is None:
        categorical_data = np.concatenate([df[recurrent_categorical_features].values for df in df_list])
        continuous_data = np.concatenate([df[recurrent_continuous_features].values for df in df_list])
        one_hot_encoder = OneHotEncoder(handle_unknown='ignore').fit(categorical_data)
        normalizer = StandardScaler().fit(continuous_data)
    processed_input = []
    for df in df_list:
        categorical_data = one_hot_encoder.transform(df[recurrent_categorical_features]).toarray()
        continuous_data = np.nan_to_num(normalizer.transform(df[recurrent_continuous_features]))
        combined_data = np.concatenate([categorical_data, continuous_data], axis=1)
        if combined_data.shape[0] >= max_sequence_length:
            resized_data = combined_data[-max_sequence_length:, :]
        else:
            resized_data = np.zeros((max_sequence_length, combined_data.shape[1]))
            resized_data[-combined_data.shape[0]:, :] = combined_data
        processed_input.append(resized_data)
    return np.stack(processed_input), one_hot_encoder, normalizer


def process_prior_input(df, one_hot_encoder=None, normalizer=None):
    if one_hot_encoder is None:
        categorical_data = df[prior_categorical_features].values
        continuous_data = df[prior_continuous_features].values
        one_hot_encoder = OneHotEncoder(handle_unknown='ignore').fit(categorical_data)
        normalizer = StandardScaler().fit(continuous_data)
    categorical_data = one_hot_encoder.transform(df[prior_categorical_features]).toarray()
    continuous_data = np.nan_to_num(normalizer.transform(df[prior_continuous_features]))
    combined_data = np.concatenate([categorical_data, continuous_data], axis=1)
    return combined_data, one_hot_encoder, normalizer


# Split the data into training and testing sets
# Normalize and one hot encode the data based on the input data

N_SPLITS = 5
MSL = 20
count = 0
for train_index, test_index in GroupKFold(N_SPLITS).split(modeling_data, groups=modeling_data['class_id']):
    # Create the data for training and testing
    recurrent_training_input, recurrent_input_one_hot_encoder, recurrent_input_normalizer = process_recurrent_input(modeling_data.iloc[train_index]['previous_assignments'].tolist(), MSL)
    prior_training_input, prior_input_one_hot_encoder, prior_input_normalizer = process_prior_input(modeling_data.iloc[train_index])
    completion_training_target = modeling_data.iloc[train_index][['assignment_completed']].values
    problems_training_target = modeling_data.iloc[train_index][['problems_completed']].values
    recurrent_testing_input, _, _ = process_recurrent_input(modeling_data.iloc[test_index]['previous_assignments'].tolist(), MSL, recurrent_input_one_hot_encoder, recurrent_input_normalizer)
    prior_testing_input, _, _ = process_prior_input(modeling_data.iloc[test_index], prior_input_one_hot_encoder, prior_input_normalizer)
    completion_testing_target = modeling_data.iloc[test_index][['assignment_completed']].values
    problems_testing_target = modeling_data.iloc[test_index][['problems_completed']].values

    np.save(f'processed_data_{MSL}/recurrent_training_input_{count}.npy', recurrent_training_input)
    np.save(f'processed_data_{MSL}/prior_training_input_{count}.npy', prior_training_input)
    np.save(f'processed_data_{MSL}/completion_training_target_{count}.npy', completion_training_target)
    np.save(f'processed_data_{MSL}/problems_training_target_{count}.npy', problems_training_target)
    np.save(f'processed_data_{MSL}/recurrent_testing_input_{count}.npy', recurrent_testing_input)
    np.save(f'processed_data_{MSL}/prior_testing_input_{count}.npy', prior_testing_input)
    np.save(f'processed_data_{MSL}/completion_testing_target_{count}.npy', completion_testing_target)
    np.save(f'processed_data_{MSL}/problems_testing_target_{count}.npy', problems_testing_target)

    count += 1
