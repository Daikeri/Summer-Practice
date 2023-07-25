import csv
import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
from knobs import DBMSVersion


def read_data(filename):
    dbms_parameters = DBMSVersion.get_knobs(os.path.basename(filename))
    combined_matrix = [dbms_parameters]
    latency_list = []

    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            latency_list.append(row["Latency"])
            combined_matrix.append(row["result"].split(';'))
    return latency_list, combined_matrix


def get_column(container, index):
    acc = []
    for row in container:
        acc.append(row[index])
    return acc


def separate_matrices(combined_matrix, latency_list):
    scalar_matrix = {'latency': latency_list}
    categorical_matrix = {'latency': latency_list}

    for val in range(len(combined_matrix[0])):
        future_dict = get_column(combined_matrix, val)
        if combined_matrix[1][val].isdigit():
            scalar_matrix[future_dict[0]] = future_dict[1:]
        else:
            categorical_matrix[future_dict[0]] = future_dict[1:]
    return scalar_matrix, categorical_matrix


def remove_uninformative_keys(scalar_matrix):
    keys_to_remove = []
    for key, values in scalar_matrix.items():
        if all(value == "null" for value in values):
            keys_to_remove.append(key)
        else:
            numeric_values = [int(value) for value in values if value.isdigit()]
            if numeric_values:
                mean_value = sum(numeric_values) / len(numeric_values)
                scalar_matrix[key] = [str(mean_value) if value == "null" else value for value in values]

    for key in keys_to_remove:
        del scalar_matrix[key]

    return {key: value for key, value in scalar_matrix.items() if len(set(value)) > 1}


def plot_with_linear_regression(scalar_matrix, predictors, version):
    df = pd.DataFrame(scalar_matrix)

    correlations = []
    for predictor in predictors:
        df[predictor] = pd.to_numeric(df[predictor], errors='coerce')
        df['latency'] = pd.to_numeric(df['latency'], errors='coerce')

        valid_df = df.dropna(subset=[predictor, 'latency'])

        if len(valid_df) > 0:
            correlation, _ = pearsonr(valid_df[predictor], valid_df['latency'])

            if not np.isnan(correlation):
                correlations.append((predictor, correlation))

    sorted_predictors = sorted(correlations, key=lambda x: abs(x[1]), reverse=True)[:8]

    num_plots = len(sorted_predictors)
    num_cols = 4 if num_plots > 4 else num_plots
    num_rows = (num_plots + num_cols - 1) // num_cols

    colors = sns.color_palette('husl', n_colors=num_plots)

    fig, axes = plt.subplots(num_rows, num_cols, figsize=(15, 5 * num_rows))
    for i, (predictor, correlation) in enumerate(sorted_predictors):
        row = i // num_cols
        col = i % num_cols
        sns.regplot(x=predictor, y='latency', data=df, ax=axes[row, col], scatter_kws={'s': 6}, color=colors[i])
        axes[row, col].set_title(f'(Corr: {correlation:.2f})', fontsize=10)

    plt.suptitle(f'8 параметров с наиболее выраженной корреляцией {version[:-4]}', fontsize=14)

    if num_plots < num_cols:
        for col in range(num_plots, num_cols):
            axes[num_rows - 1, col].set_axis_off()

    plt.tight_layout()
    plt.show()


def plot_simple(scalar_matrix, predictors, version):
    correlations = []
    valid_predictors = []
    for predictor in predictors:
        scalar_matrix[predictor] = list(map(float, scalar_matrix[predictor]))
        scalar_matrix['latency'] = list(map(float, scalar_matrix['latency']))

        correlation, _ = pearsonr(scalar_matrix[predictor], scalar_matrix['latency'])

        if not np.isnan(correlation):
            correlations.append(correlation)
            valid_predictors.append(predictor)

    plt.figure(figsize=(12, 9))

    for i, predictor in enumerate(valid_predictors):
        plt.scatter(scalar_matrix[predictor], scalar_matrix['latency'], s=3,
                    label=f'{predictor} (Corr: {correlations[i]:.2f})')

    plt.xlabel('Предикторы')
    plt.ylabel('Latency')
    plt.title(f"Диаграмма рассеивания. Версия СУБД: {version[:-4]}. Объем выборки: {len(scalar_matrix['latency'])} записей.")

    plt.legend(loc='upper right')
    plt.grid()
    plt.show()


def iterate_files_in_directory(source_path):
    for filename in os.listdir(source_path):
        if os.path.isfile(os.path.join(source_path, filename)):
            yield filename


def main(directory_path):
    for filename in iterate_files_in_directory(directory_path):
        latency_list, combined_matrix = read_data(f'{directory_path}\\{filename}')
        scalar_matrix, categorical_matrix = separate_matrices(combined_matrix, latency_list)

        scalar_matrix = remove_uninformative_keys(scalar_matrix)

        predictors = [key for key in scalar_matrix if key != 'latency']
        plot_simple(scalar_matrix, predictors, filename)
        plot_with_linear_regression(scalar_matrix, predictors, filename)


main('C:\Python_Project\SummerPractice\processed_tables')
