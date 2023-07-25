import pandas as pd
import os

csv_names = ['MySQL 5.6', 'MySQL 5.7', 'MySQL 8.0', 'MariaDB 10.0', 'MariaDB 10.2',
             'MariaDB 10.3', 'Percona 5.6', 'Percona 5.7', 'Percona 8.0']

for name in csv_names:
    input_filename = f'C:/Users/Дмитрий/Desktop/DBMS/{name}.csv'
    data = pd.read_csv(input_filename, sep=',')

    def extract_last_two_nodes(url):
        path = url.split('/')[-2:]
        return '/'.join(path)

    data['metric_file'] = data['metric_file'].apply(extract_last_two_nodes)

    data.to_csv(input_filename, index=False)
