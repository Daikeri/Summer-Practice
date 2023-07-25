import pandas as pd


def filter_by_version(data, version_range):
    return data[data['mysql_version'].str.contains(version_range, case=False) & ~data['Latency'].isnull()]


header_written = False

input_filename = f'C:/Users/Дмитрий/Desktop/Archive/UsersWithoutServers1.csv'
data = pd.read_csv(input_filename)

my_sql_5_6 = filter_by_version(data, 'MySQL 5.6.([1-9]\d|\d)')
my_sql_5_6.to_csv(f'C:/Users/Дмитрий/Desktop/DBMS/MySQL 5.6.csv', index=False, mode='a', header=not header_written)

my_sql_5_7 = filter_by_version(data, 'MySQL 5.7.([1-9]\d|\d)')
my_sql_5_7.to_csv(f'C:/Users/Дмитрий/Desktop/DBMS/MySQL 5.7.csv', index=False, mode='a', header=not header_written)

my_sql_8_0 = filter_by_version(data, 'MySQL 8.0.([1-9]\d|\d)')
my_sql_8_0.to_csv(f'C:/Users/Дмитрий/Desktop/DBMS/MySQL 8.0.csv', index=False, mode='a', header=not header_written)

maria_db_10_0 = filter_by_version(data, 'MariaDB 10\.0\.([1-9]\d|\d)')
maria_db_10_0.to_csv(f'C:/Users/Дмитрий/Desktop/DBMS/MariaDB 10.0.csv', index=False, mode='a', header=not header_written)

maria_db_10_2 = filter_by_version(data, 'MariaDB 10\.2\.([1-9]\d|\d)')
maria_db_10_2.to_csv(f'C:/Users/Дмитрий/Desktop/DBMS/MariaDB 10.2.csv', index=False, mode='a', header=not header_written)

maria_db_10_3 = filter_by_version(data, 'MariaDB 10\.([1-9]\d|[3-9])\.([1-9]\d|\d)')
maria_db_10_3.to_csv(f'C:/Users/Дмитрий/Desktop/DBMS/MariaDB 10.3.csv', index=False, mode='a', header=not header_written)

percona_5_6 = filter_by_version(data, 'Percona 5\.6\.([1-9]\d|\d)')
percona_5_6.to_csv(f'C:/Users/Дмитрий/Desktop/DBMS/Percona 5.6.csv', index=False, mode='a', header=not header_written)

percona_5_7 = filter_by_version(data, 'Percona 5\.7\.([1-9]\d|\d)')
percona_5_7.to_csv(f'C:/Users/Дмитрий/Desktop/DBMS/Percona 5.7.csv', index=False, mode='a', header=not header_written)

percona_8_0 = filter_by_version(data, 'Percona 8\.0\.([1-9]\d|\d)')
percona_8_0.to_csv(f'C:/Users/Дмитрий/Desktop/DBMS/Percona 8.0.csv', index=False, mode='a', header=not header_written)
