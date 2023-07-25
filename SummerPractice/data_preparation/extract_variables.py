import pandas as pd
import json
import os
import re

knobs_for_5_6 = [
    'innodb_adaptive_flushing_lwm',
    'innodb_adaptive_hash_index',
    'innodb_adaptive_max_sleep_delay',
    'innodb_buffer_pool_instances',
    'innodb_buffer_pool_size',
    'innodb_change_buffering',
    'innodb_io_capacity',
    'innodb_log_file_size',
    'innodb_max_dirty_pages_pct',
    'innodb_sync_array_size',
    'innodb_thread_concurrency',
    'max_heap_table_size',
    'thread_cache_size',
    'tmp_table_size',
    'binlog_cache_size',
    'binlog_max_flush_queue_time',
    'binlog_stmt_cache_size',
    'eq_range_index_dive_limit',
    'host_cache_size',
    'innodb_adaptive_flushing',
    'innodb_autoextend_increment',
    'innodb_buffer_pool_dump_now',
    'innodb_buffer_pool_load_at_startup',
    'innodb_buffer_pool_load_now',
    'innodb_change_buffer_max_size',
    'innodb_commit_concurrency',
    'innodb_compression_failure_threshold_pct',
    'innodb_compression_level',
    'innodb_compression_pad_pct_max',
    'innodb_concurrency_tickets',
    'innodb_flush_log_at_timeout',
    'innodb_flush_neighbors',
    'innodb_flushing_avg_loops',
    'innodb_ft_cache_size',
    'innodb_ft_result_cache_limit',
    'innodb_ft_sort_pll_degree',
    'innodb_io_capacity_max',
    'innodb_lock_wait_timeout',
    'innodb_log_buffer_size',
    'innodb_lru_scan_depth',
    'innodb_max_purge_lag',
    'innodb_max_purge_lag_delay',
    'innodb_old_blocks_pct',
    'innodb_old_blocks_time',
    'innodb_online_alter_log_max_size',
    'innodb_page_size',
    'innodb_purge_batch_size',
    'innodb_purge_threads',
    'innodb_random_read_ahead',
    'innodb_read_ahead_threshold',
    'innodb_read_io_threads',
    'innodb_replication_delay',
    'innodb_rollback_segments',
    'innodb_sort_buffer_size',
    'innodb_spin_wait_delay',
    'innodb_sync_spin_loops',
    'innodb_thread_sleep_delay',
    'innodb_use_native_aio',
    'innodb_write_io_threads',
    'join_buffer_size',
    'lock_wait_timeout',
    'max_binlog_cache_size',
    'max_binlog_size',
    'max_binlog_stmt_cache_size',
    'max_delayed_threads',
    'max_insert_delayed_threads',
    'max_join_size',
    'max_length_for_sort_data',
    'max_seeks_for_key',
    'max_sort_length',
    'max_sp_recursion_depth',
    'max_tmp_tables',
    'max_write_lock_count',
    'metadata_locks_cache_size',
    'optimizer_prune_level',
    'optimizer_search_depth',
    'preload_buffer_size',
    'query_alloc_block_size',
    'query_cache_limit',
    'query_cache_min_res_unit',
    'query_cache_size',
    'query_cache_type',
    'query_cache_wlock_invalidate',
    'query_prealloc_size',
    'range_alloc_block_size',
    'read_buffer_size',
    'read_rnd_buffer_size',
    'slave_checkpoint_group',
    'slave_checkpoint_period',
    'slave_parallel_workers',
    'slave_pending_jobs_size_max',
    'sort_buffer_size',
    'stored_program_cache',
    'table_definition_cache',
    'table_open_cache',
    'table_open_cache_instances',
    'thread_stack',
    'timed_mutexes',
    'transaction_alloc_block_size',
    'transaction_prealloc_size'
]

knobs_for_5_7 = [
    'innodb_adaptive_flushing_lwm',
    'innodb_adaptive_hash_index',
    'innodb_adaptive_max_sleep_delay',
    'innodb_buffer_pool_instances',
    'innodb_buffer_pool_size',
    'innodb_change_buffering',
    'innodb_io_capacity',
    'innodb_log_file_size',
    'innodb_max_dirty_pages_pct',
    'innodb_max_dirty_pages_pct_lwm',
    'innodb_sync_array_size',
    'innodb_thread_concurrency',
    'max_heap_table_size',
    'thread_cache_size',
    'tmp_table_size'

    'binlog_cache_size',
    'binlog_max_flush_queue_time',
    'binlog_stmt_cache_size',
    'eq_range_index_dive_limit',
    'host_cache_size',
    'innodb_adaptive_flushing',
    'innodb_autoextend_increment',
    'innodb_buffer_pool_dump_now',
    'innodb_buffer_pool_load_at_startup',
    'innodb_buffer_pool_load_now',
    'innodb_change_buffer_max_size',
    'innodb_commit_concurrency',
    'innodb_compression_failure_threshold_pct',
    'innodb_compression_level',
    'innodb_compression_pad_pct_max',
    'innodb_concurrency_tickets',
    'innodb_flush_log_at_timeout',
    'innodb_flush_neighbors',
    'innodb_flushing_avg_loops',
    'innodb_ft_cache_size',
    'innodb_ft_result_cache_limit',
    'innodb_ft_sort_pll_degree',
    'innodb_io_capacity_max',
    'innodb_lock_wait_timeout',
    'innodb_log_buffer_size',
    'innodb_lru_scan_depth',
    'innodb_max_purge_lag',
    'innodb_max_purge_lag_delay',
    'innodb_old_blocks_pct',
    'innodb_old_blocks_time',
    'innodb_online_alter_log_max_size',
    'innodb_page_size',
    'innodb_purge_batch_size',
    'innodb_purge_threads',
    'innodb_random_read_ahead',
    'innodb_read_ahead_threshold',
    'innodb_read_io_threads',
    'innodb_replication_delay',
    'innodb_rollback_segments',
    'innodb_sort_buffer_size',
    'innodb_spin_wait_delay',
    'innodb_sync_spin_loops',
    'innodb_thread_sleep_delay',
    'innodb_use_native_aio',
    'innodb_write_io_threads',
    'join_buffer_size',
    'lock_wait_timeout',
    'max_binlog_cache_size',
    'max_binlog_size',
    'max_binlog_stmt_cache_size',
    'max_delayed_threads',
    'max_insert_delayed_threads',
    'max_join_size',
    'max_length_for_sort_data',
    'max_seeks_for_key',
    'max_sort_length',
    'max_sp_recursion_depth',
    'max_tmp_tables',
    'max_write_lock_count',
    'metadata_locks_cache_size',
    'optimizer_prune_level',
    'optimizer_search_depth',
    'preload_buffer_size',
    'query_alloc_block_size',
    'query_cache_limit',
    'query_cache_min_res_unit',
    'query_cache_size',
    'query_cache_type',
    'query_cache_wlock_invalidate',
    'query_prealloc_size',
    'range_alloc_block_size',
    'read_buffer_size',
    'read_rnd_buffer_size',
    'slave_checkpoint_group',
    'slave_checkpoint_period',
    'slave_parallel_workers',
    'slave_pending_jobs_size_max',
    'sort_buffer_size',
    'stored_program_cache',
    'table_definition_cache',
    'table_open_cache',
    'table_open_cache_instances',
    'thread_stack',
    'transaction_alloc_block_size',
    'transaction_prealloc_size'

    'binlog_group_commit_sync_delay',
    'binlog_group_commit_sync_no_delay_count',
    'innodb_adaptive_hash_index_parts',
    'innodb_buffer_pool_chunk_size',
    'innodb_buffer_pool_dump_pct',
    'innodb_disable_sort_file_cache',
    'innodb_flush_sync',
    'innodb_ft_total_cache_size',
    'innodb_log_write_ahead_size',
    'innodb_max_undo_log_size',
    'innodb_page_cleaners',
    'innodb_purge_rseg_truncate_frequency',
    'range_optimizer_max_mem_size',
    'rpl_stop_slave_timeout',
    'slave_allow_batching',
    'slave_parallel_type',

]

knobs_for_8_0 = [
    'innodb_adaptive_flushing_lwm',
    'innodb_adaptive_hash_index',
    'innodb_adaptive_max_sleep_delay',
    'innodb_buffer_pool_instances',
    'innodb_buffer_pool_size',
    'innodb_change_buffering',
    'innodb_io_capacity',
    'innodb_log_file_size',
    'innodb_max_dirty_pages_pct',
    'innodb_max_dirty_pages_pct_lwm',
    'innodb_sync_array_size',
    'innodb_thread_concurrency',
    'max_heap_table_size',
    'thread_cache_size',
    'tmp_table_size'

    'binlog_cache_size',
    'binlog_max_flush_queue_time',
    'binlog_stmt_cache_size',
    'eq_range_index_dive_limit',
    'host_cache_size',
    'innodb_adaptive_flushing',
    'innodb_autoextend_increment',
    'innodb_buffer_pool_dump_now',
    'innodb_buffer_pool_load_at_startup',
    'innodb_buffer_pool_load_now',
    'innodb_change_buffer_max_size',
    'innodb_commit_concurrency',
    'innodb_compression_failure_threshold_pct',
    'innodb_compression_level',
    'innodb_compression_pad_pct_max',
    'innodb_concurrency_tickets',
    'innodb_flush_log_at_timeout',
    'innodb_flush_neighbors',
    'innodb_flushing_avg_loops',
    'innodb_ft_cache_size',
    'innodb_ft_result_cache_limit',
    'innodb_ft_sort_pll_degree',
    'innodb_io_capacity_max',
    'innodb_lock_wait_timeout',
    'innodb_log_buffer_size',
    'innodb_lru_scan_depth',
    'innodb_max_purge_lag',
    'innodb_max_purge_lag_delay',
    'innodb_old_blocks_pct',
    'innodb_old_blocks_time',
    'innodb_online_alter_log_max_size',
    'innodb_page_size',
    'innodb_purge_batch_size',
    'innodb_purge_threads',
    'innodb_random_read_ahead',
    'innodb_read_ahead_threshold',
    'innodb_read_io_threads',
    'innodb_replication_delay',
    'innodb_rollback_segments',
    'innodb_sort_buffer_size',
    'innodb_spin_wait_delay',
    'innodb_sync_spin_loops',
    'innodb_thread_sleep_delay',
    'innodb_use_native_aio',
    'innodb_write_io_threads',
    'join_buffer_size',
    'lock_wait_timeout',
    'max_binlog_cache_size',
    'max_binlog_size',
    'max_binlog_stmt_cache_size',
    'max_delayed_threads',
    'max_insert_delayed_threads',
    'max_join_size',
    'max_length_for_sort_data',
    'max_seeks_for_key',
    'max_sort_length',
    'max_sp_recursion_depth',
    'max_write_lock_count',
    'optimizer_prune_level',
    'optimizer_search_depth',
    'preload_buffer_size',
    'query_alloc_block_size',
    'query_prealloc_size',
    'range_alloc_block_size',
    'read_buffer_size',
    'read_rnd_buffer_size',
    'slave_checkpoint_group',
    'slave_checkpoint_period',
    'slave_parallel_workers',
    'slave_pending_jobs_size_max',
    'sort_buffer_size',
    'stored_program_cache',
    'table_definition_cache',
    'table_open_cache',
    'table_open_cache_instances',
    'thread_stack',
    'transaction_alloc_block_size',
    'transaction_prealloc_size'

    'binlog_group_commit_sync_delay',
    'binlog_group_commit_sync_no_delay_count',
    'innodb_adaptive_hash_index_parts',
    'innodb_buffer_pool_chunk_size',
    'innodb_buffer_pool_dump_pct',
    'innodb_disable_sort_file_cache',
    'innodb_flush_sync',
    'innodb_ft_total_cache_size',
    'innodb_log_write_ahead_size',
    'innodb_max_undo_log_size',
    'innodb_page_cleaners',
    'innodb_purge_rseg_truncate_frequency',
    'range_optimizer_max_mem_size',
    'rpl_stop_slave_timeout',
    'slave_allow_batching',
    'slave_parallel_type',

    'innodb_dedicated_server',
    'innodb_doublewrite_batch_size',
    'innodb_doublewrite_files',
    'innodb_doublewrite_pages',
    'innodb_log_files_in_group',
    'innodb_log_spin_cpu_abs_lwm',
    'innodb_log_spin_cpu_pct_hwm',
    'innodb_log_wait_for_flush_spin_hwm',
    'max_relay_log_size',
    'open_files_limit',
    'parser_max_mem_size',
    'relay_log_space_limit',
    'rpl_read_size',
    'stored_program_definition_cache',
    'tablespace_definition_cache',
    'temptable_max_ram',
]

interest_params = {
    r'MySQL 5.6.([1-9]\d|\d)': knobs_for_5_6,
    r'MySQL 5.7.([1-9]\d|\d)': knobs_for_5_7,
    r'MySQL 8.0.([1-9]\d|\d)': knobs_for_8_0,
    r'MariaDB 10\.0\.([1-9]\d|\d)': knobs_for_5_6,
    r'MariaDB 10\.2\.([1-9]\d|\d)': knobs_for_5_7,
    r'MariaDB 10\.([1-9]\d|[3-9])\.([1-9]\d|\d)': knobs_for_8_0,
    r'Percona 5.6.([1-9]\d|\d)': knobs_for_5_6,
    r'Percona 5.7.([1-9]\d|\d)': knobs_for_5_7,
    r'Percona 8.0.([1-9]\d|\d)': knobs_for_8_0,
}



def find_variables(json_obj):
    if isinstance(json_obj, dict):
        if 'Variables' in json_obj:
            return json_obj['Variables']
        for value in json_obj.values():
            found_variables = find_variables(value)
            if found_variables:
                return found_variables
    elif isinstance(json_obj, list):
        for item in json_obj:
            found_variables = find_variables(item)
            if found_variables:
                return found_variables
    return None


def extract_attributes(json_obj, db_version):
    for version_pattern, params in interest_params.items():
        if re.match(version_pattern, db_version):
            variables = find_variables(json_obj)
            if variables is not None:
                attributes_values = [str(variables.get(param, 'null')) for param in params]
                return ';'.join(attributes_values)
            return 'null;' * len(params)
    return 'null;'


def process_row(row):
    json_file_path = f"C:/Users/Дмитрий/Desktop/Archive_json/{row['metric_file']}"
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            try:
                json_obj = json.load(json_file)
                attributes_values = extract_attributes(json_obj, row['mysql_version'])
                return f"{attributes_values}"
            except:
                return None
    else:
        return None


def iterate_files_in_directory(source_path):
    for filename in os.listdir(source_path):
        if os.path.isfile(os.path.join(source_path, filename)):
            yield filename


def main(directory_path):
    for filename in iterate_files_in_directory(directory_path):
        df = pd.read_csv(f'{directory_path}\\{filename}')
        df['result'] = df.apply(process_row, axis=1)

        df = df.dropna(subset=['result'])

        output_csv_file = f'../processed_tables/{filename}'
        df.to_csv(output_csv_file, index=False)


main('C:\\Users\Дмитрий\Desktop\DBMS')
