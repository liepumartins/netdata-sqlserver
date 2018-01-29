# -*- coding: utf-8 -*-
# Description: SQL server netdata python.d module
# Authors: facetoe, dangtranhoang, liepumartins
# Based on postgres python.d module by facetoe, dangtranhoang

from copy import deepcopy

try:
    import pymssql
    PY_MSSQL = True
except ImportError:
    PY_MSSQL = False

from bases.FrameworkServices.SimpleService import SimpleService

# default module values (can be overridden per job in `config`)
update_every = 1
priority = 60000
retries = 60

METRICS = dict(
    OBJECTS=[
        'SQLServer:General Statistics',
        'SQLServer:Memory Manager',
        'SQLServer:Buffer Manager',
        'SQLServer:CLR',
        'SQLServer:Latches',
        'SQLServer:Locks',
        'SQLServer:SQL Errors',
        'SQLServer:SQL Statistics',
        'SQLServer:Transactions',
        'SQLServer:Wait Statistics',
    ],

    # Detailed waits
    WAITS=[
        'ASYNC_IO_COMPLETION',
        'ASYNC_NETWORK_IO',
        'BACKUPBUFFER',
        'BACKUPIO',
        'CHKPT',
        'CMEMTHREAD',
        'CXCONSUMER',
        'CXPACKET',
        'DTC',
        'IO_COMPLETION',
        'LATCH (non-buffer)',
        'LOCKS',
        'LOGBUFFER',
        'MEMORY_ALLOCATION_EXT',
        'NETWORKIO',
        'OLEDB',
        'PAGE I/O LATCH',
        'PAGE LATCH (non-I/O)',
        'PAGEIOLATCH_EX',
        'PARALLEL_REDO_WORKER_WAIT_WORK',
        'RESERVED_MEMORY_ALLOCATION_EXT',
        'SERVER_IDLE_CHECK',
        'SESSION_WAIT_STATS_CHILDREN',
        'SOS_SCHEDULER_YIELD',
        'STARTUP_DEPENDENCY_MANAGER',
        'THREADPOOL',
        'WAIT_XTP_HOST_WAIT',
        'WRITE_COMPLETION',
        'WRITELOG',
    ]
)

QUERIES = dict(
    COUNTERS="""
select replace(rtrim(counter_name),' ','_') as counter_name,
replace(rtrim(instance_name),' ','_') as instance_name,
cntr_value, rtrim(object_name) as object_name
from sys.dm_os_performance_counters
where object_name in %(objects)s or 
(instance_name in %(instances)s and object_name = 'SQLServer:Databases')
""",
    WAITS="""
    """,
    FIND_DATABASES="""
SELECT name FROM master.dbo.sysdatabases;
"""
)

QUERY_STATS = {
    QUERIES['COUNTERS'],
}

ORDER = [
    'database_size',
    'log_size',
    'log_used_size',
    'server_latches',
    'errors',
    'waits_started',
    'waits_in_progress',
    'cumulative_wait_time',
    'average_wait_time',
    'memory',
    'memory_grants',
    'memory_lock_blocks',
    'memory_lock_owner_blocks',
    'statistics',
    'statistics_batch',
    'statistics_compilations',
    'statistics_recompilations',
    'statistics_attention',
    'statistics_param',
    'statistics_param_safety',
    'general_statistics_logins',
    'general_statistics_connections',
]

TEMPLATES = {
    'db_transactions': {
        'options': [None, 'Transactions on db', 'transactions/s', 'db statistics', 'sqlsrv.db_stat_transactions',
                    'line'],
        'lines': [
            ['Transactions/sec', 'transactions', 'absolute'],
            ['Write_Transactions/sec', 'write transactions', 'absolute'],
            ['Tracked_transactions/sec', 'tracked transactions', 'absolute'],
            ['Active_Transactions', 'active transactions', 'absolute']
        ]},
    'db_cache': {
        'options': [None, 'Cache entries on db', 'count', 'db statistics', 'sqlsrv.db_stat_cache', 'line'],
        'lines': [
            ['Cache_Entries_Count', 'entries', 'absolute'],
            ['Cache_Entries_Pinned_Count', 'pinned', 'absolute']
        ]},
    'db_cache_hit': {
        'options': [None, 'Cache Hit on db', 'ratio', 'db statistics', 'sqlsrv.db_stat_cache_hit', 'line'],
        'lines': [
            ['Cache_Hit_Ratio', 'hit', 'absolute'],
            ['Cache_Hit_Ratio_Base', 'hit base', 'absolute']
        ]},
}

CHARTS = {

    'server_latches': {
        'options': [None, 'Latches', 'waits/s', 'latch waits', 'sqlsrv.srv_latches', 'line'],
        'lines': [
            ['Latch_Waits/sec', 'latch waits', 'absolute']
        ]
    },
    'crl_exec': {
        'options': [None, 'CRL Executions', 'executions', 'crl executions', 'sqlsrv.srv_crl', 'line'],
        'lines': [
            ['CLR_Execution', 'crl exec', 'absolute']
        ]
    },


    'database_size': {
        'options': [None, 'Database file size', 'MB', 'database size', 'sqlsrv.db_file_size', 'stacked'],
        'lines': [
        ]},
    'log_size': {
        'options': [None, 'Log file size', 'MB', 'log size', 'sqlsrv.db_log_file_size', 'stacked'],
        'lines': [
        ]},
    'log_used_size': {
        'options': [None, 'Log used size', 'MB', 'log size', 'sqlsrv.db_log_used_size', 'stacked'],
        'lines': [
        ]},
    'errors': {
        'options': [None, 'Errors', 'errors', 'errors', 'sqlsrv.db_errors', 'stacked'],
        'lines': [
            ['DB_Offline_Errors_Errors/sec', 'db offline', 'absolute'],
            ['Kill_Connection_Errors_Errors/sec', 'kill connection', 'absolute'],
            ['User_Errors_Errors/sec', 'user', 'absolute'],
            ['Info_Errors_Errors/sec', 'info', 'absolute']
        ]},
    'waits_started': {
        'options': [None, 'Waits started', 'waits', 'resource waits', 'sqlsrv.db_waits_started', 'stacked'],
        'lines': [
            ['Waits_started_per_second_Lock_waits', 'lock', 'absolute'],
            ['Waits_started_per_second_Log_buffer_waits', 'log buffer', 'absolute'],
            ['Waits_started_per_second_Log_write_waits', 'log write', 'absolute'],
            ['Waits_started_per_second_Memory_grant_queue_waits', 'memory grant queue', 'absolute'],
            ['Waits_started_per_second_Network_IO_waits', 'network io', 'absolute'],
            ['Waits_started_per_second_Page_latch_waits', 'page latch', 'absolute'],
            ['Waits_started_per_second_Page_IO_latch_waits', 'page io latch', 'absolute'],
            ['Waits_started_per_second_Thread-safe_memory_objects_waits', 'thread-safe mem obj', 'absolute'],
            ['Waits_started_per_second_Transaction_ownership_waits', 'transaction ownership', 'absolute'],
            ['Waits_started_per_second_Wait_for_the_worker', 'wait for worker', 'absolute'],
            ['Waits_started_per_second_Workspace_synchronization_waits', 'worspace sync', 'absolute']
        ]
    },
    'waits_in_progress': {
        'options': [None, 'Waits in progress', 'waits', 'resource waits', 'sqlsrv.db_waits_in_progress', 'stacked'],
        'lines': [
            ['Waits_in_progress_Lock_waits', 'lock', 'absolute'],
            ['Waits_in_progress_Log_buffer_waits', 'log buffer', 'absolute'],
            ['Waits_in_progress_Log_write_waits', 'log write', 'absolute'],
            ['Waits_in_progress_Memory_grant_queue_waits', 'memory grant queue', 'absolute'],
            ['Waits_in_progress_Network_IO_waits', 'network io', 'absolute'],
            ['Waits_in_progress_Page_latch_waits', 'page latch', 'absolute'],
            ['Waits_in_progress_Page_IO_latch_waits', 'page io latch', 'absolute'],
            ['Waits_in_progress_Thread-safe_memory_objects_waits', 'thread-safe mem obj', 'absolute'],
            ['Waits_in_progress_Transaction_ownership_waits', 'transaction ownership', 'absolute'],
            ['Waits_in_progress_Wait_for_the_worker', 'wait for worker', 'absolute'],
            ['Waits_in_progress_Workspace_synchronization_waits', 'worspace sync', 'absolute']
        ]
    },
    'cumulative_wait_time': {
        'options': [None, 'Cumulative wait time', 'ms', 'resource waits', 'sqlsrv.db_cumulative_wait_time', 'stacked'],
        'lines': [
            ['Cumulative_wait_time_(ms)_per_second_Lock_waits', 'lock', 'absolute'],
            ['Cumulative_wait_time_(ms)_per_second_Log_buffer_waits', 'log buffer', 'absolute'],
            ['Cumulative_wait_time_(ms)_per_second_Log_write_waits', 'log write', 'absolute'],
            ['Cumulative_wait_time_(ms)_per_second_Memory_grant_queue_waits', 'memory grant queue', 'absolute'],
            ['Cumulative_wait_time_(ms)_per_second_Network_IO_waits', 'network io', 'absolute'],
            ['Cumulative_wait_time_(ms)_per_second_Page_latch_waits', 'page latch', 'absolute'],
            ['Cumulative_wait_time_(ms)_per_second_Page_IO_latch_waits', 'page io latch', 'absolute'],
            ['Cumulative_wait_time_(ms)_per_second_Thread-safe_memory_objects_waits', 'thread-safe memobj', 'absolute'],
            ['Cumulative_wait_time_(ms)_per_second_Transaction_ownership_waits', 'transaction ownership', 'absolute'],
            ['Cumulative_wait_time_(ms)_per_second_Wait_for_the_worker', 'wait for worker', 'absolute'],
            ['Cumulative_wait_time_(ms)_per_second_Workspace_synchronization_waits', 'workspace sync', 'absolute']
        ]
    },
    'average_wait_time': {
        'options': [None, 'Average wait time', 'ms', 'resource waits', 'sqlsrv.db_average_wait_time', 'stacked'],
        'lines': [
            ['Average_wait_time_(ms)_Lock_waits', 'lock', 'absolute'],
            ['Average_wait_time_(ms)_Log_buffer_waits', 'log buffer', 'absolute'],
            ['Average_wait_time_(ms)_Log_write_waits', 'log write', 'absolute'],
            ['Average_wait_time_(ms)_Memory_grant_queue_waits', 'memory grant queue', 'absolute'],
            ['Average_wait_time_(ms)_Network_IO_waits', 'network io', 'absolute'],
            ['Average_wait_time_(ms)_Page_latch_waits', 'page latch', 'absolute'],
            ['Average_wait_time_(ms)_Page_IO_latch_waits', 'page io latch', 'absolute'],
            ['Average_wait_time_(ms)_Thread-safe_memory_objects_waits', 'thread-safe mem obj', 'absolute'],
            ['Average_wait_time_(ms)_Transaction_ownership_waits', 'transaction ownership', 'absolute'],
            ['Average_wait_time_(ms)_Wait_for_the_worker', 'wait for worker', 'absolute'],
            ['Average_wait_time_(ms)_Workspace_synchronization_waits', 'worspace sync', 'absolute']
        ]
    },

    'memory_grants': {
        'options': [None, 'Memory Grants', 'grants', 'memory', 'sqlsrv.db_memory_grants', 'line'],
        'lines': [
            ['Memory_Grants_Outstanding', 'outstanding', 'absolute'],
            ['Memory_Grants_Pending', 'pending', 'absolute']
        ]
    },
    'memory_lock_blocks': {
        'options': [None, 'Lock blocks', 'blocks', 'memory', 'sqlsrv.db_memory_lock_blocks', 'line'],
        'lines': [
            ['Lock_Blocks_Allocated', 'allocated', 'absolute'],
            ['Lock_Blocks', 'blocks', 'absolute'],
        ]
    },
    'memory_lock_owner_blocks': {
        'options': [None, 'Lock blocks', 'blocks', 'memory', 'sqlsrv.db_memory_lock_owner_blocks', 'line'],
        'lines': [
            ['Lock_Owner_Blocks_Allocated', 'allocated', 'absolute'],
            ['Lock_Owner_Blocks', 'blocks', 'absolute']
        ]
    },
    'memory': {
        'options': [None, 'memory', 'MB', 'memory', 'sqlsrv.db_memory', 'line'],
        'lines': [
            ['Total_Server_Memory_(KB)', 'total', 'absolute', 1, 1024],
            ['Target_Server_Memory_(KB)', 'target', 'absolute', 1, 1024],
            ['Log_Pool_Memory_(KB)', 'log pool', 'absolute', 1, 1024],
            ['Stolen_Server_Memory_(KB)', 'stolen', 'absolute', 1, 1024],
            ['SQL_Cache_Memory_(KB)', 'sql cache', 'absolute', 1, 1024],
            ['Reserved_Server_Memory_(KB)', 'reserved', 'absolute', 1, 1024],
            ['Optimizer_Memory_(KB)', 'optimizer', 'absolute', 1, 1024],
            ['Maximum_Workspace_Memory_(KB)', 'max workspace', 'absolute', 1, 1024],
            ['Lock_Memory_(KB)', 'lock', 'absolute', 1, 1024],
            ['Granted_Workspace_Memory_(KB)', 'granted workspace', 'absolute', 1, 1024],
            ['Free_Memory_(KB)', 'free', 'absolute', 1, 1024],
            ['Database_Cache_Memory_(KB)', 'database cache', 'absolute', 1, 1024],
            ['Connection_Memory_(KB)', 'connection', 'absolute', 1, 1024],
        ]
    },
    'statistics': {
        'options': [None, 'Plan executions', 'count', 'sql statistics', 'sqlsrv.plan_exec', 'line'],
        'lines': [
            ['Guided_plan_executions/sec', 'guided plan exec', 'absolute'],
            ['Misguided_plan_executions/sec', 'misguided plan exec', 'absolute'],
        ]
    },
    'statistics_batch': {
        'options': [None, 'Batch requests', 'count', 'sql statistics', 'sqlsrv.batch', 'line'],
        'lines': [
            ['Batch_Requests/sec', 'batch requests', 'absolute'],
        ]
    },
    'statistics_compilations': {
        'options': [None, 'SQL compilations', 'count', 'sql statistics', 'sqlsrv.compilations', 'line'],
        'lines': [
            ['SQL_Compilations/sec', 'sql compilations', 'absolute'],
        ]
    },
    'statistics_recompilations': {
        'options': [None, 'SQL recompilations', 'count', 'sql statistics', 'sqlsrv.recompilations', 'line'],
        'lines': [
            ['SQL_Re-Compilations/sec', 're-compilations', 'absolute'],
        ]
    },
    'statistics_attention': {
        'options': [None, 'SQL attention rate', 'count', 'sql statistics', 'sqlsrv.attention', 'line'],
        'lines': [
            ['SQL_Attention_rate', 'attention rate', 'absolute'],
        ]
    },
    'statistics_param': {
        'options': [None, 'Param statistics', 'params', 'sql statistics', 'sqlsrv.param', 'line'],
        'lines': [
            ['Forced_Parameterizations/sec', 'forced param', 'absolute'],
            ['Auto-Param_Attempts/sec', 'auto attempts', 'absolute'],
            ['Failed_Auto-Params/sec', 'failed auto', 'absolute'],
        ]
    },
    'statistics_param_safety': {
        'options': [None, 'Safe/Unsafe Auto Params', 'params', 'sql statistics', 'sqlsrv.param_safety', 'stacked'],
        'lines': [
            ['Safe_Auto-Params/sec', 'safe', 'absolute'],
            ['Unsafe_Auto-Params/sec', 'unsafe', 'absolute'],
        ]
    },
    'general_statistics_logins': {
        'options': [None, 'Logins/Logouts', 'count', 'general statistics', 'sqlsrv.logins', 'line'],
        'lines': [
            ['Logouts/sec', 'logouts', 'absolute'],
            ['Logins/sec', 'logins', 'absolute'],
        ]
    },
    'general_statistics_connections': {
        'options': [None, 'Connections', 'count', 'general statistics', 'sqlsrv.connections', 'line'],
        'lines': [
            ['User_Connections', 'user', 'absolute'],
            ['Logical_Connections', 'logical', 'absolute'],
        ]
    },

}


class Service(SimpleService):
    def __init__(self, configuration=None, name=None):
        SimpleService.__init__(self, configuration=configuration, name=name)
        self.order = ORDER[:]
        self.definitions = deepcopy(CHARTS)
        self.database_poll = configuration.pop('databases', None)
        self.configuration = configuration
        self.connection = False
        self.data = dict()
        self.databases = list()
        self.queries = QUERY_STATS.copy()

    def _connect(self):
        params = dict(
            server='localhost',
            port=1433,
            user='netdata',
            password=None,
            appname='netdata monitoring'
            )
        params.update(self.configuration)

        if not self.connection:
            try:
                self.connection = pymssql.connect(**params)
            except pymssql.OperationalError as error:
                return False, str(error)
        return True, True

    def check(self):
        if not PY_MSSQL:
            self.error('pymssql module is needed to use mssql.chart.py plugin')
            return False
        result, error = self._connect()
        if not result:
            conf = dict((k, (lambda k, v: v if k != 'password' else '*****')(k, v))
                        for k, v in self.configuration.items())
            self.error('Failed to connect to %s. Error: %s' % (str(conf), error))
            return False
        try:
            cursor = self.connection.cursor()
            self.databases = discover_databases_(cursor, QUERIES['FIND_DATABASES'])
            cursor.close()

            if self.database_poll and isinstance(self.database_poll, str):
                self.databases = [dbase for dbase in self.databases if dbase in self.database_poll.split()]\
                                 or self.databases
            self.create_dynamic_charts_()

            return True
        except Exception as error:
            self.error(str(error))
            return False

    def create_dynamic_charts_(self):
        for database_name in self.databases[::-1]:
            self.definitions['database_size']['lines'].append(
                [database_name + '_Data_File(s)_Size_(KB)', database_name, 'absolute', 1, 1024])
            self.definitions['log_size']['lines'].append(
                [database_name + '_Log_File(s)_Size_(KB)', database_name, 'absolute', 1, 1024])
            self.definitions['log_used_size']['lines'].append(
                [database_name + '_Log_File(s)_Used_Size_(KB)', database_name, 'absolute', 1, 1024])

            for chart_name in [name for name in TEMPLATES if name.startswith('db_')]:
                add_chart_from_template_(order=self.order, definitions=self.definitions, name=chart_name,
                                         database_name=database_name)

    def _get_data(self):
        result, error = self._connect()
        if result:
            try:
                cursor = self.connection.cursor(as_dict=True)
                for query in self.queries:
                    self.query_stats_(cursor, query)

            except pymssql.OperationalError:
                self.connection = False
                cursor.close()
                return None
            else:
                cursor.close()
                return self.data
        else:
            return None

    def query_stats_(self, cursor, query):
        instances = self.databases[:]
        cursor.execute(query, dict(objects=tuple(METRICS['OBJECTS']), instances=tuple(instances)))
        for row in cursor:
            if 'instance_name' in row:
                dimension_id = '_'.join([row['instance_name'], row['counter_name']]) if row['instance_name'] != '' else\
                    row['counter_name']
                self.data[dimension_id] = int(row['cntr_value'])
            elif 'wait_category' in row:
                self.data[row['wait_category'] + "_wait_time_ms"] = int(row['wait_time_ms'])
                self.data[row['wait_category'] + '_waiting_tasks_count'] = int(row['waiting_tasks_count'])
                self.data[row['wait_category'] + '_max_wait_time_ms'] = int(row['max_wait_time_ms'])


def discover_databases_(cursor, query):
    cursor.execute(query)
    result = list()
    for db in [database[0] for database in cursor]:
        if db not in result:
            result.append(db)
    return result


def add_chart_from_template_(order, definitions, name, database_name):
    def create_lines(database, lines):
        result = list()
        for line in lines:
            new_line = ['_'.join([database, line[0]])] + line[1:]
            result.append(new_line)
        return result

    chart_template = TEMPLATES[name]
    chart_name = '_'.join([database_name, name])
    order.insert(0, chart_name)
    name, title, units, family, context, chart_type = chart_template['options']
    definitions[chart_name] = {
        'options': [name, title + ': ' + database_name,  units, 'db ' + database_name, context,  chart_type],
        'lines': create_lines(database_name, chart_template['lines'])
    }

