# Import custom modules.
from common.subprocess import executeCommand

# Import native modules.
import codecs
import os

def materializeQueryResult(
    table_name,
    path,
    selected_columns = [],
    where_clause = ''
):
    # Prepare the initial set of parameters.
    parameters = [
        '/usr/bin/sqoop', 'import',
        '--connect', 'jdbc:oracle:thin:@dwhprddv.st.sk:1525/EWHPRDP1.world',
        '--username', os.environ['_DWH_USERNAME'],
        '--password-file', os.environ['_DWH_PASSWORD_HDFS_FILE_PATH'],
        '--table', table_name,
        '--target-dir', path,
        '--null-string', '',
        '--null-non-string', '',
        '--num-mappers', '1',
        '--delete-target-dir',
        '--as-textfile',
        '--fields-terminated-by', '\t',
        '--lines-terminated-by', '\n',
        '--hive-delims-replacement', 'anything'
    ]
    
    # Add the optional columns parameter.
    if len(selected_columns) > 0:
        parameters.append('--columns')
        parameters.append(','.join(selected_columns))
    
    # Add the optional where parameter.    
    if len(where_clause) > 0:
        parameters.append('--where')
        parameters.append(where_clause)
    
    # Execute the prepared parameters.
    executeCommand(parameters, True)
    
    # Remove the created java code.
    os.remove(table_name  + '.java')

def iterateQueryResult(
    table_name,
    hdfs_temp_directory_path,
    local_temp_file_path,
    selected_columns = [],
    where_clause = ''
):
    # Materialize the query result.
    materializeQueryResult(table_name, hdfs_temp_directory_path, selected_columns, where_clause)
    
    # Merge and move the materialized hive query into the local temp file path.
    executeCommand(['hadoop', 'fs', '-getmerge', hdfs_temp_directory_path, local_temp_file_path], True)
    executeCommand(['hadoop', 'fs', '-rm', '-r', '-f', '-skipTrash', hdfs_temp_directory_path], True)
    
    # Open and iterate through each line of the local temp file.
    with codecs.open(local_temp_file_path, encoding = 'utf-8') as file:
        for line in file:
            yield line[:-1].split('\t')
    
    # Remove the local temp file and the generated crc file.
    crc_file_name = '.' + os.path.basename(local_temp_file_path) + '.crc'
    os.remove(os.path.join(os.path.dirname(local_temp_file_path), crc_file_name))
    os.remove(local_temp_file_path)
