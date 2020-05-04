# Import custom modules.
from common.subprocess import executeCommand

# Import external modules.
import pyspark.sql

# Import internal modules.
import __main__ as main
import codecs
import os

def dataframeToHdfsTsv(
    dataframe,
    path
):    
    # Store the dataframe as a csv in hdfs.
    dataframe.write.format('csv') \
        .option('header', 'false') \
        .option('sep', '\t') \
        .option('emptyValue', '') \
        .mode('overwrite').save(path)

def generateSession(
    app_name = 'Default',
    memory = 16,
    cores = 4
):
    # Determine the script path for the app name.
    script_path = os.getcwd()
    if hasattr(main, '__file__'):
        script_path = main.__file__
    
    # Procure a spark session, subscribed at yarn and configured with the given parameters.
    return pyspark.sql.SparkSession.builder.master('yarn') \
        .appName(script_path + ' : ' + app_name) \
        .config('spark.executor.memory', str(memory) + 'G') \
        .config('spark.executor.cores', str(cores)) \
        .config('spark.dynamicAllocation.maxExecutors', '5') \
        .config('mapred.job.queue.name', 'root.users') \
        .enableHiveSupport().getOrCreate()

def materializeHiveQueryResult(
    query,
    path,
    memory = 16,
    cores = 4,
    spark = None,
    is_public = True
):
    # Initialize the spark session if none has been supplied.
    local_spark = spark
    is_local_spark = False
    if local_spark is None:
        local_spark = generateSession('Materialize hive query result', memory, cores)
        is_local_spark = True
    
    # Execute the hive query.
    dataframe = local_spark.sql(query)
    
    # Store the dataframe as a csv in hdfs.
    dataframeToHdfsTsv(dataframe, path)

    # Terminate the spark session if none has been supplied.
    if is_local_spark:
        local_spark.stop()
    
    # If result is public, correctly set the directory's permissions as well as the contained files.
    if is_public:
         # Set the correct permissions upon the created directory and its contents.
        executeCommand(['hadoop', 'fs', '-chmod', '-R', '+r', path], True)
        executeCommand(['hadoop', 'fs', '-chmod', '+x', path], True)

def iterateHiveQueryResult(
    query,
    hdfs_temp_directory_path,
    local_temp_file_path,
    memory = 16,
    cores = 4,
    spark = None
):
    # Materialize the hive query result.
    materializeHiveQueryResult(query, hdfs_temp_directory_path, memory, cores, spark, False)
    
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

def copyLocalDataFileToHiveTable(
    local_data_file_path,
    hdfs_directory_path,
    hive_table_name,
    partitions = [],
    is_public = True
):
    # Set the initial hdfs directory as the directory to contain the data file.
    hdfs_final_directory_path = hdfs_directory_path
    
    # Process each partition level.
    for name, value in partitions:
        # Update the hdfs final directory path.
        hdfs_final_directory_path = os.path.join(hdfs_final_directory_path, f'{name}={value}')
        
        # Create a new subdirectory if required.
        executeCommand(['hadoop', 'fs', '-mkdir', '-p', hdfs_final_directory_path], True)
        
        # If the hive table is public, correctly set the directory's permissions.
        if is_public:
            executeCommand(['hadoop', 'fs', '-chmod', '+r', hdfs_final_directory_path], True)
            executeCommand(['hadoop', 'fs', '-chmod', '+x', hdfs_final_directory_path], True)
    
    # Copy the local data file to the hdfs final directory.
    executeCommand(['hadoop', 'fs', '-copyFromLocal', '-f', local_data_file_path, hdfs_final_directory_path], True)
    
    # If the hive table is public, correctly set the file's permissions.
    if is_public:
        data_file_name = os.path.basename(local_data_file_path)
        executeCommand(['hadoop', 'fs', '-chmod', '+r', os.path.join(hdfs_final_directory_path, data_file_name)], True)

    # Refresh the hive table to take into account the newly added data.
    executeCommand(['hive', '-e', f'MSCK REPAIR TABLE {hive_table_name}'], True)

def appendDataToHiveTableFile(
    data,
    hdfs_data_file_path,
    hive_table_name,
    local_temp_file_path,
    is_public = True
):     
    # Write the text to be appended into the local temp file.
    with codecs.open(local_temp_file_path, 'w', encoding = 'utf-8') as file:
        for line in data:
            file.write('\t'.join(line) + '\n')
    
    # Append the content of the local temp file to the hdfs file corresponding to the given base table.
    executeCommand(['hdfs', 'dfs', '-appendToFile', local_temp_file_path, hdfs_data_file_path], True)
    
    # Remove the local temp file.
    os.remove(local_temp_file_path)
    
    # Refresh the hive table to take into account the newly added data.
    executeCommand(['hive', '-e', f'MSCK REPAIR TABLE {hive_table_name}'], True)
    
    # If the hive table is public, correctly set the file's permissions, in case it was newly created.
    if is_public:
        executeCommand(['hadoop', 'fs', '-chmod', '+r', hdfs_data_file_path], True)

def transformHdfsDirectoryToFile(
    hdfs_directory_path,
    hdfs_file_path,
    local_temp_file_path
):
    # Merge and move the hdfs directory's children into the local temp file path.
    executeCommand(['hadoop', 'fs', '-getmerge', hdfs_directory_path, local_temp_file_path], True)
    executeCommand(['hadoop', 'fs', '-rm', '-r', '-f', '-skipTrash', hdfs_directory_path], True)
    
    # Move the local temp file to the hdfs file path.
    executeCommand(['hadoop', 'fs', '-copyFromLocal', '-f', local_temp_file_path, hdfs_file_path], True)
    os.remove(local_temp_file_path)
