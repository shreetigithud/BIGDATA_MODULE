#!/usr/bin/env python
# coding: utf-8

# # Write a Python program to read a Hadoop configuration file and display the core components of Hadoop.

# In[ ]:


from configparser import ConfigParser

def display_hadoop_core_components(config_file_path):
    # Create a ConfigParser object
    config = ConfigParser()

    # Read the configuration file
    config.read(config_file_path)

    # Get the sections containing Hadoop core components
    core_sections = [section for section in config.sections() if 'core' in section.lower()]

    # Display the core components
    if core_sections:
        print("Hadoop Core Components:")
        for section in core_sections:
            print(f"- {section}")
    else:
        print("No Hadoop core components found in the configuration file.")

# Specify the path to your Hadoop configuration file
config_file_path = '/path/to/hadoop/config/file.xml'

# Call the function to display the core components
display_hadoop_core_components(config_file_path)


# # 2. Implement a Python function that calculates the total file size in a Hadoop Distributed File System (HDFS) directory.
# 

# In[ ]:


import pyarrow.hdfs as hdfs

def calculate_hdfs_directory_size(hdfs_host, hdfs_port, hdfs_directory):
    # Create an HDFS client
    client = hdfs.connect(host=hdfs_host, port=hdfs_port)

    # Initialize total size
    total_size = 0

    # Iterate over files in the directory
    for file_info in client.ls(hdfs_directory):
        if file_info['kind'] == 'file':
            total_size += file_info['size']

    # Close the HDFS client
    client.close()

    # Return the total file size
    return total_size

# Specify the HDFS host, port, and directory path
hdfs_host = 'localhost'
hdfs_port = 9000
hdfs_directory = '/path/to/hdfs/directory'

# Call the function to calculate the total file size
total_size = calculate_hdfs_directory_size(hdfs_host, hdfs_port, hdfs_directory)
print(f"Total File Size in HDFS Directory: {total_size} bytes")


# # 3. Create a Python program that extracts and displays the top N most frequent words from a large text file using the MapReduce approach.

# In[ ]:


from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class TopNWords(MRJob):
    
    def configure_args(self):
        super(TopNWords, self).configure_args()
        self.add_passthru_arg('-n', '--top_n', type=int, help='Number of top words to display')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_top_n_words)
        ]

    def mapper_get_words(self, _, line):
        words = re.findall(r'\w+', line.lower())
        for word in words:
            yield word, 1

    def combiner_count_words(self, word, counts):
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)

    def reducer_top_n_words(self, _, word_counts):
        top_n = self.options.top_n
        sorted_word_counts = sorted(word_counts, reverse=True)[:top_n]
        for count, word in sorted_word_counts:
            yield word, count

if __name__ == '__main__':
    TopNWords.run()
python top_n_words.py -n 10 large_text_file.txt


# # 4. Write a Python script that checks the health status of the NameNode and DataNodes in a Hadoop cluster using Hadoop's REST API.
# 

# In[ ]:


import requests

# Specify the Hadoop cluster's NameNode and DataNode REST API URLs
namenode_url = 'http://namenode_host:50070'
datanode_url = 'http://datanode_host:50075'

def check_namenode_health():
    response = requests.get(f'{namenode_url}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus')
    if response.status_code == 200:
        health_status = response.json()['beans'][0]['State']
        print(f"NameNode Health Status: {health_status}")
    else:
        print("Failed to retrieve NameNode health status.")

def check_datanode_health():
    response = requests.get(f'{datanode_url}/jmx?qry=Hadoop:service=DataNode,name=FSDatasetState-null')
    if response.status_code == 200:
        datanodes = response.json()['beans']
        for datanode in datanodes:
            health_status = datanode['FSDatasetState']
            datanode_hostname = datanode['tag.Hostname']
            print(f"DataNode Hostname: {datanode_hostname}, Health Status: {health_status}")
    else:
        print("Failed to retrieve DataNode health status.")

# Call the functions to check the health status
check_namenode_health()
check_datanode_health()


# # 5. Develop a Python program that lists all the files and directories in a specific HDFS path.

# In[ ]:


import pyarrow.hdfs as hdfs

def list_hdfs_path(hdfs_host, hdfs_port, hdfs_path):
    # Create an HDFS client
    client = hdfs.connect(host=hdfs_host, port=hdfs_port)

    # List files and directories in the HDFS path
    files = client.ls(hdfs_path)

    # Print the list of files and directories
    for file_info in files:
        print(file_info['name'])

    # Close the HDFS client
    client.close()

# Specify the HDFS host, port, and path
hdfs_host = 'localhost'
hdfs_port = 9000
hdfs_path = '/path/to/hdfs/directory'

# Call the function to list files and directories
list_hdfs_path(hdfs_host, hdfs_port, hdfs_path)


# # 6. Implement a Python program that analyzes the storage utilization of DataNodes in a Hadoop cluster and identifies the nodes with the highest and lowest storage capacities.

# In[ ]:


import requests

def analyze_data_node_storage(data_node_url):
    # Retrieve the DataNode's storage metrics
    response = requests.get(f'{data_node_url}/jmx?qry=Hadoop:service=DataNode,name=DataNodeInfo')
    if response.status_code == 200:
        data_node_info = response.json()['beans'][0]

        # Extract relevant storage metrics
        capacity = data_node_info['Capacity']
        used = data_node_info['Used']
        remaining = data_node_info['Remaining']

        # Calculate storage utilization percentage
        utilization = (used / capacity) * 100

        return capacity, used, remaining, utilization

    return None

def get_highest_lowest_storage_data_node(data_nodes):
    highest_capacity = 0
    highest_capacity_node = None
    lowest_capacity = float('inf')
    lowest_capacity_node = None

    for node in data_nodes:
        capacity, _, _, _ = analyze_data_node_storage(node['httpAddress'])
        if capacity is not None:
            if capacity > highest_capacity:
                highest_capacity = capacity
                highest_capacity_node = node
            if capacity < lowest_capacity:
                lowest_capacity = capacity
                lowest_capacity_node = node

    return highest_capacity_node, lowest_capacity_node

def analyze_hadoop_cluster_storage(data_nodes_url):
    # Retrieve the list of DataNodes in the cluster
    response = requests.get(data_nodes_url)
    if response.status_code == 200:
        data_nodes = response.json()['beans']

        # Get the DataNode with the highest and lowest storage capacities
        highest_capacity_node, lowest_capacity_node = get_highest_lowest_storage_data_node(data_nodes)

        # Print the analysis results
        print("Storage Utilization Analysis:")
        print(f"Highest Capacity Node: {highest_capacity_node['name']} (Capacity: {highest_capacity_node['capacity']})")
        print(f"Lowest Capacity Node: {lowest_capacity_node['name']} (Capacity: {lowest_capacity_node['capacity']})")
    else:
        print("Failed to retrieve the list of DataNodes.")

# Specify the URL of the Hadoop cluster's DataNodes API
data_nodes_url = 'http://namenode_host:50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo'

# Call the function to analyze the storage utilization
analyze_hadoop_cluster_storage(data_nodes_url)


# # 7. Create a Python script that interacts with YARN's ResourceManager API to submit a Hadoop job, monitor its progress, and retrieve the final output.
# 

# In[ ]:


import requests
import time

def submit_hadoop_job(resource_manager_url, jar_path, main_class, input_path, output_path):
    # Submit the Hadoop job
    submit_url = f'{resource_manager_url}/ws/v1/cluster/apps/new-application'
    response = requests.post(submit_url)
    if response.status_code == 200:
        application_id = response.json()['application-id']

        # Set up the job submission request
        job_submit_url = f'{resource_manager_url}/ws/v1/cluster/apps'
        data = {
            "application-id": application_id,
            "application-name": "Hadoop Job",
            "am-container-spec": {
                "commands": {
                    "command": f"yarn jar {jar_path} {main_class} {input_path} {output_path}"
                },
                "local-resources": {}
            },
            "application-type": "MAPREDUCE"
        }

        # Submit the job
        response = requests.post(job_submit_url, json=data)
        if response.status_code == 202:
            print("Hadoop job submitted successfully.")
            return application_id
        else:
            print("Failed to submit the Hadoop job.")
    else:
        print("Failed to get a new application ID.")

    return None

def monitor_job_progress(resource_manager_url, application_id):
    # Check the job status periodically
    while True:
        job_status_url = f'{resource_manager_url}/ws/v1/cluster/apps/{application_id}'
        response = requests.get(job_status_url)
        if response.status_code == 200:
            status = response.json()['app']['state']
            if status in ['ACCEPTED', 'RUNNING']:
                print(f"Job status: {status}")
                time.sleep(5)  # Wait for 5 seconds before checking again
            elif status == 'FINISHED':
                print("Job completed successfully.")
                break
            else:
                print("Job failed or terminated.")
                break
        else:
            print("Failed to retrieve job status.")
            break

def retrieve_job_output(resource_manager_url, application_id, output_path):
    # Retrieve the job output
    job_output_url = f'{resource_manager_url}/ws/v1/cluster/apps/{application_id}/attemptlogs'
    response = requests.get(job_output_url)
    if response.status_code == 200:
        logs = response.json()['logs']
        print(f"Job output:\n{logs}")
    else:
        print("Failed to retrieve job output.")

# Specify the ResourceManager URL
resource_manager_url = 'http://resourcemanager_host:8088'

# Specify the Hadoop job details
jar_path = '/path/to/hadoop/job.jar'
main_class = 'com.example.JobMainClass'
input_path = '/path/to/input'
output_path = '/path/to/output'

# Submit the Hadoop job
application_id = submit_hadoop_job(resource_manager_url, jar_path, main_class, input_path, output_path)

if application_id:
    # Monitor job progress
    monitor_job_progress(resource_manager_url, application_id)

    # Retrieve job output
    retrieve_job_output(resource_manager_url, application_id, output_path)


# # 8. Create a Python script that interacts with YARN's ResourceManager API to submit a Hadoop job, set resource requirements, and track resource usage during job execution.
# 

# In[ ]:


import requests
import time

def submit_hadoop_job(resource_manager_url, jar_path, main_class, input_path, output_path, memory_mb, vcores):
    # Submit the Hadoop job
    submit_url = f'{resource_manager_url}/ws/v1/cluster/apps/new-application'
    response = requests.post(submit_url)
    if response.status_code == 200:
        application_id = response.json()['application-id']

        # Set up the job submission request
        job_submit_url = f'{resource_manager_url}/ws/v1/cluster/apps'
        data = {
            "application-id": application_id,
            "application-name": "Hadoop Job",
            "am-container-spec": {
                "commands": {
                    "command": f"yarn jar {jar_path} {main_class} {input_path} {output_path}"
                },
                "local-resources": {}
            },
            "application-type": "MAPREDUCE",
            "resource": {
                "memory": memory_mb,
                "vCores": vcores
            }
        }

        # Submit the job
        response = requests.post(job_submit_url, json=data)
        if response.status_code == 202:
            print("Hadoop job submitted successfully.")
            return application_id
        else:
            print("Failed to submit the Hadoop job.")
    else:
        print("Failed to get a new application ID.")

    return None

def monitor_job_resource_usage(resource_manager_url, application_id):
    # Check the job resource usage periodically
    while True:
        job_status_url = f'{resource_manager_url}/ws/v1/cluster/apps/{application_id}'
        response = requests.get(job_status_url)
        if response.status_code == 200:
            status = response.json()['app']['state']
            if status in ['ACCEPTED', 'RUNNING']:
                resources_used = response.json()['app']['allocatedResources']
                memory_mb = resources_used['memory']
                vcores = resources_used['vCores']
                print(f"Resources used: Memory: {memory_mb} MB, vCores: {vcores}")
                time.sleep(5)  # Wait for 5 seconds before checking again
            elif status == 'FINISHED':
                print("Job completed successfully.")
                break
            else:
                print("Job failed or terminated.")
                break
        else:
            print("Failed to retrieve job status.")
            break

# Specify the ResourceManager URL
resource_manager_url = 'http://resourcemanager_host:8088'

# Specify the Hadoop job details and resource requirements
jar_path = '/path/to/hadoop/job.jar'
main_class = 'com.example.JobMainClass'
input_path = '/path/to/input'
output_path = '/path/to/output'
memory_mb = 2048
vcores = 2

# Submit the Hadoop job
application_id = submit_hadoop_job(resource_manager_url, jar_path, main_class, input_path, output_path, memory_mb, vcores)

if application_id:
    # Monitor job resource usage
    monitor_job_resource_usage(resource_manager_url, application_id)


# # 9. Write a Python program that compares the performance of a MapReduce job with different input split sizes, showcasing the impact on overall job execution time.

# In[ ]:


import subprocess
import time

def run_mapreduce_job(input_file, split_size):
    # Set the Hadoop input split size
    subprocess.run(['hdfs', 'dfs', '-D', 'mapreduce.input.fileinputformat.split.maxsize=' + str(split_size), '-put', input_file, '/input'])

    # Start the timer
    start_time = time.time()

    # Run the MapReduce job
    subprocess.run(['yarn', 'jar', 'path_to_hadoop_jar', 'MainClass', '/input', '/output'])

    # Calculate the elapsed time
    elapsed_time = time.time() - start_time

    # Return the elapsed time
    return elapsed_time

def compare_input_split_sizes(input_file, split_sizes):
    print("Comparing MapReduce job performance with different input split sizes:")

    for split_size in split_sizes:
        elapsed_time = run_mapreduce_job(input_file, split_size)
        print(f"Input Split Size: {split_size} bytes, Elapsed Time: {elapsed_time} seconds")

# Specify the input file path
input_file = '/path/to/input/file.txt'

# Specify the different input split sizes to compare
split_sizes = [128 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024]

# Run the comparison
compare_input_split_sizes(input_file, split_sizes)

