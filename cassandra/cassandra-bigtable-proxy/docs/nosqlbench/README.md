# NoSQLBench Setup and Execution Guide

This guide provides detailed instructions on how to set up and execute NoSQLBench on a virtual machine (VM) with a specific focus on the `bigtable_workload.yaml` file.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Setup and Configuration](#setup-and-configuration)
  - [Configuring the Workload File](#configuring-the-workload-file)
  - [Running NoSQLBench](#running-nosqlbench)
  - [Scenarios and Commands](#scenarios-and-commands)
- [Troubleshooting](#troubleshooting)

## Prerequisites

- **Google Cloud SDK**: Ensure that the Google Cloud SDK is installed and configured on your local machine.
- **Access to VM Instance**: You will need to SSH into your Google Cloud VM instance where NoSQLBench will be executed.
- **Java**: Ensure that Java is installed on your VM. Java 11 is sufficient, but Java 17 is installed on this VM as it is the latest LTS (Long-Term Support) version and provides enhanced performance and security features.
- **NoSQLBench**: Download the NoSQLBench jar (`nb5.jar`) from the official website.

## Installation

1. **Login to Google Cloud and Access the VM Instance**:

   - Log in to your Google Cloud account:
     ```sh
     gcloud auth login
     ```
   - Access your VM instance using SSH:
     ```sh
     gcloud compute ssh <your-vm-instance-name>
     ```

2. **Download NoSQLBench**:

   - **Option 1: Create and Setup on VM**
     - **Create a Directory on the VM**:
       - SSH into your VM and create a directory for NoSQLBench:
         ```sh
         gcloud compute ssh <your-vm-instance-name>
         mkdir -p ~/nosqlbench
         ```
     - **Create the Workload File**:
       - While still on the VM, create the `bigtable_workload.yaml` file within the directory:
         ```sh
         nano ~/nosqlbench/bigtable_workload.yaml
         ```
       - Paste the workload content into the file and save it.
     - **Upload NoSQLBench Jar File**:
       - On your local machine, use SCP to upload the `nb5.jar` file to the same directory on the VM:
         ```sh
         gcloud compute scp --recurse /local/path/nosqlbench user_name@vm_name:/home/user_name/nosqlbench
         ```
         Or

       - Download the NoSQLBench jar file directly onto your VM within the `nosqlbench` directory:
         ```sh
         wget -P ~/nosqlbench https://github.com/nosqlbench/nosqlbench/releases/download/latest/nb5.jar
         ```


   - **Option 2: Create Locally and Upload to VM**
     - **Create a Directory and Workload File locally and upload on VM using gcloud compute command**:
      - Create directory on Local - `nosqlbench`
      - Create file in `nosqlbench` directory `bigtable_workload.yaml` add the contents to workload file. 
      - Download the NoSQLBench jar file and place it in the same `nosqlbench` directory:
      - SSH into your VM and create a directory for NoSQLBench:
         ```sh
         gcloud compute ssh <your-vm-instance-name>
         mkdir -p ~/nosqlbench
         ```
      - On your local machine, use SCP to upload the `nb5.jar` file to the same directory on the VM:
         ```sh
         gcloud compute scp --recurse /local/path/nosqlbench user_name@vm_name:/home/user_name/nosqlbench
         ```
         This will upload all the files located at your local `nosqlbench` directory to VM inside `nosqlbench` directory


3. **Install Java**:

   - Ensure Java is installed and accessible from the command line.
   - You can check if Java is installed by running:
     ```sh
     java -version
     ```
   - **Note**: Java 11 is sufficient, but Java 17 is installed on this VM as it is the latest LTS (Long-Term Support) version and provides enhanced performance and security features.

4. **Verify NoSQLBench Installation**:
   - Run NoSQLBench with the following command to verify the installation:
     ```sh
     java -jar nb5.jar
     ```

## Setup and Configuration

### Configuring the Workload File

- The `bigtable_workload.yaml` file is a crucial part of running NoSQLBench against your Bigtable instance. This file contains various scenarios, bindings, parameters, and blocks for running different operations on your Bigtable instance.

- **Sample Configuration**:
  - **Scenarios**: Define different operations such as `insert-only`, `select-prepare-only`, `main-crud`, and more.
  - **Bindings**: Variables like `name`, `age`, `code` are defined here to be used within the scenarios.
  - **Parameters**: Global settings like consistency level (`cl`) and timeouts.
  - **Blocks**: Specific database operations like creating keyspaces, tables, and running CRUD operations.

### Running NoSQLBench

1. **Navigate to the Directory**:

   - Go to the directory where `nb5.jar` and `bigtable_workload.yaml` are located.

2. **Run the Workload**:
   - Use the following command to run the workload:
     ```sh
     java -jar nb5.jar run driver=cqld4 workload=bigtable_workload.yaml
     ```
   - This will execute the scenarios defined in the `bigtable_workload.yaml` file against your Bigtable instance.

### Scenarios and Commands

The following are the scenarios defined in the `bigtable_workload.yaml` file, along with the corresponding commands to execute each scenario:

1. **Insert Prepare Only**:

   - **Description**: Inserts data into the `user_info` table using prepared statements.
   - **Command**:
     ```sh
     java -jar nb5.jar bigtable_workload.yaml insert-prepare-only host=LOCALHOST port=9042 driver=cqld4 --report-csv-to reports/insert-prepare-only/ --progress console:1s localdc=datacenter1 --log-histograms histodata.log
     ```

2. **Select Prepare Only**:

   - **Description**: Selects data from the `user_info` table using prepared statements.
   - **Command**:
     ```sh
     java -jar nb5.jar bigtable_workload.yaml select-prepare-only host=LOCALHOST port=9042 driver=cqld4 --report-csv-to reports/select-prepare-only/ --progress console:1s localdc=datacenter1 --log-histograms histodata.log
     ```

3. **Update Prepare Only**:

   - **Description**: Updates data in the `user_info` table using prepared statements.
   - **Command**:
     ```sh
     java -jar nb5.jar bigtable_workload.yaml update-prepare-only host=LOCALHOST port=9042 driver=cqld4 --report-csv-to reports/update-prepare-only/ --progress console:1s localdc=datacenter1 --log-histograms histodata.log
     ```

4. **Delete Prepare Only**:

   - **Description**: Deletes data from the `user_info` table using prepared statements.
   - **Command**:
     ```sh
     java -jar nb5.jar bigtable_workload.yaml delete-prepare-only host=LOCALHOST port=9042 driver=cqld4 --report-csv-to reports/delete-prepare-only/ --progress console:1s localdc=datacenter1 --log-histograms histodata.log
     ```

5. **Main CRUD**:
   - **Description**: Executes a series of CRUD (Create, Read, Update, Delete) operations on the `user_info` table using prepared statements.
   - **Command**:
     ```sh
     java -jar nb5.jar bigtable_workload.yaml main-crud host=LOCALHOST port=9042 driver=cqld4 --report-csv-to reports/main-crud/ --progress console:1s localdc=datacenter1 --log-histograms histodata.log
     ```

## Troubleshooting

- **Java Not Found:**
  - Ensure Java is installed and the `JAVA_HOME` environment variable is set correctly.
- **NoSQLBench Errors:**
  - Check the syntax in your `bigtable_workload.yaml` file.
  - Ensure the Bigtable instance is accessible and the correct permissions are set.
