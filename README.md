# Example of running the spark job on dataproc #

## Prerequisites ##
This example requires _gradle_ and _python 3.6_ to be installed.

## Running the job ##
### Build the jar file ###
`$ ./gradlew clean shadowJar`

### Configure python virtual environment ###
`$ ./python_setup.sh`

### Activate virtual environment ###
`$ source venvs/python3.6/bin/activate`

### Run the script ###
`$ ./deploy_jar.py --project=my-dataproc-project --key=my-dataproc-project-key.json --bucket=my-dataproc-bucket [upload] [start_cluster] [submit_job] [delete_cluster]`
Commands in [] are optional and script supports chaining commands

### Deactivate python virtual environment
`$ deactivate`
