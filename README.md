# data-science-summit-airflow
Welcome to Airflow training session for Nov 2017 Data Science Summit in London! We hope that you will find the training useful and you will take the knowledge you obtained to the next level which will result in very interesting and exciting projects! 

Please read the content of this page carefully and make sure that you have everything ready before the session. 

If you have any questions about the training, please direct them to Dmitri Safin (dmitri.safin@umusic.com) and cc Chris Blakey (Christopher.Blakey@umusic.com)

> Note that all the instructions here are provided assuming that you have Mac laptop. If you have Windows, please let us know and we'll add instructions for Windows.

## Prerequisites
### Skills
This is a hands-on deep dive training and the audience is expected to have the following skills:
* Python Programming
* Google Cloud 
  * Launch a new GCE instance
  * Configure gcloud  
* Basic Unix skills: 
  * SSH into an instance
  * SCP files into an instance
* Git and GitHub
  * All the code samples and scripts for the training are checked into this repository, so you are expected to clone it to your local folder. 
* Docker (nice to have)
  * The version of the Airflow we are going to be using runs in Docker containers, so it's nice to have an idea on what Docker is. If not, the commands will be given, so it's not mandatory

### Google Cloud Project
You are welcome to do the labs from your own project on GCP, but if you don't have access to any GCP project, please let Dmitri Safine or Chris Blakey know ahead of time and we'll provide the access to `umg-data-science` project for you. 

### Sample data
We are going to be using data from Spotify Datamart, residing in `umg-partner` BigQuery project. If you don't have access to this project, let us know ahead of the training.

### BigQuery dataset
We are going to be using a new dataset in BigQuery to keep all the tables used in this traning. You are welcome to create a new dataset in your own BigQuery project, or let us know and we'll create a dataset under `umg-data-science` project in BigQuery for you.

### Firewall
If you are using your own GCP project, the TCP ports 8080 and 5555 should be allowed for external access from within UMG London Office network. Please contact us if you need help with this. 

## Labs
The following lab exercises are available as part of the training:

1. [Launching Airflow Server on Google Cloud](dags/lab1)
2. [Using BashOperator and WGET to download files](dags/lab2)
3. [Using PythonOperator and community contributed operators to upload file it to GCS and import to BigQuery](dags/lab3)
4. [Building GCS Sensor](dags/lab4)

