# data-science-summit-airflow
Welcome to Airflow training session for Nov 2017 Data Science Summit in London! We hope that you will find the training useful and you will take the knowledge you obtained to the next level which will result in very interesting and exciting projects! 

Please read the content of this page carefully and make sure that you have everything ready before the session. 

If you have any questions about the training, please direct them to Dmitri Safin (dmitri.safin@umusic.com) and cc Chris Blakey (Christopher.Blakey@umusic.com)

Note that all the instructions here are provided assuming that you have Mac laptop. If you have Windows, please let us know and we'll add instructions for Windows.

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


## Launching Airflow Server on Google Cloud
You are welcome to try to create an instance of Airflow server ahead of the training, to save us some time. If you have troubles with it, please let us know, so we can assist you.

Our goal is to provide you with steps to create an Airflow instance that you can use for development and for production deployment. Therefore we'll ask you to do all the operations under `airflow` user, just to give ability to someone else on your team to do the same on the same instance without complication of providing access rights to multiple user accounts. 
 
### Step 1. Launch a new GCE Instance

1. Login to Google Cloud Console and launch a new instance. Call it `airflow-training-<your name>` and leave all the settings as default. 

2. SSH into the instance from your terminal


### Step 2. Install Docker CE

1. Update the `apt` package index 

```
$ sudo apt-get update
```

2. Install packages to allow apt to use a repository over HTTPS
```
$ sudo apt-get install \
     apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common
```

3. Add Docker’s official GPG key:
```
$ curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg | sudo apt-key add -
```

Verify that you now have the key with the fingerprint 9DC8 5822 9FC7 DD38 854A E2D8 8D81 803C 0EBF CD88, by searching for the last 8 characters of the fingerprint.
```
$ sudo apt-key fingerprint 0EBFCD88
```

It should return the following:
```
pub   rsa4096 2017-02-22 [SCEA]
      9DC8 5822 9FC7 DD38 854A  E2D8 8D81 803C 0EBF CD88
uid           [ unknown] Docker Release (CE deb) <docker@docker.com>
sub   rsa4096 2017-02-22 [S]
```
4. Set up the Docker repository
```
$ sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
   $(lsb_release -cs) \
   stable"
```

5. Refresh `apt` index again
```
$ sudo apt-get update
```
6. Install Docker
```
$ sudo apt-get install docker-ce
```

### Step 3. Install Docker Compose

1. Run this command to download the latest version of Docker Compose:
```
$ sudo curl -L https://github.com/docker/compose/releases/download/1.16.1/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
```

2. Apply executable permissions to the binary:
```
$ sudo chmod +x /usr/local/bin/docker-compose
```

3. Test the installation
```
$ docker-compose --version
docker-compose version 1.16.1, build 6d1ac21
```

### Step 4. Copy `airflow-1.8.1.yml` file to remote instance

1. Open another terminal window, clone this repository to your local folder and navigate inside
```
$ git clone https://github.com/umg/data-science-summit-airflow.git

$ cd data-science-summit-airflow

```

2. Update `airflow-1.8.1.yml` file with your GCP project.
```
PROJECT_NAME=<your project>
```
You will have to update it in 3 places, under `webserver`, `scheduler` and `worker` entries.

3. Copy `airflow-1.8.1.yml` file into your home directory on the remote instance
```
$ scp -i ~/<your private ssh key> airflow-1.8.1.yml <your username>@<server ip>:/home/<your username>
```

### Step 5. Create `airflow` user

1. Create an `airflow` user with home directory and Bash shell as default

```
$ sudo useradd -m -d /home/airflow -s /bin/bash airflow
```

2. Set `airflow` as a password
```
$ sudo passwd airflow
Enter new UNIX password: airflow
Retype new UNIX password: airflow
passwd: password updated successfully
```

Note: password is not going to be visible when you type. 

3. Provide `sudo` rigths to `airflow` user
```
$ sudo usermod -aG sudo airflow
```

4. Login as airflow user
```
$ su airflow
Password: 

```
5. Navigate to home directory
```
$ cd ~
```

### Step 6. Launch Airflow Docker container
1. Copy `airflow-1.8.1.yml` into `airflow` user home directory
```
$ sudo cp /home/<your user name>/airflow-1.8.1.yml ~
```

2. Create directories that will be mapped to container volumes
```
$ sudo mkdir /opt/app
$ sudo mkdir /opt/airflow
$ sudo mkdir /opt/airflow/dags
$ sudo mkdir /opt/airflow/plugins
```

3. Launch the Airflow container with Docker Compose
```
$ sudo docker-compose -f airflow-1.8.1.yml up -d
```

### Step 7. Test Airflow UI

1. Go to http://<instance ip address>:8080 
You should see Airflow UI. At this point your Airflow server is up and running. Congratulations!

## Post install procedures

### Create service account key
This key will be used to connect to your GCP project from Airflow via API

1. Open terminal on your laptop
2. Initialize your Google Cloud SDK
```
$gcloud init
```
Choose your project at the prompt

3. List service accounts
```
$ gcloud iam service-accounts list
```
You should see list of service accounts in your project

```
NAME                                                EMAIL
Compute Engine default service account              874919087886-compute@developer.gserviceaccount.com
svc-swift-alerts@umg-swift.iam.gserviceaccount.com  svc-swift-alerts@umg-swift.iam.gserviceaccount.com
svc-swift-api                                       svc-swift-api@umg-swift.iam.gserviceaccount.com
```
Note: this is just an example. Your project will have different accounts

4. Create a key for Compute Engine default service account
```
$ gcloud iam service-accounts keys create <your project>-key.json --iam-account=<Compute Engine default service account email>
```
Note: you should name the key file as <project name>-key.json 

5. Copy the key file to the Airflow instance
```
$ scp -i ~/<your private ssh key> <your project name>-key.json <your username>@<server ip>:/home/<your username>
```

6. From SSH session on your instance change permissions on your key file
```
$ sudo chmod 644 <your project name>-key.json
``` 

7. Place the key file into `/opt/app` directory, so it can be picked up by Docker containers
```
$ sudo cp <your project name>-key.json /opt/app
```

### Create BigQuery dataset
1. Open BigQuery UI in your project and create a new empty dataset. Call it `airflow_training_<your name>`

### Configure Airflow Connections

#### BigQuery Connection
1. From Airflow UI go to `Admin -> Connections`

2. Find `bigquery_default` connection and click `Edit`(pencil icon)

3. Add the following values to the connection properties
* Conn Name: `bigquery_default`
* Conn Type: `Google Cloud Platform`
* Project ID: `<your project>`
* Keyfile Path: `/opt/app/<your project>-key.json`
* Scopes: `https://www.googleapis.com/auth/bigquery`


### Configure Airflow Variables
1. From Airflow UI go to `Admin -> Variables`

2. Create the following variables:
* Key: `spotify_tracks_src`, Value: `umg-partner.spotify.tracks`
* Key: `spotify_tracks_dst`, Value: `<your project>.<your dataset>.spotify_tracks`

### Copy sample plugin








