# Lab 1. Launching Airflow Server on Google Cloud

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

> Note: if you don't have GitHub access set up via HTTPS, you may try an alternative command via SSH:
```
git clone git@github.com:umg/data-science-summit-airflow.git
```

If you have issues with cloning the repository, please refer to GitHub documentation on how to set up access when 2FA is enabled.
https://help.github.com/articles/providing-your-2fa-authentication-code/

2. Update `airflow-1.8.1.yml` file with your GCP project.
```
PROJECT_NAME=<your project>
```
You will have to update it in 3 places, under `webserver`, `scheduler` and `worker` entries.

3. Copy `airflow-1.8.1.yml` file into your home directory on the remote instance
```
$ scp -i ~/.ssh/<your private ssh key> airflow-1.8.1.yml <your username>@<server ip>:/home/<your username>
```

> Note: if you don't have SSH private key in `~/.ssh` directory, please refer to Google Cloud documentation on how to create the SSH keys.
https://cloud.google.com/compute/docs/instances/adding-removing-ssh-keys

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

> Note: password is not going to be visible when you type. 

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

Change permissions to the created folders:

```
$ sudo chmod 777 /opt/airflow/dags
$ sudo chmod 777 /opt/airflow/plugins
```

3. Launch the Airflow container with Docker Compose
```
$ sudo docker-compose -f airflow-1.8.1.yml up -d
```

### Step 7. Test Airflow UI

1. Go to http://instance_ip_address:8080 
You should see Airflow UI. At this point your Airflow server is up and running. Congratulations!

## Post installation procedures

### Step 8. Create service account key
This key will be used to connect to your GCP project from Airflow via API

1. Open terminal on your laptop
2. Initialize your Google Cloud SDK
```
$ gcloud init
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
> Note: this is just an example. Your project will have different accounts

4. Create a key for Compute Engine default service account. 

> Note: you should name the key file as `<your project name>-key.json` 
```
$ gcloud iam service-accounts keys create <your project>-key.json --iam-account=<Compute Engine default service account email>
```


5. Copy the key file to the Airflow instance
```
$ scp -i ~/.ssh/<your private ssh key> <your project name>-key.json <your username>@<server ip>:/home/<your username>
```

6. From SSH session on your instance copy the key file into `/opt/app` directory, so it can be picked up by Docker containers
```
$ sudo cp <your project name>-key.json /opt/app
```

7. Change permissions on your key file
```
$ sudo chmod 644 /opt/app/<your project name>-key.json
``` 

### Step 9. Create GCS project bucket for Airflow logs

1. From the terminal use `gsutil` command to create bucket and the folder for the logs
```
$ gsutil mb gs://<your project name>
```

> Note: if the bucket already exists you will receive an error about that and that's ok. 

### Step 10. Create BigQuery dataset
1. Open BigQuery UI in your project and create a new empty dataset. Call it `airflow_training_<your name>`

### Step 11. Configure Airflow Connections

### BigQuery Connection
1. From Airflow UI go to `Admin -> Connections`

2. Find `bigquery_default` connection and click `Edit`(pencil icon)

3. Add the following values to the connection properties
* Conn Name: `bigquery_default`
* Conn Type: `Google Cloud Platform`
* Project ID: `<your project>`
* Keyfile Path: `/opt/app/<your project name>-key.json`
* Scopes: `https://www.googleapis.com/auth/bigquery`

### Google Cloud Connection
1. From Airflow UI go to `Admin -> Connections`

2. Create `google_cloud_default` connection.

3. Add the following values to the connection properties
* Conn Name: `google_cloud_default`
* Conn Type: `Google Cloud Platform`
* Project ID: `<your project>`
* Keyfile Path: `/opt/app/<your project name>-key.json`
* Scopes: `https://www.googleapis.com/auth/devstorage.read_write`

### Step 12. Configure Airflow Variables
1. From Airflow UI go to `Admin -> Variables`

2. Create the following variables:
* Key: `spotify_streams_src`, Value: `umg-partner.spotify.streams`
* Key: `spotify_streams_dst`, Value: `<your project>.<your dataset>.spotify_streams`

### Step 13. Copy sample files

1. Copy `plugins/bq_hook.py`, `plugins/bq_plugin.py` and `dags/lab1.py` files into your home directory on the remote instance
```
$ scp -i ~/.ssh/<your private ssh key> plugins/bq_hook.py <your username>@<server ip>:/home/<your username>

$ scp -i ~/.ssh/<your private ssh key> plugins/bq_plugin.py <your username>@<server ip>:/home/<your username>

$ scp -i ~/.ssh/<your private ssh key> dags/lab1.py <your username>@<server ip>:/home/<your username>
```

2. From your SSH session copy the files from your home directory to Docker container volume folders

```
$ sudo cp bq_hook.py /opt/airflow/plugins

$ sudo cp bq_plugin.py /opt/airflow/plugins

$ sudo cp lab1.py /opt/airflow/dags

```

### Step 14. Restart Docker containers

1. From your SSH session on remote instance login as `airflow` user with `airflow` password and navigate to home directory
```
$ su airflow

$ cd ~
```

2. Restart Docker containers
```
$ sudo docker-compose -f airflow-1.8.1.yml restart
```


### Step 15. Test Lab1 DAG

1. Go to Airflow UI and make sure that you have `lab1` DAG appearing on the home page.

2. Toggle `On/Off` switch to turn the DAG on

3. Click `Run` button which looks like a play button to start the DAG

4. Click on the `lab1` DAG's name on the home page, to go inside the DAG

5. Refresh your browser to see how the DAG is being executed. 

6. If it finishes with `load_spotify_streams` box coloured in dark green, your set up is correct. Congratulations and you may open a bottle of champaign to celebrate!

7. If the `load_spotify_streams` box is red, click on the box, go to `View Log`, scroll all the way to the bottom and let us know what is the error reported. We will assist you. 







