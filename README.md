# Melbourne-GTFS
Real-time dashboard for Melbourne Metro and Vline Transport.
The aim of this project is create live dashboard that can allow user to idenitfy which metro and Vline train routes are busier helping them to manage their schedule along with live vehicle location.
### Technologies used

- Apache Kafka - For handling GTFS data i.e events

- Apache Flink - Performing aggregation for the live data.

- Postgres - Storing Data

- Apache Airflow - For scheduling data from VicRoads GTFS API.

- Terraform - Getting necessary resources from GCP

- Ansible - Instaling kafka and flink on VM.

> Planning to use Apache Iceberg and PowerBI when I reach at that stage

## How to Install Flink cli

Download lates version of flink [Download](https://flink.apache.org/downloads/) \
Extract flink 

```console
export $FLINK_HOME= <Flink location extracted>
export PATH=$PATH:$FLINK_HOME/bin
```
edit the /.zshrc 
```console
nano ~/.zshrc
```
Add the path of flink and reload using the below command

```console
source ~/.zshrc
```
Check the if flink is installed 

```console
flink -v
```

**For airflow follow the astronomer guide to install and run locally [follow](https://www.astronomer.io/docs/astro/cli/install-cli/)**
