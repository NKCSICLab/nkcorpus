# Common Crawl Downloader

Languages: [English](https://github.com/AlumiK/comcrawl-downloader/blob/main/README.md) | [中文](https://github.com/AlumiK/comcrawl-downloader/blob/main/README_CN.md)

![python-3.7-3.8-3.9](https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9-blue)
[![license-MIT](https://img.shields.io/badge/license-MIT-green)](https://github.com/AlumiK/comcrawl-downloader/blob/main/LICENSE)

Distributed download scripts for Common Crawl data.

## Dependencies

The downloader requires Python >= 3.7 to run.

Install dependencies by:

```
pip install -r requirements.txt
```

Please note that `libmysqlclient-dev` may also be required on Ubuntu：

```
sudo apt install libmysqlclient-dev
```

You can install equivalent packages on other Linux distros by your self.

## Run

### Configurations

The default config file is located at `configs/default.conf`, which lists all the modifiable entries. Their descriptions and default values are listed below:

```ini
[database]
drivername = mysql
username = user
password = password
host = localhost
port = 3306
database = comcrawl_data

[worker]
; The name of this worker, used to identify this worker in the database
name = unknown
; The interval of retries in seconds
retry_interval = 5
; The number of retries before exit the program
retries = 10
; The internet connection timeout in seconds
socket_timeout = 30
; The download root path
download_path = downloaded

[schedule]
; Whether to restrict download time
enabled = false
; The start of the allowed download time
start_time = 20:00:00
; The end of the allowed download time
end_time = 07:59:59
; The interval of retries when the download is restricted
retry_interval = 300
```

Please **do not** modify the default config file directly. You can create a `local.conf` under the `configs` folder and add the entries you want to modify in it.

Below is an example of a valid config file:

```ini
[database]
username = comcrawl
password = &WcKLEsX!
host = 58.250.74.108

[schedule]
enabled = true
start_time = 20:00:00
end_time = 07:59:59
```

### Execute the download script

Run the following command at the root path of the project:

```
python src/main.py
```

## Database Structure

### data

| Field | Type | Description |
| :- | :- | :- |
| id | int | **Primary Key** Data ID |
| uri | varchar(256) | The URI of the data. It also constitutes the download URL and the folder structure |
| size | int | The size of the data in bytes |
| started_at | datetime | Download start time (CST) |
| finished_at | datetime | Download end time (CST) |
| download_state | tinyint | Download state <br/>`0` for pending<br/>`1` for downloading<br/>`2` for finished<br/>`3` for failed |
| id_worker | int | **Foreign Key** The ID of the worker that downloads this data |
| archive | varchar(30) | The year and month of this data on Common Crawl |

### worker

| Field | Type | Description |
| :- | :- | :- |
| id | int | **Primary Key** Worker ID |
| name | varchar(128) | The name of the worker |
