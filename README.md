## Installation

You may need to install python on the machine. I used version 3.8 for this task. It can be downloaded from [HERE](https://www.python.org/downloads/). 
Or use CMD for linux:
```bash
sudo apt install python3.8
```
There is one dependacy that should be installed:
```bash
python3 -m pip install -r requirements.txt
```
or
```bash
python3 -m pip install pandas
```
Depending on the machine, instead of python3, another python alias should be used. E.g. python or python3.8

## How to run
You need to specify path to historical data in `src/configs.py`. By default it's `historical_data`.
This folder should be on the same level as `src`. It accepts raw data in format of JSONs. Files should be also placed into year folders, e.g.:
`historical_data/2022/01.json`
`historical_data/2023/10.json`

in order to run, execute from project's root folder:
```bash
python3 main.py
```
