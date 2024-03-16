# Data Collection Pipeline for SchemaPile

Below you can find the instruction to re-create the SchemaPile corpus, starting from a [list of urls](sqlfiles-and-licenses.md) that we provide.

## Run
Runs on Windows / Linux / MacOS with Python >= 3.8; >= 32GB RAM and >=50GB disk space recommended

Install the required dependencies:

`pip3 install -r requirements.txt`

Run the full collection process: 

`python3 data_collection_pipeline.py`

*Note: The download process is very time-consuming (expect 1.5 - 2 days runtime). To prevent rate limiting, we only use 10 concurrent download threads. The script can be stopped and started again, and will automatically continue the download process where it stopped.*


To run the collection process only for the first n files, add a cutoff parameter (e.g. 100):

`python3 data_collection_pipeline.py 100`

