# Scraper

## Architecture

Prefect for orchestration

AWS S3 for scraped website storage (could be minio if selfhosted)

MotherDuck for cleaned input and share values

## Data Flows

### Fetching raw content

- Goal: Get the (text) content of all websites from all listed companies

- SubFlow: Get all websites of possible companies

### Crawl Content

- Goal: Search through the text of all previously downloaded websites and mark all of them, that contain the term searched for
-> Save the found company names in warehouse

### Get new share values

- Goal: Download all share values 


## prefect

I'm using prefect to manage the webscraping workflow. 

### commands

prefect-cloud login

prefect work-pool create machete-pool --type prefect:managed

