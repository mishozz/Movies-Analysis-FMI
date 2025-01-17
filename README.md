# IMDb Dataset Analysis

## Description

This project aims to analyze the [IMDb titles dataset](https://www.kaggle.com/datasets/ashirwadsangwan/imdb-dataset) to extract meaningful insights and visualizations. The analysis includes:

- Rating distribution of the titles
- Trends in releases over the years
- Genre analysis by average rating

The data is analyzed using [Apache Spark](https://spark.apache.org/docs/latest/api/python/index.html), and the tasks are orchestrated by an [Apache Airflow](https://airflow.apache.org/) pipeline. The results of the analysis are compiled into a comprehensive report in PDF format *(imbd_analysis_visualized.pdf)*, which includes various plots and visualizations to facilitate a better understanding of the data.

## Local Setup

This project requires downloading and preparing a dataset from Kaggle. Follow the steps below to set up the project locally:

### Prerequisites

1. **Python3**
2. **Pip3**
3. **8 GB of free space for the IMBd dataset**

### Install python dependencies
```sh
pip3 install -r requirements.txt
```

### Download the IMBd dataset

```sh
python3 setup_dataset.py
```

### Start Airflow

```sh
make airflow
```

### Access Airflow UI
`http://localhost:8080/`


* The end result of the pipeline is a PDF file named `report.pdf`, which contains all the plots from the dataset analysis.

* The pipeline utilizes an "in-memory" database abstraction using Parquet files to store the state between the pipeline steps.

## Running Unit Tests
To run the tests, use the following command:
```sh
make test
```