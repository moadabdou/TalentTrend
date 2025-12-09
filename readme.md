# TalentTrend

TalentTrend is a data analysis and machine learning project designed to track and analyze job market trends. It specifically targets "Who is hiring" threads from Hacker News to extract insights about in-demand skills, salaries, roles, and remote work opportunities.

## Project Overview

The project consists of three main components:
1.  **ETL Pipeline**:
    *   **Extract**: Scrapes "Who is hiring" threads from Hacker News.
    *   **Transform**: Cleans raw text, extracts structured data (skills, salary, location), and enriches the dataset.
2.  **Data Analysis**: Jupyter notebooks for exploring the processed data.
3.  **Machine Learning**: Models to predict trends or classify job postings.

## Project Structure

*   `src/etl_pipeline/`: Contains the extraction and transformation logic.
*   `src/analysis/`: Jupyter notebooks for data analysis.
*   `src/machine_learning/`: Jupyter notebooks for model training.
*   `data/`: Stores raw and processed data (Parquet and JSON files).

## Installation

1.  Clone the repository.
2.  Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### 1. Data Extraction
To fetch the latest "Who is hiring" threads from Hacker News:

```bash
python -m src.etl_pipeline.extract.main
```
This will download the data and save it to the `data/` directory.

### 2. Data Transformation
To process the raw data and extract structured information:

```bash
python -m src.etl_pipeline.transform.pipeline
```
This will generate a structured Parquet file (e.g., `hn_jobs_structured.parquet`) in the `data/` directory.

### 3. Analysis & Modeling
You can explore the data and train models using the provided Jupyter notebooks:
*   **Analysis**: Open `src/analysis/analysis.ipynb`
*   **Model Training**: Open `src/machine_learning/model_training.ipynb`

## Data
The data folder can be found at : https://drive.google.com/file/d/1NW41juhc1iXLhmiy_TGT_ht-fWw2tFM6/view?usp=sharing
 