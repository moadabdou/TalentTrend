import pandas as pd
import os
from datetime import datetime
from src.etl_pipeline.transform.extractors import (
    parse_salary, extract_skills, classify_role, extract_company, clean_text,
    extract_experience_level, extract_location_features, extract_company_stage, extract_compensation_features
)

# Paths
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), "data")
INPUT_FILE = os.path.join(DATA_DIR, "hn_jobs_raw_2020-01-01_2025-12-01.parquet")
OUTPUT_FILE = os.path.join(DATA_DIR, "hn_jobs_structured.parquet")

def run_transform_pipeline(input_path: str, output_path: str, sample_size: int = None):
    print(f"Loading data from {input_path}...")
    try:
        df = pd.read_parquet(input_path)
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_path}")
        return

    print(f"Initial rows: {len(df)}")

    # 1. Sanitation Layer
    # Deduplication
    # Assuming input has 'id', 'text', 'date' (or similar)
    # Map column names if necessary. Let's assume standard names or inspect.
    # For this implementation, I'll assume columns are 'id', 'text', 'date' based on typical scraping results.
    # If 'raw_text' is the name, I'll rename or use it.
    
    if 'raw_text' not in df.columns and 'text' in df.columns:
        df.rename(columns={'text': 'raw_text'}, inplace=True)

    if 'date' not in df.columns and 'thread_date' in df.columns:
        df.rename(columns={'thread_date': 'date'}, inplace=True)
    
    if 'raw_text' not in df.columns:
        print("Error: 'raw_text' column not found.")
        return

    # Dedupe by ID
    df.drop_duplicates(subset=['id'], inplace=True)
    
    # Dedupe by content within same month (Secondary Check)
    # Ensure date is datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        # Create a month identifier for deduplication
        df['month_str'] = df['date'].dt.to_period('M')
        df.drop_duplicates(subset=['month_str', 'raw_text'], keep='first', inplace=True)
        df.drop(columns=['month_str'], inplace=True)
    
    # Spam Filter
    # Drop rows where length < 50
    df = df[df['raw_text'].str.len() >= 50]
    # Drop rows starting with >
    df = df[~df['raw_text'].str.strip().str.startswith('>')]

    print(f"Rows after sanitation: {len(df)}")

    if sample_size:
        print(f"Running on sample of {sample_size} rows for validation...")
        df = df.head(sample_size).copy()

    # 2. Transformation
    print("Applying transformations...")
    
    # Apply extractors
    # We need to generate: company_name, role_title, salary_min, salary_max, salary_avg, currency, is_remote, tech_stack, job_category
    
    # Pre-calculate to avoid repeated calls
    df['clean_text'] = df['raw_text'].apply(clean_text)
    
    # Salary
    salary_data = df['clean_text'].apply(parse_salary)
    df['salary_min'] = salary_data.apply(lambda x: x[0])
    df['salary_max'] = salary_data.apply(lambda x: x[1])
    df['currency'] = salary_data.apply(lambda x: x[2])
    
    # Calculate Avg
    df['salary_avg'] = (df['salary_min'] + df['salary_max']) / 2
    df['salary_avg'] = df['salary_avg'].fillna(0).astype(int) # Fillna 0 for int conversion, then replace? 
    # Actually schema says nullable int. Pandas nullable int is 'Int64'.
    
    # Tech Stack
    df['tech_stack'] = df['clean_text'].apply(extract_skills)
    
    # Role Classification
    df['job_category'] = df['clean_text'].apply(classify_role)
    
    # Other fields
    df['is_remote'] = df['clean_text'].str.lower().str.contains("remote")
    df['company_name'] = df['clean_text'].apply(extract_company)
    
    # Role Title - Heuristic: similar to company, maybe 2nd part of pipe? 
    # Or just leave null as it's hard to extract without NER.
    # Spec says "Extracted job title (e.g., "Senior Backend Eng")".
    # I'll try a simple heuristic: 2nd part of pipe if exists.
    def extract_role_title(text):
        parts = text.split('|')
        if len(parts) > 1:
            return parts[1].strip()
        return None
    df['role_title'] = df['clean_text'].apply(extract_role_title)

    # Experience Level
    exp_data = df['clean_text'].apply(extract_experience_level)
    df['is_senior'] = exp_data.apply(lambda x: x['is_senior'])
    df['is_junior'] = exp_data.apply(lambda x: x['is_junior'])
    df['is_manager'] = exp_data.apply(lambda x: x['is_manager'])
    df['years_experience'] = exp_data.apply(lambda x: x['years_experience'])

    # Location Features
    loc_data = df['clean_text'].apply(extract_location_features)
    df['is_tier_1_city'] = loc_data.apply(lambda x: x['is_tier_1_city'])
    df['is_europe'] = loc_data.apply(lambda x: x['is_europe'])
    df['is_global_remote'] = loc_data.apply(lambda x: x['is_global_remote'])

    # Company Stage
    stage_data = df['clean_text'].apply(extract_company_stage)
    df['is_yc'] = stage_data.apply(lambda x: x['is_yc'])
    df['is_funded'] = stage_data.apply(lambda x: x['is_funded'])
    df['is_crypto'] = stage_data.apply(lambda x: x['is_crypto'])

    # Compensation Structure
    comp_data = df['clean_text'].apply(extract_compensation_features)
    df['has_equity'] = comp_data.apply(lambda x: x['has_equity'])
    df['offers_visa'] = comp_data.apply(lambda x: x['offers_visa'])

    # Interaction Features
    # tech_combo_ai: is_python AND (is_pytorch OR is_llm)
    def check_skill(stack, skill):
        return skill in stack

    df['is_python'] = df['tech_stack'].apply(lambda x: check_skill(x, 'Python'))
    df['is_rust'] = df['tech_stack'].apply(lambda x: check_skill(x, 'Rust'))
    
    df['has_pytorch_or_llm'] = df['clean_text'].str.lower().str.contains(r'pytorch|llm|large language model', regex=True)
    df['tech_combo_ai'] = df['is_python'] & df['has_pytorch_or_llm']
    
    df['tech_combo_blockchain'] = df['is_rust'] & df['is_crypto']

    # 3. Formatting & Schema Enforcement
    # Select and Order columns
    # id, date, company_name, role_title, salary_min, salary_max, salary_avg, currency, is_remote, tech_stack, job_category
    
    # Ensure types
    # Pandas nullable integers
    df['salary_min'] = df['salary_min'].astype('Int64')
    df['salary_max'] = df['salary_max'].astype('Int64')
    df['salary_avg'] = df['salary_avg'].replace(0, pd.NA).astype('Int64') # Fix the 0 fill above
    df['years_experience'] = df['years_experience'].astype('Int64')
    
    final_cols = [
        'id', 'date', 'raw_text', 'company_name', 'role_title', 
        'salary_min', 'salary_max', 'salary_avg', 'currency', 
        'is_remote', 'tech_stack', 'job_category',
        'is_senior', 'is_junior', 'is_manager', 'years_experience',
        'is_tier_1_city', 'is_europe', 'is_global_remote',
        'is_yc', 'is_funded', 'is_crypto',
        'has_equity', 'offers_visa',
        'tech_combo_ai', 'tech_combo_blockchain'
    ]
    
    # Filter columns that exist (id, date should be there)
    available_cols = [c for c in final_cols if c in df.columns]
    final_df = df[available_cols]
    
    print("Transformation complete.")
    print(final_df.head())
    
    if not sample_size:
        print(f"Saving to {output_path}...")
        final_df.to_parquet(output_path)
        print("Done.")

if __name__ == "__main__":
    # Check if input file exists, if not create a dummy one for testing logic if needed, 
    # but for now assume user has it or we just run validation on empty/mock if file missing?
    # The user said "Input: hn_jobs_raw...". I'll assume it's there or I should mock it for the "Validation Step" if I can't find it.
    
    if not os.path.exists(INPUT_FILE):
        print(f"Warning: {INPUT_FILE} not found. Creating a mock file for validation.")
        # Create mock data
        mock_data = {
            'id': ['1', '2', '3', '4'],
            'date': [datetime(2023, 1, 1), datetime(2023, 1, 1), datetime(2023, 2, 1), datetime(2023, 2, 1)],
            'raw_text': [
                "Company A | Senior Engineer | Remote | $120k - $150k | Python, AWS",
                "Company B | Junior Dev | NYC | $80k+ | Javascript, React",
                "> This is a reply",
                "Short"
            ]
        }
        pd.DataFrame(mock_data).to_parquet(INPUT_FILE)
    
    # Run full pipeline
    print("Starting full transformation pipeline...")
    run_transform_pipeline(INPUT_FILE, OUTPUT_FILE)
