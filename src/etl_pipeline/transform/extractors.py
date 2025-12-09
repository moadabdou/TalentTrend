import re
from typing import Tuple, List, Optional, Dict
from src.etl_pipeline.transform.config import (
    SKILL_KEYWORDS, ROLE_KEYWORDS,
    SENIORITY_KEYWORDS, JUNIORITY_KEYWORDS, MANAGEMENT_KEYWORDS,
    TIER_1_CITIES, EUROPE_LOCATIONS, GLOBAL_REMOTE_KEYWORDS,
    YC_KEYWORDS, FUNDING_KEYWORDS, CRYPTO_KEYWORDS,
    EQUITY_KEYWORDS, VISA_KEYWORDS
)

def clean_text(text: str) -> str:
    """
    Basic text normalization.
    """
    if not text:
        return ""
    return text.strip()

def parse_salary(text: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """
    Extracts salary information using a waterfall strategy.
    Returns (min_salary, max_salary, currency).
    """
    if not text:
        return None, None, None

    text_lower = text.lower().replace(",", "")
    
    # Currency detection
    currency = "USD"
    if "£" in text or "gbp" in text_lower:
        currency = "GBP"
    elif "€" in text or "eur" in text_lower:
        currency = "EUR"
    
    # Helper to convert k to 000
    def parse_value(val_str):
        try:
            val_str = val_str.lower().replace("k", "000").replace("$", "").replace("£", "").replace("€", "")
            return int(float(val_str))
        except ValueError:
            return None

    # Pattern 1: Explicit Range ($100k - $140k or 100-140k)
    # Matches: $100k-$140k, 100k-140k, 100-140k, $100,000 - $140,000
    range_pattern = re.search(r'[\$€£]?(\d{2,3}k?|\d{5,6})\s*-\s*[\$€£]?(\d{2,3}k?|\d{5,6})', text_lower)
    if range_pattern:
        min_val = parse_value(range_pattern.group(1))
        max_val = parse_value(range_pattern.group(2))
        
        # Handle case where 'k' is only on the second number (e.g. 100-140k)
        if min_val and max_val:
            if min_val < 1000 and max_val > 1000:
                 min_val *= 1000
            
            # Guardrails
            if min_val < 200: # Hourly trap
                min_val *= 2000
                max_val *= 2000
            
            if min_val > 50: # Equity trap check (simple)
                return min_val, max_val, currency

    # Pattern 2: "Starting At" ($120k+ or from $120k)
    start_at_pattern = re.search(r'(?:from|starting at|\+)\s*[\$€£]?(\d{2,3}k|\d{5,6})\+?', text_lower)
    if not start_at_pattern:
         # Try pattern like $120k+
         start_at_pattern = re.search(r'[\$€£]?(\d{2,3}k|\d{5,6})\+', text_lower)

    if start_at_pattern:
        val = parse_value(start_at_pattern.group(1))
        if val:
            if val < 200: val *= 2000
            if val > 50:
                return val, val, currency

    # Pattern 3: Single Number (Low Confidence)
    # Look for isolated mentions of $150k
    single_pattern = re.search(r'[\$€£](\d{2,3}k|\d{5,6})', text_lower)
    if single_pattern:
        val = parse_value(single_pattern.group(1))
        if val:
            if val < 200: val *= 2000
            if val > 50:
                return val, val, currency

    return None, None, None

def extract_skills(text: str) -> List[str]:
    """
    Extracts tech stack entities based on dictionary.
    """
    if not text:
        return []
    
    found_skills = set()
    text_lower = text.lower()
    
    for skill, variations in SKILL_KEYWORDS.items():
        for variation in variations:
            # Regex word boundary check
            # Escape variation to handle special chars like c++ or .net
            pattern = r'\b' + re.escape(variation) + r'\b'
            if re.search(pattern, text_lower):
                found_skills.add(skill)
                break # Found the canonical skill, move to next skill
                
    return list(found_skills)

def classify_role(text: str) -> str:
    """
    Classifies role based on priority keywords.
    """
    if not text:
        return "General"
    
    text_lower = text.lower()
    
    # Priority 1: Data/AI
    for keyword in ROLE_KEYWORDS["Data/AI"]:
        if keyword in text_lower:
            return "Data/AI"
            
    # Priority 2: DevOps
    for keyword in ROLE_KEYWORDS["DevOps"]:
        if keyword in text_lower:
            return "DevOps"
            
    # Priority 3: Mobile
    for keyword in ROLE_KEYWORDS["Mobile"]:
        if keyword in text_lower:
            return "Mobile"
            
    # Priority 4: Web (Frontend)
    for keyword in ROLE_KEYWORDS["Frontend"]:
        if keyword in text_lower:
            return "Frontend"
            
    # Priority 5: Backend
    for keyword in ROLE_KEYWORDS["Backend"]:
        if keyword in text_lower:
            return "Backend"
            
    # Fallback
    if "fullstack" in text_lower or "full-stack" in text_lower:
        return "Fullstack"
        
    return "General"

def extract_company(text: str) -> Optional[str]:
    """
    Simple heuristic to extract company name.
    Assumes format often starts with "Company Name |" or similar.
    """
    if not text:
        return None
    
    # Heuristic: Split by pipe | and take first part if it's short enough
    parts = text.split('|')
    if len(parts) > 1:
        candidate = parts[0].strip()
        if len(candidate) < 50: # Arbitrary length check to avoid capturing long sentences
            return candidate
            
    return None

def extract_experience_level(text: str) -> Dict[str, any]:
    """
    Extracts seniority, juniority, management role, and years of experience.
    """
    if not text:
        return {
            "is_senior": False,
            "is_junior": False,
            "is_manager": False,
            "years_experience": None
        }
    
    text_lower = text.lower()
    
    is_senior = any(keyword in text_lower for keyword in SENIORITY_KEYWORDS)
    is_junior = any(keyword in text_lower for keyword in JUNIORITY_KEYWORDS)
    is_manager = any(keyword in text_lower for keyword in MANAGEMENT_KEYWORDS)
    
    # Extract years of experience
    # Regex: "(\d+)[\+]? years"
    years_exp = None
    years_match = re.search(r'(\d+)[\+]?\s*years?', text_lower)
    if years_match:
        try:
            years_exp = int(years_match.group(1))
        except ValueError:
            pass
            
    return {
        "is_senior": is_senior,
        "is_junior": is_junior,
        "is_manager": is_manager,
        "years_experience": years_exp
    }

def extract_location_features(text: str) -> Dict[str, bool]:
    """
    Extracts location tier information.
    """
    if not text:
        return {
            "is_tier_1_city": False,
            "is_europe": False,
            "is_global_remote": False
        }
        
    text_lower = text.lower()
    
    is_tier_1 = any(keyword in text_lower for keyword in TIER_1_CITIES)
    is_europe = any(keyword in text_lower for keyword in EUROPE_LOCATIONS)
    is_global_remote = any(keyword in text_lower for keyword in GLOBAL_REMOTE_KEYWORDS)
    
    return {
        "is_tier_1_city": is_tier_1,
        "is_europe": is_europe,
        "is_global_remote": is_global_remote
    }

def extract_company_stage(text: str) -> Dict[str, bool]:
    """
    Extracts company stage information (YC, Funded, Crypto).
    """
    if not text:
        return {
            "is_yc": False,
            "is_funded": False,
            "is_crypto": False
        }
        
    text_lower = text.lower()
    
    is_yc = any(keyword in text_lower for keyword in YC_KEYWORDS)
    is_funded = any(keyword in text_lower for keyword in FUNDING_KEYWORDS)
    is_crypto = any(keyword in text_lower for keyword in CRYPTO_KEYWORDS)
    
    return {
        "is_yc": is_yc,
        "is_funded": is_funded,
        "is_crypto": is_crypto
    }

def extract_compensation_features(text: str) -> Dict[str, bool]:
    """
    Extracts compensation structure (Equity, Visa).
    """
    if not text:
        return {
            "has_equity": False,
            "offers_visa": False
        }
        
    text_lower = text.lower()
    
    has_equity = any(keyword in text_lower for keyword in EQUITY_KEYWORDS)
    offers_visa = any(keyword in text_lower for keyword in VISA_KEYWORDS)
    
    return {
        "has_equity": has_equity,
        "offers_visa": offers_visa
    }
