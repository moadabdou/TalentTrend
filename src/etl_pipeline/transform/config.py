# Tech Stack Dictionary
# Mapping canonical names to list of variations/synonyms
SKILL_KEYWORDS = {
    "Python": ["python", "py"],
    "JavaScript": ["javascript", "js", "es6", "node.js", "nodejs", "typescript", "ts"],
    "React": ["react", "reactjs", "react.js"],
    "Vue": ["vue", "vuejs", "vue.js"],
    "Angular": ["angular", "angularjs"],
    "Java": ["java", "jvm"],
    "C++": ["c++", "cpp"],
    "C#": ["c#", "csharp", ".net", "dotnet"],
    "Go": ["golang", "go"],
    "Rust": ["rust"],
    "Ruby": ["ruby", "rails"],
    "PHP": ["php", "laravel"],
    "SQL": ["sql", "mysql", "postgresql", "postgres"],
    "NoSQL": ["nosql", "mongodb", "mongo", "cassandra", "redis"],
    "AWS": ["aws", "amazon web services", "ec2", "lambda", "s3"],
    "GCP": ["gcp", "google cloud"],
    "Azure": ["azure"],
    "Docker": ["docker"],
    "Kubernetes": ["kubernetes", "k8s"],
    "Terraform": ["terraform"],
    "Linux": ["linux", "unix", "ubuntu", "debian", "centos", "redhat"],
    "Git": ["git", "github", "gitlab"],
    "Machine Learning": ["machine learning", "ml", "tensorflow", "pytorch", "scikit-learn", "sklearn", "keras"],
    "Deep Learning": ["deep learning", "dl", "neural network"],
    "Data Science": ["data science", "data scientist", "pandas", "numpy"],
    "Big Data": ["big data", "hadoop", "spark", "kafka"],
    "iOS": ["ios", "swift", "objective-c"],
    "Android": ["android", "kotlin"],
    "Flutter": ["flutter", "dart"],
    "React Native": ["react native"],
}

# Role Classification Keywords (Priority Order is handled in logic, this is just for reference/grouping if needed)
# The logic will use these keywords to classify.
ROLE_KEYWORDS = {
    "Data/AI": ["machine learning", "ai", "data scientist", "data engineer", "computer vision", "nlp"],
    "DevOps": ["devops", "sre", "site reliability", "kubernetes", "platform engineer", "infrastructure"],
    "Mobile": ["ios", "android", "swift", "kotlin", "mobile"],
    "Frontend": ["frontend", "front-end", "react", "vue", "angular", "ui/ux", "web developer"],
    "Backend": ["backend", "back-end", "java", "go", "rust", "api", "distributed systems", "scala", "ruby", "php"],
    "Fullstack": ["fullstack", "full-stack"],
}

# Experience Level Keywords
SENIORITY_KEYWORDS = ["senior", "lead", "principal", "staff", "architect"]
JUNIORITY_KEYWORDS = ["junior", "entry-level", "entry level", "intern", "associate", "new grad", "fresh grad"]
MANAGEMENT_KEYWORDS = ["manager", "director", "vp", "head of", "founder", "chief", "cto", "ceo", "engineering manager"]

# Location Keywords
TIER_1_CITIES = ["san francisco", "sf", "nyc", "new york", "seattle", "bay area", "silicon valley", "london"] # London is often high tier too, but user put it in Europe. I'll follow user's list for now but keep London in Europe list as requested.
EUROPE_LOCATIONS = ["london", "berlin", "paris", "amsterdam", "dublin", "remote eu", "europe"]
GLOBAL_REMOTE_KEYWORDS = ["remote worldwide", "global remote", "anywhere", "remote (global)"]

# Company Stage Keywords
YC_KEYWORDS = ["yc", "y combinator", "ycombinator"]
FUNDING_KEYWORDS = ["series a", "series b", "series c", "raised", "funding", "backed"]
CRYPTO_KEYWORDS = ["web3", "crypto", "blockchain", "defi", "ethereum", "solana", "bitcoin", "smart contract"]

# Compensation Keywords
EQUITY_KEYWORDS = ["equity", "stock options", "rsu", "stock", "ownership", "%"]
VISA_KEYWORDS = ["visa", "sponsorship", "h1b", "relocation"]
