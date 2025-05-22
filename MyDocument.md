# Project Reflections: NYC Jobs Data Pipeline

## 🧠 Key Learnings

- **Jupyter + Spark Version Constraints**  
  Since Jupyter was booted up with Spark 2.x, we couldn't take advantage of **Adaptive Query Execution (AQE)** which is available only in Spark 3.x and above.

- **Large File Handling Consideration**  
  If the input CSV is very large, Spark may struggle with single-file processing. We may request the upstream system to:
  - Break it down into multiple smaller files
  - Provide data in a more Spark-friendly format like **Parquet** or **ORC** to enable **partitioned parallelism**

- **Clean Code & Design Principles Applied**
  - Each part of the ETL pipeline (extraction, wrangling, transformation, etc.) is separated into its own class
  - This improves **modularity**, **testability**, and adheres to **SOLID** and **KISS** principles

- **CSV Parsing Nuances**
  - `quote = "\""`: Ensures values with embedded commas inside quotes are treated correctly
  - `escape = "\""`: Allows proper handling of nested quotes like `He said "hello"`
  - `multiLine = true`: Critical for parsing complex job descriptions that span multiple lines
  - `mode = "PERMISSIVE"`: Keeps processing robust by capturing corrupted rows rather than failing the job

- **Skill Extraction Using Regex + Heuristics**
  - Extracting skills from free-form text (e.g. bullet lists in job postings) involved cleaning, tokenization, and filtering for minimal length.
  - While basic regex works, advanced NLP would improve this significantly (see "Challenges").

## 🖼️ Visualizations

Bar charts were generated to explore various KPIs and salary insights, and saved to:

📁 `/dataset/output/charts/`

### Plots created:
- **Top 10 Job Categories by Count**: `jobs_per_category.png`
- **Average Salary per Job Category**: `salary_distribution.png`
- **Degree Level vs Salary**: `degree_vs_salary.png`
- **Highest Paid Skills**: `highest_paid_skills.png`
- **Highest Salary per Agency**: `max_salary.png`
- **Avg Salary in Last 2 Years per Agency**: `avg_salary_2y.png`

## 🧩 Modular Class-Based Code Architecture

| Class Name           | Responsibility                             |
|----------------------|---------------------------------------------|
| `SparkFactory`        | Creates a SparkSession instance             |
| `JobDataExtractor`    | Reads the CSV file into a DataFrame         |
| `JobDataWrangler`     | Cleans and standardizes column names        |
| `JobDataTransformer`  | Adds derived columns, transforms text data  |
| `JobDataValidator`    | Validates key fields like salary and agency |
| `JobDataLoader`       | Writes processed DataFrame to target file   |
| `JobDataProfiler`     | Analyzes schema, nulls, and column types    |
| `JobKPI`              | Computes various business KPIs              |
| `JobVisualizer`       | Converts KPI data into charts               |

## ✅ Assumptions

- Input CSV is encoded using **ISO-8859-1** (Latin-1) instead of UTF-8 to handle extended characters
- Not all job postings will have salary or skills — handled nulls gracefully
- Minimal downstream consumers — so output format was kept simple (CSV)
- Only the most relevant fields were selected based on data profiling
- `avgSalary` was used as a normalized salary metric across the dataset

## ⚠️ Challenges

- **Spacy Installation Failure**
  - Couldn’t install **spaCy** due to an old Jupyter image/environment issue. Limited advanced NLP techniques.

- **Schema Inference vs Explicit Types**
  - Spark’s `inferSchema` can misclassify types; explicit schemas are preferred for production.

- **UDF Performance Overhead**
  - Python UDFs (e.g. skill extraction) are slower than native Spark functions. Needs optimization if scaling.

## 🔄 Additional Considerations

- **Reproducibility**: Use time-stamped or versioned outputs
- **Testing**: UDF-heavy logic needs more robust integration testing
- **Deployment**: Consider Dockerizing the notebook and triggering via Airflow, Databricks Jobs, or `spark-submit`
- **Observability**: Logging was added; could be extended with Prometheus + Grafana

## 🚀 Triggering the Pipeline

To automate or trigger the notebook:
- Use **Papermill** or **nbconvert** to schedule the Jupyter notebook
- Alternatively, extract the classes and logic into a `.py` script and run via `spark-submit`
- Consider building a CLI using `argparse` to allow different job modes (extract-only, full run, profile-only, etc.)

## 📦 Final Output

- Processed dataset stored in `/dataset/output/` as partitioned, cleaned CSV
- Charts stored in `/dataset/output/charts/`
- Full ETL pipeline implemented in an object-oriented, modular format
- Logs embedded across each step to ensure traceability

## 📊 Output Visualizations

![Jobs per Category](/dataset/output/charts/jobs_per_category.png)
![Salary Distribution](/dataset/output/charts/salary_distribution.png)
![Degree vs Salary](/dataset/output/charts/degree_vs_salary.png)
![Max Salary per Agency](/dataset/output/charts/max_salary.png)
![Average Salary Last 2 Years](/dataset/output/charts/avg_salary_2y.png)
![Highest Paid Skills](/dataset/output/charts/highest_paid_skills.png)
