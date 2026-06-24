**Project**: Aisera Migration – Data Cloud integration for Slack data

**summary**
Worked as a data integration engineer, building and automating the pipeline to migrate unstructured Slack data into Workday Data Cloud for downstream analytics and Aisera use cases.

**What I built**

Developed Python/PySpark scripts to convert Slack JSON/.txt exports into clean, structured CSV files for ingestion.

Adapted and fixed an offshore‑provided PySpark script (syntax fixes, dependency setup, requirement changes) to reliably generate CSV output from Slack data.

Designed the flow to move data from S3 to ADL (with GTM team support) and feed it into Data Cloud via the Slack data ingestion APIs.

Created a repeatable process to handle channel‑level JSON exports and transform them into standardized CSV datasets.

**Key challenges I faced**

Handling unstructured and inconsistent Slack export formats (.txt and JSON) and normalizing them into a stable CSV schema.

Resolving PySpark script issues (syntax errors, missing dependencies, environment setup) and aligning the logic with our business requirements.

Coordinating with the Slack team and channel owners to get the correct list of channels and data exports, which are required before conversion.

Orchestrating cross‑team steps (Slack team, GTM team, Data Cloud) so that S3 → ADL → Data Cloud ingestion works end‑to‑end.

**Tools and tech stack**

Languages: Python, PySpark

Data formats: JSON, .txt, CSV

Platforms: AWS S3, Azure Data Lake (ADL), Workday Data Cloud

Integrations/APIs: Slack data exports and ingestion APIs

Collaboration/process: Slack, ServiceNow (SNOW tickets), coordination with GTM and Slack teams
