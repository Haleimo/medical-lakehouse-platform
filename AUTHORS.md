# Project Authors & Team Contributions

This project, **Medical Data Lakehouse Platform**, is engineered as a collaborative production-grade portfolio project demonstrating end-to-end **DataOps**, **GitOps**, and **Data Lakehouse Architecture**.

---

## 👥 Authors & Team Leads

### 1. Abdelhaleem Mohammed ([@Haleimo](https://github.com/Haleimo))
* **Role**: Lead DevOps & GitOps Platform Engineer
* **Scope of Work**: Infrastructure as Code, CI/CD Pipelines, Security Scanning, Kubernetes Orchestration & ArgoCD Deployment Engine.

#### Key Contributions & Accomplishments:
* **Kubernetes Infrastructure**: Designed namespace architecture (`data-lake`), platform ConfigMaps, Secrets policies, and Helm chart implementations (`charts/garage/`).
* **Multi-Cloud CI/CD Automation**:
  * **GitHub Actions (`.github/workflows/`)**: Automated Python testing matrix (3.11/3.12), `ruff` code formatting, `helm lint` validation, and Trivy security scanning.
  * **GitLab CI (`.gitlab-ci.yml`)**: Built multi-stage container assembly pipeline (`lint`, `test`, `build`, `scan`, `package`, `deploy`) publishing Airflow images to GitLab Container Registry (`registry.gitlab.com`).
* **ArgoCD GitOps Architecture**: Engineered the master **App-of-Apps** pattern (`argocd/root-app.yaml`) and configured declarative **Sync Waves** (0 to 4) for seamless microservice dependency bootstrapping.
* **Airflow GitSync Handoff**: Integrated real-time `gitSync` sidecar in Airflow Helm configuration for zero-downtime, continuous DAG deployment.
* **Security & Operational Runbooks**: Authored enterprise DevOps & disaster recovery runbooks (`docs/DEVOPS_PRACTICE.md`) and secrets management specifications.

---

### 2. Mahmoud Saad ([@Mahmoud2saad](https://github.com/Mahmoud2saad))
* **Role**: Lead Data Engineer & Pipeline Architect
* **Scope of Work**: Ingestion Pipelines, FHIR/DICOM Validation, De-identification Engine, PySpark Jobs & Trino Analytical Modeling.

#### Key Contributions & Accomplishments:
* **FHIR & DICOM Ingestion**: Developed FHIR R4 resource validators and parsers (`src/fhir/`, `src/ingestion/`) handling Patient, Observation, Encounter, Condition, and MedicationRequest resources.
* **PHI De-identification Engine**: Implemented field-level de-identification, date shifting, and salt-based hashing (`src/deidentification/`) strictly preserving HIPAA/GDPR privacy compliance.
* **Airflow Orchestration DAG**: Authored the 7-task `dags/medical_data_pipeline.py` workflow with automated retries, execution timeouts, and invalid-ratio quality gates.
* **PySpark Lakehouse Transformation**: Developed scalable PySpark jobs (`spark/jobs/bronze_to_silver.py`, `silver_to_gold.py`) populating Apache Iceberg tables registered under Project Nessie.
* **SQL Analytics & Data Quality**: Authored Trino analytical queries (`sql/analytics/`) and multi-layer data quality check suites (`src/quality/`).

---

## 🤝 Handoff & Collaboration Model

For detailed documentation on how the **DevOps Engineer** and **Data Engineer** collaborate via automated GitSync and CI/CD quality gates, see **[docs/DATAOPS_COLLABORATION.md](docs/DATAOPS_COLLABORATION.md)** and the full platform documentation PDF at **[docs/Medical-Lakehouse-Platform-Documentation.pdf](docs/Medical-Lakehouse-Platform-Documentation.pdf)**.
