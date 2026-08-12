# 🚀 End-to-End Customer Churn MLOps Pipeline

[![MLOps Pipeline CI](https://github.com/YOUR_USERNAME/gcc-churn-mlops/actions/workflows/ci.yml/badge.svg)](https://github.com/asuratheuser/gcc-churn-mlops/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade machine learning engineering pipeline that automates data validation, feature engineering, experiment tracking, and deployment for customer churn prediction.

---

## 📌 Project Overview

This repository demonstrates production software engineering rigor for machine learning workflows. Rather than relying on static Jupyter Notebooks, this system decouples data ingestion, feature transformations, model fitting, and validation into clean Python modules packaged inside a Docker container with automated CI/CD checks.

### Key Capabilities
* **Modular Pipeline Architecture:** Clean separation between data processing, feature engineering, and training execution.
* **Schema Validation & Quality Gates:** Runtime data verification using strict type hints and `pytest` execution.
* **Experiment Tracking & Registry:** Full tracking of hyperparameters, evaluation metrics (F1-Score, Precision, Recall, ROC-AUC), and model artifacts via **MLFlow**.
* **Containerization:** Environment encapsulation via lightweight **Docker** containers.
* **Automated CI/CD:** Continuous integration workflow powered by **GitHub Actions** running automated tests and container builds on every `git push`.

---

## 🏗 System Architecture

```text
               +-------------------------------------------+
               |               Raw Data Source             |
               +---------------------+---------------------+
                                     |
                                     v
               +---------------------+---------------------+
               |       Stage 1: Data Validation &          |
               |         Modular Ingestion                 |
               |         (src/ingest/data.py)              |
               +---------------------+---------------------+
                                     |
                                     v
               +---------------------+---------------------+
               |      Stage 2: Feature Engineering &       |
               |         Scaling (src/architecture/)       |
               +---------------------+---------------------+
                                     |
                                     v
               +---------------------+---------------------+
               |      Stage 3: Training & Experiment       |
               |         Tracking (src/execution/)         |
               +----------+----------------------+----------+
                          |                      |
                          v                      v
            +-------------+-------+    +---------+-----------+
            |  MLflow Artifacts   |    |    Docker Engine    |
            | (Metrics & Models)  |    |  (Container Runtime)|
            +---------------------+    +---------+-----------+
                                                 |
                                                 v
                                       +---------+-----------+
                                       |  GitHub Actions CI  |
                                       |  (Automated Tests)  |
                                       +---------------------+