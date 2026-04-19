# Medical Data Warehouse — Territorial Attractiveness for Physicians

A data warehouse project that analyses the **territorial attractiveness for physician settlement** across metropolitan France, using open data (crime rates, sunshine, real estate, infrastructure, tax income, demographics, unemployment, etc.).

Data is transformed via a Python/Pandas ETL into a **star schema** (fact + dimensions), exported as CSV files for visualisation in [Apache Superset](https://superset.apache.org/).

📄 [Full report](docs/rapport.pdf) · 📊 [Superset dashboard](docs/dashboard.pdf)

## Project Structure

```
.
├── etl.py                  # ETL transformation script
├── resources/              # Source data (CSV, XLSX)
├── output/                 # Generated results (CSV) — gitignored
├── docs/                   # Diagrams, report & dashboard
├── requirements.txt        # Python dependencies
└── README.md
```

## Star Schema

**Fact table:** `installation-medecins-fait` — number of physicians per 100,000 inhabitants by department.

**Dimensions:**

| Dimension | Content |
|-----------|---------|
| `departement` | Department code, name, ISO code |
| `attractivite` | Crime rate, sunshine, real-estate prices |
| `population` | Growth, density, total population, major cities |
| `economie` | Average household income, unemployment rate |
| `infrastructures` | Number of facilities by type (health, education, retail, etc.) |

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
python3 etl.py
```

Output CSV files are written to `output/`.

## Data Sources

- **Physician distribution** — DREES
- **Crime rates** — French Ministry of the Interior
- **Sunshine** — Departmental climate data
- **Real estate** — Average price per m² by municipality (INSEE)
- **Infrastructure** — Facilities by department (Eurostat)
- **Tax income** — DGFiP
- **Net migration / population** — INSEE
- **Unemployment** — INSEE / Pôle Emploi
- **Cities** — INSEE municipal database

## Visualisation

The output CSV files (`output/result.csv`) can be imported into Apache Superset to build interactive dashboards.
