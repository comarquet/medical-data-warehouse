"""
ETL — Medical Data Warehouse

Builds a star schema from French open datasets (physicians, crime,
sunshine, real estate, infrastructure, tax income, demographics,
unemployment, cities) to analyse territorial attractiveness for
physician settlement.

Usage:
    python etl.py
    Output CSV files are written to output/.
"""

import numpy as np
import pandas as pd
import unidecode


# ---------------------------------------------------------------------------
# Load sources
# ---------------------------------------------------------------------------

def load_sources(folder: str = "resources") -> dict[str, pd.DataFrame]:
    """Load all source files and return a name -> DataFrame dict."""
    return {
        "codes_dom": pd.read_csv(f"{folder}/Codes_DOM.csv", delimiter=","),
        "criminalite": pd.read_csv(f"{folder}/Criminalité.csv", delimiter=";"),
        "ensoleillement": pd.read_csv(f"{folder}/Ensoleillement.csv", delimiter=","),
        "immobilier": pd.read_csv(f"{folder}/Immobilier.csv", delimiter=","),
        "infrastructures": pd.read_csv(
            f"{folder}/Infrastructures.csv", delimiter=";", low_memory=False
        ),
        "medecins": pd.read_excel(
            f"{folder}/Répartition_médecin_département.xlsx",
            sheet_name="DEP",
            header=4,
        ),
        "revenus": pd.read_excel(f"{folder}/Revenus_fiscaux.xlsx", header=5),
        "population": pd.read_excel(
            f"{folder}/Solde_migratoire.xlsx",
            sheet_name="Territoire - Figure 1",
            header=3,
        ),
        "chomage": pd.read_csv(f"{folder}/Chomage.csv", header=3),
        "villes": pd.read_csv(f"{folder}/Villes.csv", low_memory=False),
    }


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def normalize_department_name(name: str) -> str:
    """Normalize a department name for cross-source joining."""
    name = unidecode.unidecode(str(name)).lower().replace("-", " ").replace("'", "")
    return " ".join(name.split())


def clean_numeric_column(column: pd.Series) -> pd.Series:
    """Convert a text column (with spaces, commas, etc.) to float."""
    return (
        column.astype(str)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .replace({"n.c": np.nan, "nc": np.nan, "": np.nan})
        .astype(float)
    )


def clean_department_code(code) -> str:
    """Normalize a department code to its canonical form (2 or 3 chars)."""
    code = str(code).strip()
    if code.startswith("2A") or code.startswith("2B"):
        return code[:2]
    if code.isdigit():
        return code[:3] if code.startswith("97") else code[:2]
    return code


# ---------------------------------------------------------------------------
# Domain transformations
# ---------------------------------------------------------------------------

def transform_crime(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["taux_pour_mille"] = df["taux_pour_mille"].str.replace(",", ".").astype(float)
    annee = 2024 if 2024 in df["annee"].unique() else df["annee"].max()
    agg = (
        df[df["annee"] == annee]
        .groupby("Code_departement")["taux_pour_mille"]
        .sum()
        .reset_index()
        .rename(columns={"taux_pour_mille": "tauxCriminalite"})
    )
    return agg


def transform_real_estate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Departement"] = df["INSEE_COM"].astype(str).str[:2]
    return (
        df.groupby("Departement")["Prixm2Moyen"]
        .mean()
        .reset_index()
        .rename(columns={"Prixm2Moyen": "prixM2Moyen"})
    )


def transform_infrastructure(
    df_infra: pd.DataFrame, df_codes: pd.DataFrame
) -> pd.DataFrame:
    dep = df_infra[df_infra["GEO_OBJECT"] == "DEP"].copy()
    dep["Departement"] = dep["GEO"].astype(str).str[:2]
    pivot = (
        dep.groupby(["Departement", "FACILITY_DOM"])["OBS_VALUE"]
        .sum()
        .reset_index()
        .pivot(index="Departement", columns="FACILITY_DOM", values="OBS_VALUE")
        .reset_index()
    )
    mapping = df_codes.set_index("code")["libelle français"].to_dict()
    return pivot.rename(columns=mapping)


def transform_income(df: pd.DataFrame) -> pd.DataFrame:
    total = df[
        df["Revenu fiscal de référence par tranche (en euros)"] == "Total"
    ].copy()
    total["revenu"] = clean_numeric_column(
        total["Revenu fiscal de référence des foyers fiscaux"]
    )
    total["foyers"] = clean_numeric_column(total["Nombre de foyers fiscaux"])
    total["codeDepartement"] = total["Dép."].apply(clean_department_code)
    return (
        total.groupby("codeDepartement")
        .apply(
            lambda g: pd.Series(
                {"revenuMoyenParFoyer": g["revenu"].sum() / g["foyers"].sum()}
            )
        )
        .reset_index()
    )


def transform_cities(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("dep_code")
        .agg(
            nombreVillesSup10k=("population", lambda x: (x > 10_000).sum()),
            populationTotale=("population", "sum"),
            densiteMoyenneHabitants=("densite", "mean"),
        )
        .reset_index()
    )


# ---------------------------------------------------------------------------
# Build star schema
# ---------------------------------------------------------------------------

def build_warehouse(sources: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Assemble dimensions and fact table from the loaded sources."""

    # --- Intermediate transformations ---
    criminalite = transform_crime(sources["criminalite"])
    immobilier = transform_real_estate(sources["immobilier"])
    infrastructures = transform_infrastructure(
        sources["infrastructures"], sources["codes_dom"]
    )
    revenus = transform_income(sources["revenus"])
    villes = transform_cities(sources["villes"])

    # Normalize names for sunshine <-> physicians join
    medecins = sources["medecins"].copy()
    ensoleillement = sources["ensoleillement"].copy()
    medecins["departement_normalise"] = medecins["nomDepartement"].apply(
        normalize_department_name
    )
    ensoleillement["departement_normalise"] = ensoleillement["Départements"].apply(
        normalize_department_name
    )

    population = sources["population"].copy()
    chomage = sources["chomage"].copy()

    # --- Standardize department codes to 2 chars (zero-padded) ---
    medecins["codeDepartement"] = (
        medecins["codeDepartement"].astype(str).str.zfill(2)
    )
    immobilier["Departement"] = immobilier["Departement"].str.zfill(2)
    infrastructures["Departement"] = infrastructures["Departement"].str.zfill(2)
    population["Code département"] = (
        population["Code département"].astype(str).str.zfill(2)
    )
    chomage["Code"] = chomage["Code"].astype(str).str.zfill(2)
    criminalite["Code_departement"] = (
        criminalite["Code_departement"].astype(str).str.zfill(2)
    )
    villes["codeDepartement"] = villes["dep_code"].astype(str).str.zfill(2)

    # --- Base table (physicians) ---
    base = medecins[
        [
            "codeDepartement",
            "departement_normalise",
            "Ensemble des médecins pour 100 000 habitants",
        ]
    ].rename(
        columns={
            "departement_normalise": "nomDepartement",
            "Ensemble des médecins pour 100 000 habitants": "nombreMedecins",
        }
    )
    base["codeDepartement"] = base["codeDepartement"].apply(clean_department_code)

    # --- Progressive merges ---
    base = base.merge(
        criminalite.rename(columns={"Code_departement": "codeDepartement"}),
        on="codeDepartement",
        how="left",
    )
    base = base.merge(
        ensoleillement[["departement_normalise", "Temps enseillement (jours/an)"]].rename(
            columns={
                "Temps enseillement (jours/an)": "tauxEnsoleillement",
                "departement_normalise": "nomDepartement",
            }
        ),
        on="nomDepartement",
        how="left",
    )
    base = base.merge(
        immobilier.rename(columns={"Departement": "codeDepartement"}),
        on="codeDepartement",
        how="left",
    )
    base = base.merge(
        infrastructures.rename(columns={"Departement": "codeDepartement"}),
        on="codeDepartement",
        how="left",
    )
    base = base.merge(revenus, on="codeDepartement", how="left")
    base = base.merge(
        population[["Code département", "Variation annuelle moyenne"]].rename(
            columns={
                "Code département": "codeDepartement",
                "Variation annuelle moyenne": "evolutionPopulation",
            }
        ),
        on="codeDepartement",
        how="left",
    )
    base = base.merge(
        chomage[["Code", "T4_2024"]].rename(
            columns={"Code": "codeDepartement", "T4_2024": "tauxChomage"}
        ),
        on="codeDepartement",
        how="left",
    )
    base = base.merge(
        villes[
            [
                "codeDepartement",
                "nombreVillesSup10k",
                "populationTotale",
                "densiteMoyenneHabitants",
            ]
        ],
        on="codeDepartement",
        how="left",
    )

    # Exclude overseas territories
    base = base[~base["codeDepartement"].str.startswith("97")]
    base["codeISO"] = "FR-" + base["codeDepartement"].str.zfill(2)

    # --- Extract dimensions ---
    equipement_cols = [
        c
        for c in infrastructures.columns
        if c not in ("Departement", "codeDepartement")
    ]

    dimensions = {
        "departement-dimension": base[
            ["codeDepartement", "nomDepartement", "codeISO"]
        ],
        "attractivite-dimension": base[
            ["codeDepartement", "tauxCriminalite", "tauxEnsoleillement", "prixM2Moyen"]
        ],
        "population-dimension": base[
            [
                "codeDepartement",
                "evolutionPopulation",
                "densiteMoyenneHabitants",
                "populationTotale",
                "nombreVillesSup10k",
            ]
        ],
        "economie-dimension": base[
            ["codeDepartement", "revenuMoyenParFoyer", "tauxChomage"]
        ],
        "infrastructures-dimension": base[
            ["codeDepartement"] + equipement_cols
        ],
    }

    facts = {
        "installation-medecins-fait": base[["nombreMedecins", "codeDepartement"]],
    }

    return {**dimensions, **facts, "result": base}


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export(tables: dict[str, pd.DataFrame], folder: str = "output") -> None:
    """Write each table to a CSV file."""
    for name, df in tables.items():
        path = f"{folder}/{name}.csv"
        df.to_csv(path, index=False)
        print(f"  ✓ {path}  ({len(df)} rows)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("Loading sources…")
    sources = load_sources()
    print("Building warehouse…")
    tables = build_warehouse(sources)
    print("Exporting CSV files…")
    export(tables)
    print("Done.")


if __name__ == "__main__":
    main()
