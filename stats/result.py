import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def load_stats(pattern: str = "./part-*") -> pd.DataFrame:
    files = glob.glob(pattern)
    rows = []

    for path in files:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # source;target;count;win;countSource,countTarget;prevision
                parts = line.split(";")
                if len(parts) < 6:
                    continue
                try:
                    observed = float(parts[2])      # count mesurée
                    expected = float(parts[5])      # prévision théorique
                except ValueError:
                    continue
                # colonne 0 = mesure observée, colonne 1 = estimation attendue
                rows.append((observed, expected))

    return pd.DataFrame(rows, columns=["mesure", "estimation"])


def main() -> None:
    df = load_stats()

    if df.empty:
        print("Aucune donnée chargée depuis ./part-*. Vérifie le répertoire de sortie des stats.")
        return

    # x = mesure observée, y = estimation théorique
    x = df["mesure"].values
    y = df["estimation"].values

    # Régression linéaire y = a * x + b
    a, b = np.polyfit(x, y, 1)
    y_pred = a * x + b

    # Coefficient de détermination R^2
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    plt.figure(figsize=(8, 8))
    plt.scatter(x, y, s=5, alpha=0.3, label="Matchups (mesure vs estimation)")
    # Seule droite demandée : la régression linéaire
    plt.plot(x, y_pred, color="red", label=f"Régression linéaire : y = {a:.3f}x + {b:.3f}, R² = {r2:.3f}")

    plt.xlabel("mesures")
    plt.ylabel("estimations")
    plt.title("Nuage de points et régression linéaire")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()