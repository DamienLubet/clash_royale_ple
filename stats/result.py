import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from scipy import stats


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
                if len(parts) < 7:
                    continue
                try:
                    observed = float(parts[2])      # count mesurée
                    expected = float(parts[6])      # prévision
                except ValueError:
                    continue
                rows.append((observed, expected))

    return pd.DataFrame(rows, columns=["mesure", "prevision"])


def main() -> None:
    df = load_stats()

    if df.empty:
        print("Aucune donnée chargée depuis ./part-*. Vérifie le répertoire de sortie des stats.")
        return

    df = df[df["prevision"] > 0].copy()

    # x = mesure observée, y = prevision
    x = df["mesure"].values
    y = df["prevision"].values

    ratio = x / y

    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    
    y_pred = slope * x + intercept
    r_squared = r_value**2

    plt.figure(figsize=(8, 8))
    sc = plt.scatter(x, y, c=ratio, s=5, alpha=0.5, 
                     norm=LogNorm(), 
                     cmap='viridis')

    cbar = plt.colorbar(sc)
    cbar.solids.set(alpha=1)
    cbar.set_label('Ratio (mesure / prevision)')

    label_stats = (f"y = {slope:.4f}x + {intercept:.4f}\n"
                   f"$R^2$ = {r_squared:.4f}\n"
                   f"p-value = {p_value:.4e}")

    plt.plot(x, y_pred, color="red", label=label_stats)

    plt.xlabel("mesures")
    plt.ylabel("previsions")
    plt.title("Scatter et régression linéaire")
    plt.legend(loc="upper left")
    plt.grid(True)
    plt.tight_layout()
    output_filename = "resultat_graphique.png"
    plt.savefig(output_filename)
    plt.show()


if __name__ == "__main__":
    main()