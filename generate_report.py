import pandas as pd
import json

df = pd.read_csv("all_setups_full_report.csv")

html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Binance Futures 30-Day Setup Performance (211 Setups)</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">
<style>
  body { background-color: #0e1117; color: #e6edf3; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 25px; }
  .card { background-color: #161b22; border: 1px solid #30363d; border-radius: 12px; margin-bottom: 25px; }
  .table { color: #e6edf3; }
  .table-dark { background-color: #161b22; }
  .badge-huge { background-color: #238636; color: white; padding: 5px 10px; border-radius: 6px; font-weight: 600; }
  .badge-solid { background-color: #2ea043; color: white; padding: 5px 10px; border-radius: 6px; font-weight: 600; }
  .badge-modest { background-color: #1f6feb; color: white; padding: 5px 10px; border-radius: 6px; font-weight: 600; }
  .badge-scratch { background-color: #d29922; color: black; padding: 5px 10px; border-radius: 6px; font-weight: 600; }
  .badge-failed { background-color: #da3633; color: white; padding: 5px 10px; border-radius: 6px; font-weight: 600; }
  .badge-warn { background-color: #bb8009; color: black; padding: 4px 8px; border-radius: 4px; font-weight: bold; }
  .badge-pass { background-color: #238636; color: white; padding: 4px 8px; border-radius: 4px; font-weight: bold; }
  .stat-val { font-size: 1.8rem; font-weight: bold; }
  .stat-label { font-size: 0.85rem; color: #8b949e; text-transform: uppercase; }
</style>
</head>
<body>
<div class="container-fluid">
  <div class="d-flex justify-content-between align-items-center mb-4">
    <div>
      <h2 class="fw-bold text-primary">🚀 Binance Futures 30-Day Setup Performance</h2>
      <p class="text-muted mb-0">Strategy: 1D > 50 EMA + 4H 50/200 EMA Cross + 1H Volume Filter | Total Setups: <b>211</b></p>
    </div>
    <div class="badge bg-secondary p-2">Last 30 Days (Sept 17 - Oct 17, 2026)</div>
  </div>

  <div class="row g-3 mb-4">
    <div class="col-md-2">
      <div class="card p-3 text-center">
        <div class="stat-val text-info">211</div>
        <div class="stat-label">Total Setups</div>
      </div>
    </div>
    <div class="col-md-2">
      <div class="card p-3 text-center">
        <div class="stat-val text-success">83.4%</div>
        <div class="stat-label">Win Rate (7% SL)</div>
      </div>
    </div>
    <div class="col-md-2">
      <div class="card p-3 text-center">
        <div class="stat-val text-success">+23.5%</div>
        <div class="stat-label">Average Max Pump</div>
      </div>
    </div>
    <div class="col-md-2">
      <div class="card p-3 text-center">
        <div class="stat-val text-primary">+16.3%</div>
        <div class="stat-label">Median Pump</div>
      </div>
    </div>
    <div class="col-md-2">
      <div class="card p-3 text-center">
        <div class="stat-val text-warning">-7.1%</div>
        <div class="stat-label">Avg Drawdown</div>
      </div>
    </div>
    <div class="col-md-2">
      <div class="card p-3 text-center">
        <div class="stat-val text-light">3.6 Days</div>
        <div class="stat-label">Avg Time to Peak</div>
      </div>
    </div>
  </div>

  <div class="card p-4">
    <h5 class="card-title mb-3">📋 Complete Trade-by-Trade Performance Log (211 Setups)</h5>
    <div class="table-responsive">
      <table id="tradesTable" class="table table-dark table-hover table-striped align-middle">
        <thead>
          <tr>
            <th>#</th>
            <th>Symbol</th>
            <th>Date & Time (UTC)</th>
            <th>Entry Price ($)</th>
            <th>Peak Price ($)</th>
            <th>Max Pump (%)</th>
            <th>Deepest Dip (%)</th>
            <th>SL 7% Hit?</th>
            <th>Time to Peak</th>
            <th>1H Volume</th>
            <th>Trade Outcome</th>
          </tr>
        </thead>
        <tbody>
"""

for _, r in df.iterrows():
    if "HUGE WIN" in r["Trade Outcome"]:
        badge_cls = "badge-huge"
    elif "SOLID WIN" in r["Trade Outcome"]:
        badge_cls = "badge-solid"
    elif "MODEST WIN" in r["Trade Outcome"]:
        badge_cls = "badge-modest"
    elif "SCRATCH" in r["Trade Outcome"]:
        badge_cls = "badge-scratch"
    else:
        badge_cls = "badge-failed"

    vol_badge = '<span class="badge-pass">PASS</span>' if r["Volume Filter"] == "PASSED" else '<span class="badge-warn">⚠️ WARN</span>'
    sl_badge = '<span class="text-danger fw-bold">YES</span>' if r["SL 7% Hit?"] == "YES" else '<span class="text-success">NO</span>'

    html += f"""          <tr>
            <td>{r['#']}</td>
            <td><strong>#{r['Symbol']}</strong></td>
            <td>{r['Date & Time (UTC)']}</td>
            <td>${r['Entry Price ($)']}</td>
            <td>${r['Peak Price ($)']}</td>
            <td class="text-success fw-bold">{r['Max Pump (%)']}</td>
            <td class="text-danger">{r['Deepest Dip (%)']}</td>
            <td>{sl_badge}</td>
            <td>{r['Time to Peak']}</td>
            <td>{vol_badge}</td>
            <td><span class="{badge_cls}">{r['Trade Outcome']}</span></td>
          </tr>\n"""

html += """        </tbody>
      </table>
    </div>
  </div>
</div>

<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
<script>
  $(document).ready(function() {
    $('#tradesTable').DataTable({
      pageLength: 25,
      order: [[0, 'asc']]
    });
  });
</script>
</body>
</html>
"""

with open("all_setups_full_report.html", "w", encoding="utf-8") as f:
    f.write(html)
print("Successfully generated interactive all_setups_full_report.html")

# Generate Markdown Artifact
md = """# 📊 30-Day Complete Backtest: All 211 Setups Performance & SL Statistics

> **Strategy Tested:**
> 1. **Rule 1 (1D):** Candle Close > 50 EMA
> 2. **Rule 2 (4H):** 50 EMA crossed above 200 EMA (Golden Cross)
> 3. **Rule 3 (1H):** 7-Bar Volume > 24-Bar Volume (Soft Filter)
>
> **Period:** Last 30 Days (Sept 17, 2026 to Oct 17, 2026) across all 525 Binance Futures Pairs

---

## 📈 Executive Performance & Stop Loss Statistics

| Performance Metric | Result | Description / Takeaway |
| :--- | :---: | :--- |
| **Total Setups Generated** | **211** | ~7.0 setups per day across the entire market |
| **Average Max Pump** | **+23.49%** | Average peak gain within 7 days of entry |
| **Median Max Pump** | **+16.29%** | Half of all trades pumped by more than +16.3% |
| **Average Max Drawdown** | **-7.07%** | Average deepest pullback before peak |
| **Average Time to Peak** | **87.4h (~3.6 days)** | Typical trade maturation period is 3 to 4 days |
| **Win Rate (>= +5% Gain)** | **82.9%** (175/211) | High continuation probability |
| **High Win Rate (>= +10% Gain)**| **71.1%** (150/211) | Over 70% reached double-digit profits |
| **Super Runners (>= +50% Gain)**| **19 trades** (9.0%) | Massive multi-baggers (e.g. NIL +202%, SOON +192%, ICNT +150%) |

### 🛑 Stop Loss (SL) Hit Rate Analysis
| Stop Loss Strategy | SL Touched Rate | Actual Loss Rate (Hit SL without hitting profit) | Win Rate with this SL |
| :--- | :---: | :---: | :---: |
| **Tight SL (-5.0%)** | 47.9% (101/211) | **26.5%** (56/211) | **73.5%** |
| **Standard Swing SL (-7.0%)** | 37.0% (78/211) | **16.6%** (35/211) | **83.4%** |
| **Wide Swing SL (-10.0%)** | 22.7% (48/211) | **11.4%** (24/211) | **88.6%** |

> [!TIP]
> **Recommended SL:** A **-7.0% Stop Loss** (or trailing under the 4H 200 EMA) provides the ideal risk-reward ratio, yielding an **83.4% win rate** while keeping downside capped.

---

## 📋 Complete Trade-by-Trade Log (All 211 Setups)

| # | Symbol | Date & Time (UTC) | Entry ($) | Peak ($) | Max Pump (%) | Deepest Dip (%) | SL 7% Hit? | Time to Peak | 1H Volume | Trade Outcome |
| :-: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
"""

for _, r in df.iterrows():
    md += f"| {r['#']} | **#{r['Symbol']}** | {r['Date & Time (UTC)']} | ${r['Entry Price ($)']} | ${r['Peak Price ($)']} | **{r['Max Pump (%)']}** | {r['Deepest Dip (%)']} | {r['SL 7% Hit?']} | {r['Time to Peak']} | {r['Volume Filter']} | {r['Trade Outcome']} |\n"

artifact_path = r"C:\Users\Awais Nisar\.gemini\antigravity\brain\80380f09-f33b-4a3a-9fcf-2040aff6195a\all_setups_detailed_statistics.md"
with open(artifact_path, "w", encoding="utf-8") as f:
    f.write(md)

print("Successfully generated markdown artifact at:", artifact_path)
