from crypto_lab.lab_core.pipeline.pipeline_v0 import load_and_qc
from crypto_lab.lab_core.qc.dashboard_v0 import pipeline_qc_dashboard

# 1. Load data + QC
result = load_and_qc(
    exchange_name="binance",
    symbol="BTC/USDT",
    timeframe="1m",
    limit=200,
    save=False
)

df = result["df"]
qc = result["qc"]

# 2. Generate dashboard
path = pipeline_qc_dashboard(
    df=df,
    qc=qc,
    symbol="BTC/USDT",
    timeframe="1m",
    output_dir="data/qc_reports"
)

print("Dashboard saved:", path)
