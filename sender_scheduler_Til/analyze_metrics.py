"""
TiL Sender Metrics 분석 스크립트
파일: til_sender_metrics.csv
담당: 메트릭 1~6 (counts/rates), 9, 10
메트릭 7, 8, 11은 analyze_receiver_metrics.py 참조
"""

import pandas as pd
import numpy as np

CSV_PATH = "til_sender_metrics.csv"

df = pd.read_csv(CSV_PATH)
df_valid = df[df["adv_return_ms"] > 0].copy()

# 기본 카운트
N_adv_recv     = len(df)                              # terminal decisions
N_parse_ok     = len(df_valid)                        # 파싱 성공
N_steered      = len(df[df["steer_result"] == "STEERED"])
N_pass         = len(df[df["steer_result"] == "PASS"])

print("=" * 60)
print("  TiL Sender Metrics 분석 결과")
print("=" * 60)
print(f"  파일: {CSV_PATH}")
print("=" * 60)

# ── 카운트 / 비율 메트릭 ─────────────────────────────────────
print("\n▶ Count / Rate Metrics")
print(f"  {'Metric':<45} {'Value':>10}")
print(f"  {'-'*55}")

# [1] Initiated TiL cycles → receiver 측에서 측정 (g_adv_sent_count)
print(f"  {'[1] Initiated TiL cycles':<45} {'→ receiver CSV 참조':>10}")

# [2] Returned advisories → receiver 측 g_adv_sent_count
print(f"  {'[2] Returned advisories':<45} {'→ receiver CSV 참조':>10}")

# [3] Advisory parse success rate
print(f"  {'[3] Advisory parse success rate':<45} {N_parse_ok/N_adv_recv*100:>9.1f}%")
print(f"       N_parse_ok={N_parse_ok} / N_adv_recv={N_adv_recv}")

# [4] Sensing-to-steering cycle completion rate
# = N_terminal_decision(=N_adv_recv) / N_cycle_initiated(=receiver의 adv_sent)
# sender만으로는 분모를 알 수 없으므로 receiver CSV와 함께 계산 필요
print(f"  {'[4] Sensing-to-steering completion rate':<45} {'→ receiver CSV와 결합 필요':>10}")

# [5] Steering activation rate
print(f"  {'[5] Steering activation rate':<45} {N_steered/N_adv_recv*100:>9.1f}%")
print(f"       N_steered={N_steered} / N_terminal={N_adv_recv}")

# [6] Pass-through decision rate
print(f"  {'[6] Pass-through decision rate':<45} {N_pass/N_adv_recv*100:>9.1f}%")
print(f"       N_pass={N_pass} / N_terminal={N_adv_recv}")

# ── 레이턴시 메트릭 ──────────────────────────────────────────
print("\n▶ Latency Metrics")
print(f"  {'Metric':<45} {'mean':>8} {'p95':>8}")
print(f"  {'-'*61}")

# [9] Advisory return latency
print(f"  {'[9]  Advisory return latency (ms)':<45} "
      f"{df_valid['adv_return_ms'].mean():>8.3f} "
      f"{df_valid['adv_return_ms'].quantile(0.95):>8.3f}")
print(f"       min={df_valid['adv_return_ms'].min():.3f}  "
      f"max={df_valid['adv_return_ms'].max():.3f}  "
      f"std={df_valid['adv_return_ms'].std():.3f}")

# [10] Advisory application latency
if "apply_latency_ms" in df.columns:
    print(f"  {'[10] Advisory application latency (ms)':<45} "
          f"{df['apply_latency_ms'].mean():>8.4f} "
          f"{df['apply_latency_ms'].quantile(0.95):>8.4f}")
    print(f"       min={df['apply_latency_ms'].min():.4f}  "
          f"max={df['apply_latency_ms'].max():.4f}  "
          f"std={df['apply_latency_ms'].std():.4f}")

print(f"\n  [7][8][11] → analyze_receiver_metrics.py 참조")

print("\n" + "=" * 60)
print("  요약 테이블")
print("=" * 60)
print(f"  {'Metric':<42} {'mean':>8} {'p95':>8}")
print(f"  {'-'*58}")
print(f"  {'[3]  Parse success rate':<42} {N_parse_ok/N_adv_recv*100:>7.1f}%")
print(f"  {'[5]  Steering activation rate':<42} {N_steered/N_adv_recv*100:>7.1f}%")
print(f"  {'[6]  Pass-through decision rate':<42} {N_pass/N_adv_recv*100:>7.1f}%")
print(f"  {'[9]  Advisory return latency (ms)':<42} {df_valid['adv_return_ms'].mean():>8.3f} {df_valid['adv_return_ms'].quantile(0.95):>8.3f}")
if "apply_latency_ms" in df.columns:
    print(f"  {'[10] Advisory application latency (ms)':<42} {df['apply_latency_ms'].mean():>8.4f} {df['apply_latency_ms'].quantile(0.95):>8.4f}")
print("=" * 60)