"""
TiL Receiver Metrics 분석 스크립트
파일: til_all_metrics.csv
담당: 메트릭 1, 2, 7, 8, 11
메트릭 3~6, 9, 10은 analyze_sender_metrics.py 참조
"""

import pandas as pd
import numpy as np

CSV_PATH     = "til_all_metrics.csv"
SENDER_CSV   = "til_sender_metrics.csv"   # [4] 계산 시 사용

df = pd.read_csv(CSV_PATH)
df_e2e    = df[df["total_e2e_ms"] > 0].copy()
df_ret    = df[df["adv_return_ms"] > 0].copy()
total     = len(df)

# server-side processing = twin_update + adv_gen
df["server_proc_ms"] = df["twin_update_ms"] + df["adv_gen_ms"]

print("=" * 60)
print("  TiL Receiver Metrics 분석 결과")
print("=" * 60)
print(f"  파일: {CSV_PATH}")
print(f"  전체 epoch 수: {total}")
print("=" * 60)

# ── 카운트 메트릭 ─────────────────────────────────────────────
print("\n▶ Count Metrics")

# [1] Initiated TiL cycles = receiver가 til_server에 보낸 epoch 수
N_initiated = total
print(f"  [1] Initiated TiL cycles        : {N_initiated}")

# [2] Returned advisories = advisory 전송 성공 수 (adv_return_ms > 0인 행)
N_returned = len(df_ret)
print(f"  [2] Returned advisories         : {N_returned}")

# [4] Sensing-to-steering completion rate
# = N_terminal_decision / N_cycle_initiated
# N_terminal_decision = sender CSV의 adv_recv_count 최종값
try:
    df_s = pd.read_csv(SENDER_CSV)
    N_terminal = int(df_s["adv_recv_count"].iloc[-1])
    completion_rate = N_terminal / N_initiated * 100
    print(f"  [4] Sensing-to-steering completion rate: "
          f"{N_terminal}/{N_initiated} = {completion_rate:.1f}%")
except:
    print(f"  [4] Sensing-to-steering completion rate: "
          f"sender CSV 없음 → analyze_sender_metrics.py 참조")

# ── 레이턴시 메트릭 ──────────────────────────────────────────
print("\n▶ Latency Metrics")
print(f"  {'Metric':<45} {'mean':>8} {'p95':>8}")
print(f"  {'-'*61}")

# [7] Uplink delivery latency: T_rx_server - T_tx_vehicle
uu_mean_vals = df["uu_uplink_mean_ms"]
uu_p95_vals  = df["uu_uplink_p95_ms"]
pc5_valid    = df[df["pc5_uplink_mean_ms"] > 0]

print(f"  {'[7]  Uplink latency - Uu (ms)':<45} "
      f"{uu_mean_vals.mean():>8.3f} "
      f"{uu_p95_vals.mean():>8.3f}")

if len(pc5_valid) > 0:
    pc5_mean_vals = pc5_valid["pc5_uplink_mean_ms"]
    pc5_p95_vals  = pc5_valid["pc5_uplink_p95_ms"]
    print(f"  {'[7]  Uplink latency - PC5 (ms)':<45} "
          f"{pc5_mean_vals.mean():>8.3f} "
          f"{pc5_p95_vals.mean():>8.3f}")
else:
    print(f"  {'[7]  Uplink latency - PC5 (ms)':<45} {'N/A':>8}")

# [8] Server-side processing latency: T_adv_send - T_rx_server
#     = twin_update_ms + adv_gen_ms
print(f"  {'[8]  Server-side processing (ms)':<45} "
      f"{df['server_proc_ms'].mean():>8.4f} "
      f"{df['server_proc_ms'].quantile(0.95):>8.4f}")
print(f"       twin_update: mean={df['twin_update_ms'].mean():.4f}  "
      f"adv_gen: mean={df['adv_gen_ms'].mean():.4f}")

# epoch_wait가 있으면 출력
if "epoch_wait_ms" in df.columns:
    print(f"\n  [*]  Epoch aggregation wait (ms)  "
          f"mean={df['epoch_wait_ms'].mean():.3f}  "
          f"p95={df['epoch_wait_ms'].quantile(0.95):.3f}")

# [11] Total sensing-to-steering latency: T_apply_end - T_sens_vehicle
print(f"  {'[11] Total sensing-to-steering (ms)':<45} "
      f"{df_e2e['total_e2e_ms'].mean():>8.1f} "
      f"{df_e2e['total_e2e_ms'].quantile(0.95):>8.1f}")
print(f"       min={df_e2e['total_e2e_ms'].min():.1f}  "
      f"max={df_e2e['total_e2e_ms'].max():.1f}  "
      f"std={df_e2e['total_e2e_ms'].std():.1f}")

print(f"\n  [3][5][6][9][10] → analyze_sender_metrics.py 참조")

# ── E2E 분해 검증 ─────────────────────────────────────────────
print("\n▶ E2E 분해 검증")
uplink   = uu_mean_vals.mean()
srv_proc = df["server_proc_ms"].mean()
wait     = df["epoch_wait_ms"].mean() if "epoch_wait_ms" in df.columns else 0.0
adv_ret  = df_ret["adv_return_ms"].mean()
e2e      = df_e2e["total_e2e_ms"].mean()

print(f"  [7]uplink({uplink:.1f}) + [8]server({srv_proc:.2f}) + "
      f"[*]wait({wait:.1f}) + [9]return({adv_ret:.1f})")
print(f"  = {uplink + srv_proc + wait + adv_ret:.1f} ms  "
      f"(vs [11] actual mean {e2e:.1f} ms)")

# ── 최종 요약 테이블 ──────────────────────────────────────────
print("\n" + "=" * 60)
print("  최종 요약 테이블")
print("=" * 60)
print(f"  {'Metric':<42} {'mean':>8} {'p95':>8}")
print(f"  {'-'*58}")
print(f"  {'[1]  Initiated TiL cycles':<42} {N_initiated:>8}")
print(f"  {'[2]  Returned advisories':<42} {N_returned:>8}")
print(f"  {'[7]  Uplink latency - Uu (ms)':<42} {uu_mean_vals.mean():>8.3f} {uu_p95_vals.mean():>8.3f}")
if len(pc5_valid) > 0:
    print(f"  {'[7]  Uplink latency - PC5 (ms)':<42} {pc5_mean_vals.mean():>8.3f} {pc5_p95_vals.mean():>8.3f}")
print(f"  {'[8]  Server-side processing (ms)':<42} {df['server_proc_ms'].mean():>8.4f} {df['server_proc_ms'].quantile(0.95):>8.4f}")
if "epoch_wait_ms" in df.columns:
    print(f"  {'[*]  Epoch wait (ms)':<42} {df['epoch_wait_ms'].mean():>8.3f} {df['epoch_wait_ms'].quantile(0.95):>8.3f}")
print(f"  {'[11] Total E2E latency (ms)':<42} {e2e:>8.1f} {df_e2e['total_e2e_ms'].quantile(0.95):>8.1f}")
print(f"  {'[3][5][6][9][10]':<42} {'→ sender script':>16}")
print("=" * 60)