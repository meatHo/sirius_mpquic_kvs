"""
TiL 통합 Metrics 분석 스크립트
파일: til_all_metrics.csv (receiver), til_sender_metrics.csv (sender), til_freeze.csv (receiver)
실행: python3 analyze_receiver_metrics.py
      python3 analyze_receiver_metrics.py --receiver til_all_metrics.csv --sender til_sender_metrics.csv
"""

import pandas as pd
import numpy as np
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--receiver", type=str, default="til_all_metrics.csv")
parser.add_argument("--sender",   type=str, default="til_sender_metrics.csv")
args = parser.parse_args()

# ── CSV 로드 ──────────────────────────────────────────────
df_raw = pd.read_csv(args.receiver)

sender_available = os.path.exists(args.sender)
if sender_available:
    ds_raw = pd.read_csv(args.sender)
else:
    ds_raw = None
    print(f"  [경고] sender CSV 없음: {args.sender}")

# freeze CSV 경로 추론
freeze_path = os.path.join(os.path.dirname(os.path.abspath(args.receiver)), "til_freeze.csv")
if not os.path.exists(freeze_path):
    freeze_path = "til_freeze.csv"

freeze_available = False
df_freeze = None
if os.path.exists(freeze_path):
    try:
        df_freeze = pd.read_csv(freeze_path)
        if len(df_freeze) == 0:
            print(f"  [정보] freeze 없음 (0건) ✅")
        else:
            freeze_available = True
    except Exception:
        print(f"  [경고] freeze CSV 읽기 실패: {freeze_path}")
else:
    print(f"  [경고] freeze CSV 없음: {freeze_path}")

# ── receiver 전처리 ───────────────────────────────────────
N_initiated = len(df_raw)
N_returned  = (df_raw["adv_return_ms"] > 0).sum()

df_has_uplink = df_raw[
    (df_raw["uu_uplink_mean_ms"] > 0) | (df_raw["pc5_uplink_mean_ms"] > 0)
].copy()
df_e2e_raw = df_raw[df_raw["total_e2e_ms"] > 0].copy()

df = df_has_uplink.reset_index(drop=True)
n_removed = N_initiated - len(df)

df["server_proc_ms"] = df["twin_update_ms"] + df["adv_gen_ms"]
df_ret = df[df["adv_return_ms"] > 0].copy()

e2e_p99  = df_e2e_raw["total_e2e_ms"].quantile(0.99) if len(df_e2e_raw) > 0 else 9999
wait_p99 = df["epoch_wait_ms"].quantile(0.99) if "epoch_wait_ms" in df.columns and len(df) > 0 else 9999

df_e2e_clean = df_e2e_raw[df_e2e_raw["total_e2e_ms"] <= e2e_p99].copy()
df_ret_clean = df_ret[df_ret["adv_return_ms"] <= df_ret["adv_return_ms"].quantile(0.99)].copy() if len(df_ret) > 0 else df_ret

# ── sender 전처리 ─────────────────────────────────────────
if sender_available:
    ds = ds_raw.copy()
    ds_valid = ds[ds["adv_return_ms"] > 0].copy()
    adv_ret_p99 = ds_valid["adv_return_ms"].quantile(0.99) if len(ds_valid) > 0 else 9999
    ds_clean = ds_valid[ds_valid["adv_return_ms"] <= adv_ret_p99].copy()

    N_terminal   = len(ds)
    N_steered    = (ds["steer_result"] == "STEERED").sum()
    N_pass       = (ds["steer_result"] == "PASS").sum()
    N_parse_ok   = len(ds_valid)
    adv_ret_mean = ds_clean["adv_return_ms"].mean() if len(ds_clean) > 0 else 0
    adv_ret_p95  = ds_clean["adv_return_ms"].quantile(0.95) if len(ds_clean) > 0 else 0
    apply_mean   = ds["apply_latency_ms"].mean()
    apply_p95    = ds["apply_latency_ms"].quantile(0.95)

# ── 출력 시작 ─────────────────────────────────────────────
print("=" * 62)
print("  TiL 통합 Metrics 분석 결과")
print("=" * 62)
print(f"  receiver: {args.receiver}  (전체 {N_initiated}행)")
print(f"  uplink 유효 행: {len(df)}행  (uplink=0인 {n_removed}행 latency 분석 제외)")
if sender_available:
    print(f"  sender  : {args.sender}  ({N_terminal}행)")
if freeze_available:
    print(f"  freeze  : {freeze_path}  ({len(df_freeze)}건)")
elif df_freeze is not None:
    print(f"  freeze  : {freeze_path}  (0건 ✅)")
print(f"  이상치 기준: p99")
print("=" * 62)

# ── Count Metrics ─────────────────────────────────────────
print("\n▶ Count / Rate Metrics")
print(f"  [1] Initiated TiL cycles             : {N_initiated}")
print(f"  [2] Returned advisories              : {N_returned}  ({N_returned/N_initiated*100:.1f}%)")

if sender_available:
    print(f"  [3] Advisory parse success rate      : {N_parse_ok/N_terminal*100:.1f}%  ({N_parse_ok}/{N_terminal})")
    rate4 = N_terminal / N_initiated * 100
    print(f"  [4] Sensing-to-steering completion   : {N_terminal}/{N_initiated} = {rate4:.1f}%")
    print(f"  [5] Steering activation rate         : {N_steered/N_terminal*100:.1f}%  ({N_steered}/{N_terminal})")
    print(f"  [6] Pass-through decision rate       : {N_pass/N_terminal*100:.1f}%  ({N_pass}/{N_terminal})")
else:
    print(f"  [3][4][5][6] → sender CSV 없음")

# ── Latency Metrics ───────────────────────────────────────
print("\n▶ Latency Metrics")
print(f"  {'Metric':<42} {'mean':>8} {'p95':>8}")
print(f"  {'-'*60}")

df_uu  = df[df["uu_uplink_mean_ms"] > 0]
df_pc5 = df[df["pc5_uplink_mean_ms"] > 0]

if len(df_uu) > 0:
    print(f"  {'[7]  Uplink - Uu (ms)':<42} {df_uu['uu_uplink_mean_ms'].mean():>8.3f} {df_uu['uu_uplink_p95_ms'].mean():>8.3f}")
else:
    print(f"  {'[7]  Uplink - Uu (ms)':<42} {'N/A':>8}")

if len(df_pc5) > 0:
    print(f"  {'[7]  Uplink - PC5 (ms)':<42} {df_pc5['pc5_uplink_mean_ms'].mean():>8.3f} {df_pc5['pc5_uplink_p95_ms'].mean():>8.3f}")
else:
    print(f"  {'[7]  Uplink - PC5 (ms)':<42} {'N/A':>8}")

if len(df) > 0:
    sp = df["server_proc_ms"]
    print(f"  {'[8]  Server processing (ms)':<42} {sp.mean():>8.4f} {sp.quantile(0.95):>8.4f}")
    print(f"       twin={df['twin_update_ms'].mean():.4f}ms  adv_gen={df['adv_gen_ms'].mean():.4f}ms")
    print(f"       ※ EC2 내부 처리만 측정 (receiver↔EC2 네트워크 미포함)")

if "epoch_wait_ms" in df.columns and len(df) > 0:
    ew_clean = df[df["epoch_wait_ms"] <= wait_p99]["epoch_wait_ms"]
    print(f"  {'[*]  Epoch wait (ms)':<42} {ew_clean.mean():>8.3f} {ew_clean.quantile(0.95):>8.3f}")

if sender_available:
    print(f"  {'[9]  Advisory return (ms) [EC2→sender]':<42} {adv_ret_mean:>8.3f} {adv_ret_p95:>8.3f}")
    print(f"       ※ EC2 UTC + sender UTC 동기화 기준 편도 측정")
    print(f"  {'[10] Apply latency (ms) [sender]':<42} {apply_mean:>8.4f} {apply_p95:>8.4f}")

if len(df_e2e_clean) > 0:
    e2e = df_e2e_clean["total_e2e_ms"]
    print(f"  {'[11] Total E2E (ms)':<42} {e2e.mean():>8.1f} {e2e.quantile(0.95):>8.1f}")
    print(f"       min={e2e.min():.1f}  max={df_e2e_raw['total_e2e_ms'].max():.1f}  std={e2e.std():.1f}")
    print(f"       유효행={len(df_e2e_clean)} / 전체={N_initiated}")

# ── Freeze Metrics ────────────────────────────────────────
print("\n▶ Freeze Metrics")
print(f"  {'Metric':<42} {'value':>8}")
print(f"  {'-'*52}")
if freeze_available and df_freeze is not None and len(df_freeze) > 0:
    N_freeze        = len(df_freeze)
    freeze_total_ms = df_freeze["freeze_duration_ms"].sum()
    freeze_mean_ms  = df_freeze["freeze_duration_ms"].mean()
    freeze_p95_ms   = df_freeze["freeze_duration_ms"].quantile(0.95)
    freeze_max_ms   = df_freeze["freeze_duration_ms"].max()

    if N_initiated > 1:
        total_exp_sec = (df_raw["epoch_ts"].iloc[-1] - df_raw["epoch_ts"].iloc[0]) / 1e6
        freeze_ratio  = freeze_total_ms / (total_exp_sec * 1000) * 100 if total_exp_sec > 0 else 0
    else:
        total_exp_sec = 0
        freeze_ratio  = 0

    print(f"  {'freeze 발생 횟수':<42} {N_freeze:>8}")
    print(f"  {'freeze 총 지속시간 (ms)':<42} {freeze_total_ms:>8.1f}")
    print(f"  {'freeze 평균 지속시간 (ms)':<42} {freeze_mean_ms:>8.1f}")
    print(f"  {'freeze p95 지속시간 (ms)':<42} {freeze_p95_ms:>8.1f}")
    print(f"  {'freeze 최대 지속시간 (ms)':<42} {freeze_max_ms:>8.1f}")
    if total_exp_sec > 0:
        print(f"  {'실험 총 시간 (sec)':<42} {total_exp_sec:>8.1f}")
        print(f"  {'freeze 비율 (%)':<42} {freeze_ratio:>8.2f}")
else:
    print(f"  freeze 없음 (0건) ✅")

# ── E2E 분해 검증 ─────────────────────────────────────────
print("\n▶ E2E 분해 검증")
if len(df_uu) > 0 and len(df) > 0 and len(df_e2e_clean) > 0:
    uplink_val = df_uu["uu_uplink_mean_ms"].mean()
    srv_val    = df["server_proc_ms"].mean()
    wait_val   = ew_clean.mean() if "epoch_wait_ms" in df.columns else 0.0
    e2e_act    = df_e2e_clean["total_e2e_ms"].mean()

    if sender_available and adv_ret_mean > 0:
        ret_val = adv_ret_mean
        ret_src = "sender CSV (EC2→sender 편도)"
    else:
        ret_val = df_ret_clean["adv_return_ms"].mean() if len(df_ret_clean) > 0 else 0
        ret_src = "receiver CSV"

    total_comp = uplink_val + srv_val + wait_val + ret_val
    print(f"  [7]{uplink_val:.1f} + [8]{srv_val:.2f} + [*]{wait_val:.1f} + [9]{ret_val:.1f} = {total_comp:.1f}ms")
    print(f"  [9] 출처: {ret_src}")
    print(f"  vs [11] actual {e2e_act:.1f}ms  (오차 {abs(total_comp-e2e_act):.1f}ms)")
else:
    print(f"  E2E 분해 불가 (uplink 또는 E2E 데이터 없음)")

# ── 최종 요약 테이블 ──────────────────────────────────────
print("\n" + "=" * 62)
print("  최종 요약 테이블")
print("=" * 62)
print(f"  {'Metric':<42} {'mean':>8} {'p95':>8}")
print(f"  {'-'*60}")
print(f"  {'[1]  Initiated TiL cycles':<42} {N_initiated:>8}")
print(f"  {'[2]  Returned advisories':<42} {N_returned:>8}")
if sender_available:
    print(f"  {'[3]  Parse success rate':<42} {N_parse_ok/N_terminal*100:>7.1f}%")
    print(f"  {'[4]  S2S completion rate':<42} {rate4:>7.1f}%")
    print(f"  {'[5]  Steering activation rate':<42} {N_steered/N_terminal*100:>7.1f}%")
    print(f"  {'[6]  Pass-through rate':<42} {N_pass/N_terminal*100:>7.1f}%")
if len(df_uu) > 0:
    print(f"  {'[7]  Uplink - Uu (ms)':<42} {df_uu['uu_uplink_mean_ms'].mean():>8.3f} {df_uu['uu_uplink_p95_ms'].mean():>8.3f}")
if len(df_pc5) > 0:
    print(f"  {'[7]  Uplink - PC5 (ms)':<42} {df_pc5['pc5_uplink_mean_ms'].mean():>8.3f} {df_pc5['pc5_uplink_p95_ms'].mean():>8.3f}")
if len(df) > 0:
    print(f"  {'[8]  Server processing (ms)':<42} {sp.mean():>8.4f} {sp.quantile(0.95):>8.4f}")
    if "epoch_wait_ms" in df.columns:
        print(f"  {'[*]  Epoch wait (ms)':<42} {ew_clean.mean():>8.3f} {ew_clean.quantile(0.95):>8.3f}")
if sender_available:
    print(f"  {'[9]  Advisory return (ms)':<42} {adv_ret_mean:>8.3f} {adv_ret_p95:>8.3f}")
    print(f"  {'[10] Apply latency (ms)':<42} {apply_mean:>8.4f} {apply_p95:>8.4f}")
if len(df_e2e_clean) > 0:
    print(f"  {'[11] Total E2E (ms)':<42} {e2e.mean():>8.1f} {e2e.quantile(0.95):>8.1f}")
print(f"  {'-'*60}")
if freeze_available and df_freeze is not None and len(df_freeze) > 0:
    print(f"  {'[F]  Freeze 횟수':<42} {N_freeze:>8}")
    print(f"  {'[F]  Freeze 총 시간 (ms)':<42} {freeze_total_ms:>8.1f}")
    print(f"  {'[F]  Freeze 평균 지속시간 (ms)':<42} {freeze_mean_ms:>8.1f} {freeze_p95_ms:>8.1f}")
    if total_exp_sec > 0:
        print(f"  {'[F]  Freeze 비율 (%)':<42} {freeze_ratio:>7.2f}%")
else:
    print(f"  {'[F]  Freeze':<42} {'없음 ✅':>8}")
print("=" * 62)