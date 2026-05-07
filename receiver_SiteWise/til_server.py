#!/usr/bin/env python3
"""
til_server.py  —  TiL (Twin-in-the-Loop) 추론 서버
────────────────────────────────────────────────────
역할:
  1. receiver_SiteWise.cpp 로부터 5초 epoch 피처 JSON 수신 (TCP 0.0.0.0:7777)
  2. clf_opp.lgb 모델로 조향 기회 예측 (o_t)
  3. w_til 결정 후 결과를 receiver 에 TCP 응답으로 반환
"""

import socket
import json
import time
import csv, os
import math
import numpy as np
import lightgbm as lgb
from collections import deque

# ─── 설정 ──────────────────────────────────────────
LAT_CSV   = "/home/ec2-user/til_uplink_latency.csv"
TWIN_CSV  = "/home/ec2-user/til_twin_advisory.csv"
MODEL_PATH   = "clf_opp.lgb"
META_PATH    = "clf_opp_meta.json"
LISTEN_HOST  = "0.0.0.0"
LISTEN_PORT  = 7777

RSU_LAT      = 37.2830
RSU_LON      = 127.0466
DEADLINE_MS  = 200.0
# ────────────────────────────────────────────────────

# ─── 모델 & 메타 로드 ──────────────────────────────
model = lgb.Booster(model_file=MODEL_PATH)
with open(META_PATH) as f:
    meta = json.load(f)

THETA_O   = 0.30                        # meta에 없으므로 기본값
ctrl      = meta["controller"]
LAMBDA_L  = ctrl["lambda_l"]           # 0.30
LAMBDA_S  = ctrl["lambda_s"]           # 0.50
EPSILON   = ctrl["epsilon"]            # 0.30
H0        = ctrl["h0"]                 # 5.0

print(f"[TiL] 모델 로드 완료  theta_o={THETA_O}")
print(f"[TiL] 피처 수: {meta['n_features']}")
print(f"[TiL] val_AUC={meta['val_auc']:.4f}")

# ─── 차량별 Rolling 버퍼 ────────────────────────────
r_uu_buf  = deque(maxlen=3)
r_pc5_buf = deque(maxlen=3)
consecutive_freeze = 0
w_eff_curr = 0.5
twin_update_buf = []
adv_gen_buf     = []


# ─── 유틸 ─────────────────────────────────────────
def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(max(0.0, a)))


def predict_rsu_change(lat, lon, speed_mps):
    dist = haversine_m(lat, lon, RSU_LAT, RSU_LON)
    return 1.0 if (dist > 150.0 and speed_mps > 1.0) else 0.0


# ─── 피처 계산 ────────────────────────────────────
def compute_features(ep: dict):
    global consecutive_freeze, w_eff_curr

    uu_dmf  = float(ep.get("uu_deadline_met_frac", 0.0))
    pc5_dmf = float(ep.get("pc5_deadline_met_frac", 0.0))
    p_uu    = float(ep.get("p_uu",  1.0 - uu_dmf))
    p_pc5   = float(ep.get("p_pc5", 1.0 - pc5_dmf))
    uu_jit  = float(ep.get("uu_jitter_ms",  0.0))
    pc5_jit = float(ep.get("pc5_jitter_ms", 0.0))
    d_uu    = float(ep.get("d_uu_ms",  0.0))
    d_pc5   = float(ep.get("d_pc5_ms", 0.0))
    uu_frames    = int(ep.get("uu_completed_frames",  0))
    pc5_frames   = int(ep.get("pc5_completed_frames", 0))
    scheduler_id = int(ep.get("scheduler_id", 1))
    speed_mps    = float(ep.get("speed_mps", 0.0))
    heading_deg  = float(ep.get("heading_deg", 0.0))
    lat = float(ep.get("lat", RSU_LAT))
    lon = float(ep.get("lon", RSU_LON))

    # ── Rolling buffer ──────────────────────────────
    r_uu_buf.append(np.clip(1.0 - uu_dmf,  0.0, 1.0))
    r_pc5_buf.append(np.clip(1.0 - pc5_dmf, 0.0, 1.0))
    delta_r_roll3 = float(np.mean(r_pc5_buf) - np.mean(r_uu_buf))
    delta_trend   = delta_r_roll3  # 근사값

    # ── Freeze 카운트 ────────────────────────────────
    e2e_dmf = max(uu_dmf, pc5_dmf)
    if e2e_dmf < 0.70:
        consecutive_freeze += 1
    else:
        consecutive_freeze = 0

    # ── w_eff_curr ──────────────────────────────────
    tot = uu_frames + pc5_frames
    w_eff_curr = (uu_frames / tot) if tot > 0 else 0.5

    # ── 파생 피처 ───────────────────────────────────
    delta_p        = p_pc5 - p_uu
    delta_jit_norm = (pc5_jit - uu_jit) / (pc5_jit + uu_jit + 1.0)
    delta_d_norm   = (d_pc5 - d_uu)     / (d_pc5 + d_uu + 1.0)
    both_active    = 1.0 if (uu_frames > 0 and pc5_frames > 0) else 0.0
    dist_rsu       = haversine_m(lat, lon, RSU_LAT, RSU_LON)
    pred_rsu       = predict_rsu_change(lat, lon, speed_mps)
    pred_dist_rsu  = dist_rsu  # 근사값

    # ── 피처 벡터 (20개, meta feature_names 순서) ───
    # [0]  delta_p
    # [1]  delta_jit_norm
    # [2]  delta_d_norm
    # [3]  delta_dmf
    # [4]  delta_r_roll3
    # [5]  delta_trend
    # [6]  uu_deadline_met_frac
    # [7]  pc5_deadline_met_frac
    # [8]  p_uu
    # [9]  p_pc5
    # [10] d_uu
    # [11] d_pc5
    # [12] speed_mps
    # [13] heading_deg
    # [14] dist_nearest_rsu_m
    # [15] pred_rsu_change_flag
    # [16] pred_dist_nearest_rsu_m
    # [17] scheduler_id
    # [18] both_active
    # [19] consecutive_freeze_epochs
    X = np.array([[
        delta_p,
        delta_jit_norm,
        delta_d_norm,
        uu_dmf - pc5_dmf,
        delta_r_roll3,
        delta_trend,
        uu_dmf,
        pc5_dmf,
        p_uu,
        p_pc5,
        d_uu,
        d_pc5,
        speed_mps,
        heading_deg,
        dist_rsu,
        pred_rsu,
        pred_dist_rsu,
        float(scheduler_id),
        both_active,
        float(consecutive_freeze),
    ]])
    return X, both_active, uu_dmf, pc5_dmf


# ─── 추론 & 조향 결정 ─────────────────────────────
def handle_epoch(ep_json: str):
    global w_eff_curr
    try:
        ep = json.loads(ep_json)

        # ── Twin-state update time ────────────────────
        t_tw_start = time.time()
        X, both_active, uu_dmf, pc5_dmf = compute_features(ep)
        t_tw_end = time.time()
        twin_update_ms = (t_tw_end - t_tw_start) * 1000.0

        # ── Advisory generation time ──────────────────
        t_adv_start = time.time()

        # 새 모델: 방향 예측 (p_dir = predict_proba)
        p_dir = float(model.predict(X)[0])   # 0~1
        s_t   = 2 * p_dir - 1                # -1~+1 방향 신호
        o_t   = abs(s_t)                     # 조향 강도 0~1

        # 조향 결정
        if both_active and o_t >= THETA_O:
            r_uu  = 1.0 - uu_dmf
            r_pc5 = 1.0 - pc5_dmf
            delta = float(np.clip(
                LAMBDA_L * (r_pc5 - r_uu) + LAMBDA_S * s_t,
                -EPSILON, EPSILON
            ))
            w_til = float(np.clip(w_eff_curr + delta, 0.0, 1.0))
            steer = True
        else:
            w_til = w_eff_curr
            steer = False

        t_adv_end = time.time()
        adv_ms = (t_adv_end - t_adv_start) * 1000.0
        twin_update_buf.append(twin_update_ms)
        adv_gen_buf.append(adv_ms)
        t_adv_sent_us = int(time.time() * 1e6)

        payload = json.dumps({
            "w_til":          round(w_til, 4),
            "o_t":            round(o_t,   4),
            "steer":          steer,
            "epoch_ts":       ep.get("epoch_ts", 0),
            "consec_frz":     consecutive_freeze,
            "t_adv_sent_us":  t_adv_sent_us,
            "r_uu":           round(1.0 - uu_dmf, 4),
            "r_pc5":          round(1.0 - pc5_dmf, 4),
            "uu_dmf":         round(uu_dmf, 4),
            "pc5_dmf":        round(pc5_dmf, 4),
            "w_eff":          round(w_eff_curr, 4),
            "both_active":    bool(both_active),
            "h0":             H0,
            "twin_update_ms": round(twin_update_ms, 3),
            "adv_gen_ms":     round(adv_ms, 3),
        })

        # ── Uplink latency CSV ────────────────────────
        with open(LAT_CSV, "a", newline="") as f:
            csv.writer(f).writerow([
                ep.get("epoch_ts", 0),
                round(float(ep.get("uu_mean_latency_ms",  0.0)), 3),
                round(float(ep.get("uu_p95_latency_ms",   0.0)), 3),
                round(float(ep.get("pc5_mean_latency_ms", 0.0)), 3),
                round(float(ep.get("pc5_p95_latency_ms",  0.0)), 3),
            ])

        # ── Twin / Advisory CSV ───────────────────────
        tw_mean = float(np.mean(twin_update_buf))
        tw_p95  = float(np.percentile(twin_update_buf, 95)) if len(twin_update_buf) >= 2 else twin_update_ms
        ag_mean = float(np.mean(adv_gen_buf))
        ag_p95  = float(np.percentile(adv_gen_buf, 95))     if len(adv_gen_buf) >= 2 else adv_ms
        with open(TWIN_CSV, "a", newline="") as f:
            csv.writer(f).writerow([
                ep.get("epoch_ts", 0),
                round(tw_mean, 3), round(tw_p95, 3),
                round(ag_mean, 3), round(ag_p95,  3),
            ])

        print(f"[TiL] epoch={ep.get('epoch_ts',0)}  "
              f"o_t={o_t:.3f}  s_t={s_t:.3f}  w_til={w_til:.3f}  "
              f"steer={steer}  frz={consecutive_freeze}  "
              f"uu_dmf={uu_dmf:.2f}  pc5_dmf={pc5_dmf:.2f}")
        return payload

    except Exception as e:
        print(f"[TiL] 에러: {e}")
        import traceback; traceback.print_exc()
        return None


# ─── TCP 서버 루프 ────────────────────────────────
def server_loop():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((LISTEN_HOST, LISTEN_PORT))
    srv.listen(1)
    print(f"[TiL] TCP 서버 대기: {LISTEN_HOST}:{LISTEN_PORT}")

    while True:
        conn, addr = srv.accept()
        print(f"[TiL] receiver 연결: {addr}")
        buf = ""
        try:
            while True:
                chunk = conn.recv(4096).decode("utf-8", errors="ignore")
                if not chunk:
                    break
                buf += chunk
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    result = handle_epoch(line)
                    if result:
                        try:
                            conn.sendall((result + "\n").encode())
                        except OSError:
                            break
        except Exception as e:
            print(f"[TiL] 연결 종료: {e}")
        finally:
            conn.close()
            print("[TiL] receiver 연결 끊김. 재대기...")


if __name__ == "__main__":
    with open(LAT_CSV, "w", newline="") as f:
        csv.writer(f).writerow(["epoch_ts", "uu_mean_ms", "uu_p95_ms",
                                 "pc5_mean_ms", "pc5_p95_ms"])
    with open(TWIN_CSV, "w", newline="") as f:
        csv.writer(f).writerow(["epoch_ts",
                                 "twin_update_mean_ms", "twin_update_p95_ms",
                                 "adv_gen_mean_ms",     "adv_gen_p95_ms"])
    server_loop()