#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 凡凡选股 · 盘后深度计算器（fanfan_scanner.py）
================================================================================
  核心理念：数据分层——盘中只抓轻量快照，盘后才做重型K线计算。
  本脚本从缓存读取盘中快照，针对候选池逐股拉取60日K线，计算均线粘合、
  量比、起爆前夜评分、情绪周期量化等深度指标，结果写回缓存供前端读取。

  计算内容：
    1. 起爆前夜（boom）：15日涨停基因 → 全市场过滤 → 60日K线均线粘合+量比
       → 五项评分（均线粘合/量能/位置/基因/板块）+ 负面公告过滤
    2. 情绪周期（emotion）：五维量化打分（连板20+涨停20+炸板15+溢价15+跌涨比8）
       → 五阶段判定（冰点/启动/主升/高潮/退潮）→ 主线梯队真龙辨识
    3. K线批量预取：针对近15日涨停基因库，批量拉取60日K线存入缓存

  运行方式：
    python3 fanfan_scanner.py                  # 盘后全量计算（默认）
    python3 fanfan_scanner.py --type boom      # 只算起爆前夜
    python3 fanfan_scanner.py --type emotion   # 只算情绪周期
    python3 fanfan_scanner.py --type kline     # 只批量预取K线
    python3 fanfan_scanner.py --conc 8         # 自定义并发数

  依赖：Python 3.8+ 标准库 + 同目录 fanfan_cache.py + fanfan_snapshot.py（接口常量）
================================================================================
"""

import argparse
import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import fanfan_cache as cache

# ==============================================================================
# 常量与接口
# ==============================================================================
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# 60日K线（腾讯复权日K，CORS友好）
URL_KLINE = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={secid},day,,,60,qfq"
# 个股公告（负面过滤）
URL_ANN = ("https://np-anotice-stock.eastmoney.com/api/security/ann"
           "?sr=-1&page_size=10&page_index=1&ann_type=A&client_source=web&stock_list={code}")

# 负面公告硬雷词
BEAR_KW = ["减持", "质押", "违规", "处罚", "立案", "警示", "预亏", "预减", "退市", "解禁",
           "终止", "下修", "爆雷", "诉讼", "冻结", "下调", "流拍", "失败", "低于预期",
           "问询函", "监管函", "关注函", "亏损", "逾期", "失信", "被执行", "商誉减值",
           "业绩变脸", "清仓", "减持计划", "立案调查"]

# ==============================================================================
# 基础工具
# ==============================================================================
def fetch_json(url, timeout=15):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "ignore"))


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get_secid(code):
    """股票代码 → 腾讯 secid（sh600000 / sz000001）。"""
    if code.startswith("6"):
        return f"sh{code}"
    return f"sz{code}"


# ==============================================================================
# K线获取与计算
# ==============================================================================
def fetch_kline(code):
    """拉取单只股票60日K线，返回 list of [date, open, close, high, low, vol]。"""
    # 先查缓存
    cached = cache.get_kline(code)
    if cached:
        return cached
    # 缓存未命中，拉取
    try:
        secid = get_secid(code)
        j = fetch_json(URL_KLINE.format(secid=secid))
        data = j.get("data") or {}
        # 腾讯接口可能返回 qfqday 或 day
        klines = data.get("qfqday") or data.get("day") or []
        result = []
        for k in klines:
            if len(k) >= 6:
                result.append([k[0], float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])])
        # 写入缓存
        if result:
            cache.upsert_kline(code, result)
        return result
    except Exception as e:
        log(f"  K线 {code} 失败: {e}")
        return []


def calc_ma(klines, period):
    """计算N日均线（返回最后一个值）。"""
    if len(klines) < period:
        return None
    closes = [k[2] for k in klines[-period:]]
    return sum(closes) / period


def calc_ma_convergence(klines):
    """计算均线粘合度：MA5/MA10/MA20/MA60 的标准差/均值（越小越粘合）。
    返回 (粘合度百分比, ma5, ma10, ma20, ma60)
    """
    ma5 = calc_ma(klines, 5)
    ma10 = calc_ma(klines, 10)
    ma20 = calc_ma(klines, 20)
    ma60 = calc_ma(klines, 60)
    mas = [m for m in [ma5, ma10, ma20, ma60] if m]
    if len(mas) < 3:
        return None, ma5, ma10, ma20, ma60
    avg = sum(mas) / len(mas)
    if avg == 0:
        return None, ma5, ma10, ma20, ma60
    variance = sum((m - avg) ** 2 for m in mas) / len(mas)
    std = variance ** 0.5
    convergence = (std / avg) * 100  # 百分比
    return round(convergence, 2), ma5, ma10, ma20, ma60


def calc_volume_ratio(klines):
    """计算量比：今日成交量 / 过去5日平均成交量。"""
    if len(klines) < 6:
        return None
    today_vol = klines[-1][5]
    avg5_vol = sum(k[5] for k in klines[-6:-1]) / 5
    if avg5_vol == 0:
        return None
    return round(today_vol / avg5_vol, 2)


def calc_sticky_penetration(klines):
    """估算筹码穿透率：上方套牢盘压力（简化版）。
    返回 0-100，值越小表示上方套牢盘越少。
    """
    if len(klines) < 20:
        return None
    current_price = klines[-1][2]
    # 统计近60日中收盘价高于当前价的天数占比（近似套牢盘）
    above = sum(1 for k in klines if k[2] > current_price)
    penetration = (above / len(klines)) * 100
    return round(penetration, 2)


# ==============================================================================
# 负面公告过滤
# ==============================================================================
def check_negative_announcement(code):
    """检查个股近期是否有负面公告。返回 (is_negative, keywords)。"""
    try:
        j = fetch_json(URL_ANN.format(code=code))
        data = j.get("data") or {}
        anns = data.get("list") or []
        hit_kw = []
        for ann in anns[:5]:  # 只看最近5条
            title = ann.get("title") or ""
            for kw in BEAR_KW:
                if kw in title:
                    hit_kw.append(kw)
                    break
        return len(hit_kw) > 0, hit_kw
    except Exception:
        return False, []


# ==============================================================================
# 起爆前夜计算
# ==============================================================================
def calc_boom_score(klines, convergence, vol_ratio, gene_days, sector_zt_count):
    """起爆前夜五项评分（满分100）。
    1. 均线粘合度（30分）：粘合<2%=30, <5%=20, <8%=10, 其他=0
    2. 量比（25分）：1.3-5.0=25, 1.0-1.3=15, 0.8-1.0=8, 其他=0
    3. 位置（20分）：近60日涨幅<30%=20, <50%=12, <80%=5, 其他=0
    4. 涨停基因（15分）：近3日有涨停=15, 近7日=10, 近15日=5, 其他=0
    5. 板块热度（10分）：板块涨停≥5=10, ≥3=7, ≥1=4, 其他=0
    """
    score = 0
    details = {}

    # 1. 均线粘合
    if convergence is not None:
        if convergence < 2:
            s = 30
        elif convergence < 5:
            s = 20
        elif convergence < 8:
            s = 10
        else:
            s = 0
        score += s
        details["convergence"] = {"value": convergence, "score": s}

    # 2. 量比
    if vol_ratio is not None:
        if 1.3 <= vol_ratio <= 5.0:
            s = 25
        elif 1.0 <= vol_ratio < 1.3:
            s = 15
        elif 0.8 <= vol_ratio < 1.0:
            s = 8
        else:
            s = 0
        score += s
        details["volume_ratio"] = {"value": vol_ratio, "score": s}

    # 3. 位置（近60日涨幅）
    if klines and len(klines) >= 2:
        gain_60 = (klines[-1][2] / klines[0][2] - 1) * 100 if klines[0][2] > 0 else 0
        if gain_60 < 30:
            s = 20
        elif gain_60 < 50:
            s = 12
        elif gain_60 < 80:
            s = 5
        else:
            s = 0
        score += s
        details["position"] = {"value": round(gain_60, 2), "score": s}

    # 4. 涨停基因
    if gene_days <= 3:
        s = 15
    elif gene_days <= 7:
        s = 10
    elif gene_days <= 15:
        s = 5
    else:
        s = 0
    score += s
    details["gene"] = {"days": gene_days, "score": s}

    # 5. 板块热度
    if sector_zt_count >= 5:
        s = 10
    elif sector_zt_count >= 3:
        s = 7
    elif sector_zt_count >= 1:
        s = 4
    else:
        s = 0
    score += s
    details["sector"] = {"zt_count": sector_zt_count, "score": s}

    return score, details


def run_boom_scan(conc=6, min_score=50, mcap_max=300):
    """执行起爆前夜全流程计算。"""
    log("=" * 50)
    log("💥 起爆前夜计算启动")
    today_str = datetime.now().strftime("%Y-%m-%d")

    # 1. 构建近15日涨停基因库
    log("📌 步骤1：构建近15日涨停基因库...")
    gene_pool = {}  # code -> {name, last_zt_days, sector}
    for i in range(15):
        d = datetime.now() - timedelta(days=i)
        if d.weekday() >= 5:
            continue  # 跳过周末
        date_str = d.strftime("%Y-%m-%d")
        zt = cache.get_zt_pool(date_str)
        for s in zt:
            code = s["code"]
            if code not in gene_pool:
                gene_pool[code] = {
                    "name": s.get("name"),
                    "last_zt_days": i,
                    "sector": s.get("hybk", "—"),
                    "ltsz": s.get("ltsz", 0),
                }
    log(f"  基因库: {len(gene_pool)} 只")

    # 2. 从快照过滤：涨幅-3%~3% / 非ST / 流通市值≤上限
    log("📌 步骤2：全市场快照过滤...")
    snapshot = cache.get_snapshot()
    snap_map = {s["code"]: s for s in snapshot}
    candidates = []
    for code, gene in gene_pool.items():
        s = snap_map.get(code)
        if not s:
            continue
        zdp = s.get("zdp") or 0
        name = s.get("name") or ""
        ltsz = (s.get("ltsz") or 0) / 1e8  # 转亿
        if not (-3 <= zdp <= 3):
            continue
        if "ST" in name or "*ST" in name or "退" in name:
            continue
        if ltsz > mcap_max:
            continue
        candidates.append({
            "code": code,
            "name": name,
            "price": s.get("price"),
            "zdp": zdp,
            "hs": s.get("hs"),
            "ltsz": ltsz,
            "sector": gene["sector"],
            "gene_days": gene["last_zt_days"],
        })
    log(f"  初筛候选: {len(candidates)} 只（涨幅-3~3% / 非ST / 市值≤{mcap_max}亿 / 有涨停基因）")

    if not candidates:
        log("⚠️ 无候选股，跳过K线分析")
        cache.set_scan_result("boom", today_str, {"candidates": [], "message": "无候选股"})
        return

    # 3. 逐股拉K线 + 计算指标（线程池并发）
    log(f"📌 步骤3：拉取K线并计算指标（并发{conc}）...")
    results = []
    lock = __import__("threading").Lock()
    done = [0]

    def analyze(stock):
        code = stock["code"]
        klines = fetch_kline(code)
        if not klines or len(klines) < 20:
            return None
        convergence, ma5, ma10, ma20, ma60 = calc_ma_convergence(klines)
        vol_ratio = calc_volume_ratio(klines)
        penetration = calc_sticky_penetration(klines)

        # 硬性检查
        if convergence is None or convergence >= 2:
            return None
        if vol_ratio is None or not (1.3 <= vol_ratio <= 5.0):
            return None

        # 板块涨停数
        sector_zt = cache.get_zt_pool(today_str)
        sector_zt_count = sum(1 for z in sector_zt if z.get("hybk") == stock["sector"])

        # 五项评分
        score, details = calc_boom_score(
            klines, convergence, vol_ratio, stock["gene_days"], sector_zt_count
        )

        # 负面公告过滤
        is_neg, neg_kw = check_negative_announcement(code)

        result = {
            **stock,
            "convergence": convergence,
            "vol_ratio": vol_ratio,
            "penetration": penetration,
            "ma5": round(ma5, 2) if ma5 else None,
            "ma10": round(ma10, 2) if ma10 else None,
            "ma20": round(ma20, 2) if ma20 else None,
            "ma60": round(ma60, 2) if ma60 else None,
            "score": score,
            "score_details": details,
            "negative": is_neg,
            "negative_kw": neg_kw,
        }
        with lock:
            done[0] += 1
            if done[0] % 10 == 0:
                log(f"  进度: {done[0]}/{len(candidates)}")
        return result

    with ThreadPoolExecutor(max_workers=conc) as ex:
        futures = [ex.submit(analyze, s) for s in candidates]
        for f in as_completed(futures):
            r = f.result()
            if r and r["score"] >= min_score:
                results.append(r)

    # 按评分降序
    results.sort(key=lambda x: x["score"], reverse=True)
    log(f"✅ 起爆前夜完成: {len(results)} 只候选（评分≥{min_score}）")

    # 写入缓存
    cache.set_scan_result("boom", today_str, {
        "candidates": results,
        "gene_pool_size": len(gene_pool),
        "initial_filter": len(candidates),
        "min_score": min_score,
        "mcap_max": mcap_max,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    return results


# ==============================================================================
# 情绪周期计算
# ==============================================================================
def run_emotion_scan():
    """执行情绪周期五维量化计算。"""
    log("=" * 50)
    log("🎭 情绪周期计算启动")
    today_str = datetime.now().strftime("%Y-%m-%d")

    # 读取数据
    zt_pool = cache.get_zt_pool(today_str)
    dt_pool = cache.get_dt_pool(today_str)
    snapshot = cache.get_snapshot()
    snap_map = {s["code"]: s for s in snapshot}

    # 维度1：最高连板
    max_lb = max((s.get("lbc") or 0) for s in zt_pool) if zt_pool else 0
    lb3_count = sum(1 for s in zt_pool if (s.get("lbc") or 0) >= 3)

    # 维度2：涨停家数
    zt_count = len(zt_pool)

    # 维度3：炸板率（全市场涨幅≥9.5% - 涨停数）/ 曾触涨停数
    touched_zt = sum(1 for s in snapshot if (s.get("zdp") or 0) >= 9.5)
    zbc_count = max(0, touched_zt - zt_count)
    zbc_rate = (zbc_count / touched_zt * 100) if touched_zt > 0 else None

    # 维度4：赚钱效应（昨日涨停股今日平均涨跌幅）
    yest_premium = None
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    yest_zt = cache.get_zt_pool(yesterday)
    if yest_zt:
        premiums = []
        for s in yest_zt:
            snap = snap_map.get(s["code"])
            if snap and snap.get("zdp") is not None:
                premiums.append(snap["zdp"])
        if premiums:
            yest_premium = sum(premiums) / len(premiums)

    # 维度5：跌/涨停比
    dt_count = len(dt_pool)
    dt_ratio = (dt_count / zt_count) if zt_count > 0 else None

    # 连板晋级率（今日≥2板数 / 昨日涨停数）
    promotion_rate = None
    if yest_zt:
        today_lb2 = sum(1 for s in zt_pool if (s.get("lbc") or 1) >= 2)
        promotion_rate = today_lb2 / len(yest_zt) * 100

    # 五维打分
    score = 0
    details = {}

    # 连板（20分）
    if max_lb >= 7:
        s1 = 20
    elif max_lb >= 5:
        s1 = 15
    elif max_lb >= 4:
        s1 = 10
    elif max_lb >= 3:
        s1 = 6
    else:
        s1 = 2
    score += s1
    details["max_lb"] = {"value": max_lb, "score": s1}

    # 涨停家数（20分）
    if zt_count > 80:
        s2 = 20
    elif zt_count >= 50:
        s2 = 14
    elif zt_count >= 20:
        s2 = 8
    else:
        s2 = 2
    score += s2
    details["zt_count"] = {"value": zt_count, "score": s2}

    # 炸板率（15分）
    if zbc_rate is not None:
        if zbc_rate < 20:
            s3 = 15
        elif zbc_rate < 30:
            s3 = 12
        elif zbc_rate < 40:
            s3 = 8
        elif zbc_rate < 50:
            s3 = 4
        else:
            s3 = 0
    else:
        s3 = 8
    score += s3
    details["zbc_rate"] = {"value": round(zbc_rate, 2) if zbc_rate else None, "score": s3}

    # 赚钱效应（15分）
    if yest_premium is not None:
        if yest_premium > 3:
            s4 = 15
        elif yest_premium >= 1:
            s4 = 12
        elif yest_premium >= 0:
            s4 = 8
        elif yest_premium >= -2:
            s4 = 4
        else:
            s4 = 0
    else:
        s4 = 6
    score += s4
    details["yest_premium"] = {"value": round(yest_premium, 2) if yest_premium is not None else None, "score": s4}

    # 跌/涨停比（8分）
    if dt_ratio is not None:
        if dt_ratio == 0:
            s5 = 8
        elif dt_ratio < 0.1:
            s5 = 6
        elif dt_ratio < 0.3:
            s5 = 4
        elif dt_ratio < 0.5:
            s5 = 2
        else:
            s5 = 0
    else:
        s5 = 4
    score += s5
    details["dt_ratio"] = {"value": round(dt_ratio, 2) if dt_ratio else None, "score": s5}

    # 阶段判定（退潮优先）
    fade_signals = 0
    fade_details = []
    if max_lb <= 3:
        fade_signals += 1
        fade_details.append("连板压缩至≤3板")
    if zbc_rate is not None and zbc_rate >= 50:
        fade_signals += 1
        fade_details.append("炸板率≥50%")
    if yest_premium is not None and yest_premium < 0:
        fade_signals += 1
        fade_details.append("赚钱效应转负")
    if dt_count >= 10:
        fade_signals += 1
        fade_details.append("跌停≥10家")
    if promotion_rate is not None and promotion_rate < 20:
        fade_signals += 1
        fade_details.append("晋级率<20%")

    strong_fade = (yest_premium is not None and yest_premium < -2) and (zbc_rate is not None and zbc_rate >= 40)

    if strong_fade or fade_signals >= 3:
        phase = "fade"
        phase_name = "退潮期"
    elif score >= 60:
        phase = "climax"
        phase_name = "高潮期"
    elif score >= 35:
        # 主升硬条件：连板≥5 或 晋级率≥30%
        if max_lb >= 5 or (promotion_rate is not None and promotion_rate >= 30):
            phase = "main"
            phase_name = "主升期"
        else:
            phase = "start"
            phase_name = "启动期（降级）"
    elif score >= 15:
        phase = "start"
        phase_name = "启动期"
    else:
        phase = "ice"
        phase_name = "冰点期"

    # 主线梯队（按行业聚合涨停）
    ladder = {}
    for s in zt_pool:
        sector = s.get("hybk") or "其他"
        if sector not in ladder:
            ladder[sector] = {"name": sector, "stocks": [], "max_lb": 0, "zt_count": 0}
        ladder[sector]["stocks"].append(s)
        ladder[sector]["max_lb"] = max(ladder[sector]["max_lb"], s.get("lbc") or 0)
        ladder[sector]["zt_count"] += 1
    ladder_list = sorted(ladder.values(), key=lambda x: x["zt_count"], reverse=True)[:10]

    result = {
        "phase": phase,
        "phase_name": phase_name,
        "score": score,
        "score_details": details,
        "max_lb": max_lb,
        "lb3_count": lb3_count,
        "zt_count": zt_count,
        "dt_count": dt_count,
        "zbc_rate": round(zbc_rate, 2) if zbc_rate else None,
        "yest_premium": round(yest_premium, 2) if yest_premium is not None else None,
        "dt_ratio": round(dt_ratio, 2) if dt_ratio else None,
        "promotion_rate": round(promotion_rate, 2) if promotion_rate else None,
        "fade_signals": fade_details,
        "ladder": ladder_list,
        "position": {
            "ice": "0~10%",
            "start": "20~30%",
            "main": "50~80%",
            "climax": "逐步降至0",
            "fade": "0%",
        }.get(phase, "—"),
        "action": {
            "ice": "空仓观望，仅在跌停数骤减时极轻仓试错破冰首板",
            "start": "轻仓试错，聚焦最先封板的先锋或一进二换手板",
            "main": "重仓围绕主线，做首次良性分歧低吸或弱转强确认",
            "climax": "只卖不买，分批减仓，去弱留强",
            "fade": "无条件清仓，停止一切低吸和打板",
        }.get(phase, "—"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    cache.set_scan_result("emotion", today_str, result)
    log(f"✅ 情绪周期完成: {phase_name}（{score}/78分），涨停{zt_count}/跌停{dt_count}，最高{max_lb}板")
    return result


# ==============================================================================
# K线批量预取
# ==============================================================================
def prefetch_klines(conc=8):
    """批量预取近15日涨停基因库的K线，存入缓存。"""
    log("=" * 50)
    log("📈 K线批量预取启动")

    # 构建基因库
    gene_codes = set()
    for i in range(15):
        d = datetime.now() - timedelta(days=i)
        if d.weekday() >= 5:
            continue
        zt = cache.get_zt_pool(d.strftime("%Y-%m-%d"))
        for s in zt:
            gene_codes.add(s["code"])

    log(f"  基因库: {len(gene_codes)} 只")

    # 检查哪些已有缓存
    to_fetch = []
    for code in gene_codes:
        if not cache.get_kline(code):
            to_fetch.append(code)
    log(f"  需拉取: {len(to_fetch)} 只（其余已在缓存）")

    if not to_fetch:
        log("✅ 全部已有缓存，无需拉取")
        return

    done = [0]
    lock = __import__("threading").Lock()

    def fetch_one(code):
        fetch_kline(code)
        with lock:
            done[0] += 1
            if done[0] % 20 == 0:
                log(f"  进度: {done[0]}/{len(to_fetch)}")

    with ThreadPoolExecutor(max_workers=conc) as ex:
        list(ex.map(fetch_one, to_fetch))

    log(f"✅ K线预取完成: {len(to_fetch)} 只")


# ==============================================================================
# 主入口
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="凡凡选股 · 盘后深度计算器")
    parser.add_argument("--type", choices=["all", "boom", "emotion", "kline"], default="all",
                        help="计算类型：all=全部, boom=起爆前夜, emotion=情绪周期, kline=K线预取")
    parser.add_argument("--conc", type=int, default=6, help="并发线程数，默认6")
    parser.add_argument("--min-score", type=int, default=50, help="起爆前夜最低评分，默认50")
    parser.add_argument("--mcap-max", type=int, default=300, help="流通市值上限（亿），默认300")
    args = parser.parse_args()

    log("=" * 60)
    log("🔧 凡凡选股 · 盘后深度计算器启动")
    log(f"   计算类型: {args.type} | 并发: {args.conc}")
    log("=" * 60)

    start = time.time()

    if args.type in ("all", "kline"):
        prefetch_klines(conc=args.conc)

    if args.type in ("all", "boom"):
        run_boom_scan(conc=args.conc, min_score=args.min_score, mcap_max=args.mcap_max)

    if args.type in ("all", "emotion"):
        run_emotion_scan()

    elapsed = time.time() - start
    log(f"\n🎉 全部计算完成，总耗时 {elapsed:.1f}s")


if __name__ == "__main__":
    main()
