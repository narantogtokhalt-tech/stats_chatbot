# app_reports.py
import os
from datetime import datetime
from typing import Dict, Any, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Header
from fastapi.responses import JSONResponse

# ================== CONFIG ==================
EXCEL_PATH = os.getenv("DAILY_DATA_XLSX", r"D:\Ai agent\Daily Data.xlsx")

SHEET_EXCHANGE = "Уул уурхайн биржийн арилжаа"
SHEET_PRODUCTS = "Экспорт бүтээгдэхүүнээр"
SHEET_TOTAL = "Нийт Экспорт"

DATE_COL_EXCHANGE = "Арилжаа явагдсан өдөр"
DATE_COL_PRODUCTS = "date"
DATE_COL_TOTAL = "огноо"

# Тоо хэмжээг тайлан дээр 1000-аар өсгөж харуулах (сая/мянган тонн)
QTY_SCALE = 1000.0

# '*-Үнийн дүн' нэгжийн хөрвүүлэлт: САЯ $ → 1_000_000  |  МЯНГАН $ → 1_000  |  $ → 1
VALUE_SCALE = 1_000_000

# 4 бүтээгдэхүүн (алтгүй)
PRODUCTS = {
    "2601": ("Төмрийн хүдэр", "тн"),
    "2603": ("Зэсийн баяжмал", "тн"),
    "2701": ("Нүүрс", "тн"),
    "2709": ("Газрын тос", "тн"),
}
VALUE_SUFFIXES = ["-Үнийн дүн", " - Үнийн дүн"]  # зайтай/зайгүй аль алиныг танина
DECIMAL_COMMA = True

router = APIRouter(tags=["reports"])

# ================== COMMON HELPERS ==================
def _read_excel(sheet: str) -> pd.DataFrame:
    if not os.path.exists(EXCEL_PATH):
        raise HTTPException(500, f"Excel not found: {EXCEL_PATH}")
    df = pd.read_excel(EXCEL_PATH, sheet_name=sheet)
    return df

def _safe_sum(s: Optional[pd.Series]) -> float:
    if s is None:
        return 0.0
    return pd.to_numeric(s, errors="coerce").sum(min_count=1)

def _yoy(curr: Optional[float], prev: Optional[float]) -> float:
    if prev and not pd.isna(prev) and prev != 0:
        return (float(curr) - float(prev)) / float(prev) * 100.0
    return float("nan")

def _fmt_money_words(x: Optional[float]) -> str:
    if x is None or pd.isna(x): return "—"
    x = float(x)
    if abs(x) >= 1_000_000_000:
        b = int(x // 1_000_000_000)
        rem = x - b * 1_000_000_000
        m = int(round(rem / 1_000_000))
        return f"{b} тэрбум" + (f" {m} сая" if m else "") + " ам.доллар"
    if abs(x) >= 1_000_000:
        m = int(round(x / 1_000_000))
        return f"{m} сая ам.доллар"
    return f"{x:,.0f} ам.доллар"

def _fmt_in_millions_abs(x: Optional[float]) -> str:
    if x is None or pd.isna(x): return "—"
    return f"{(float(x) / 1_000_000):,.0f} $"

def _fmt_qty_words(q: Optional[float], unit: str) -> str:
    if q is None or pd.isna(q): return "—"
    q = float(q)
    mil = int(q // 1_000_000)
    thou = int(round((q - mil * 1_000_000) / 1_000))
    return f"{mil} сая {thou} мянган {unit}"

def _avg_price_usd_per_ton(value_usd: Optional[float], qty_raw_thousand_ton: Optional[float]) -> float:
    # $/тонн = value / (qty_raw × 1000). qty_raw нь 'мянган' нэгжтэй гэж үзнэ.
    if not qty_raw_thousand_ton or pd.isna(qty_raw_thousand_ton):
        return float("nan")
    return float(value_usd) / (float(qty_raw_thousand_ton) * 1000.0)

def _fmt_price_1d(x: Optional[float]) -> str:
    if x is None or pd.isna(x): return "—"
    s = f"{float(x):.1f}"
    return s.replace(".", ",") if DECIMAL_COMMA else s

def _trend_word(v: Optional[float]) -> str:
    try:
        return "өссөн" if float(v) > 0 else "буурсан"
    except:
        return "буурсан"

def _detect_value_col(code: str, cols) -> Optional[str]:
    for suf in VALUE_SUFFIXES:
        c = f"{code}{suf}"
        if c in cols:
            return c
    for c in cols:
        if str(c).startswith(code) and "Үнийн дүн" in str(c):
            return c
    return None

def _find_col(cols, must_have=[], any_of=[]):
    L = [str(c).strip().lower() for c in cols]
    for orig, low in zip(cols, L):
        ok = all(k in low for k in must_have) and (True if not any_of else any(k in low for k in any_of))
        if ok:
            return orig
    return None

# ================== EXCHANGE (Бирж) ==================
def _exchange_summary_text_json() -> Dict[str, Any]:
    df = _read_excel(SHEET_EXCHANGE)
    if DATE_COL_EXCHANGE not in df.columns:
        raise HTTPException(500, f"'{DATE_COL_EXCHANGE}' багана олдсонгүй (sheet={SHEET_EXCHANGE})")
    df[DATE_COL_EXCHANGE] = pd.to_datetime(df[DATE_COL_EXCHANGE], errors="coerce")

    df_2023 = df[df[DATE_COL_EXCHANGE] >= pd.Timestamp("2023-01-01")]
    df_2025 = df[df[DATE_COL_EXCHANGE] >= pd.Timestamp("2025-01-01")]

    def agg(df_subset: pd.DataFrame):
        result = {}
        mapping = {
            "Нүүрс": ("Нүүрс (сая тн)", 1_000_000),
            "Төмрийн хүдэр, баяжмал": ("Төмрийн хүдэр, баяжмал (сая тн)", 1_000_000),
            "Жонш": ("Жонш (мян.тн)", 1_000),
            "Зэсийн баяжмал": ("Зэсийн баяжмал (мян.тн)", 1_000),
        }
        for t, (label, div) in mapping.items():
            qty = df_subset.loc[df_subset["Төрөл"] == t, "Хэмжээ /тонн/"].sum(skipna=True) / div
            result[label] = round(float(qty), 3)
        return result

    s2023 = agg(df_2023)
    s2025 = agg(df_2025)
    today = datetime.today().strftime("%Y-%m-%d")

    def block(title, s):
        lines = [
            f"• Нүүрс: {s['Нүүрс (сая тн)']:,} сая тн",
            f"• Төмрийн хүдэр, баяжмал: {s['Төмрийн хүдэр, баяжмал (сая тн)']:,} сая тн",
            f"• Жонш: {s['Жонш (мян.тн)']:,} мян. тн",
            f"• Зэсийн баяжмал: {s['Зэсийн баяжмал (мян.тн)']:,} мян. тн",
        ]
        return f"{title}\n" + "\n".join(lines)

    text = (
        f"Биржийн арилжааны товч тайлан ({today})\n\n"
        + block("2023-01-01 → одоог хүртэл:", s2023) + "\n\n"
        + block("2025-01-01 → одоог хүртэл:", s2025)
    )
    return {"date": today, "summary": {"from_2023": s2023, "from_2025": s2025}, "text": text}

@router.get("/report/exchange")
def report_exchange():
    data = _exchange_summary_text_json()
    return JSONResponse(data)

# ================== EXPORT PRODUCTS (Экспорт бүтээгдэхүүнээр) ==================
def _export_products_summary_text_json() -> Dict[str, Any]:
    # 1) Read products + total
    dfp = _read_excel(SHEET_PRODUCTS)
    dft = _read_excel(SHEET_TOTAL)

    if DATE_COL_PRODUCTS not in dfp.columns:
        raise HTTPException(500, f"'{DATE_COL_PRODUCTS}' багана sheet={SHEET_PRODUCTS} дээр байх ёстой")
    if DATE_COL_TOTAL not in dft.columns:
        raise HTTPException(500, f"'{DATE_COL_TOTAL}' багана sheet={SHEET_TOTAL} дээр байх ёстой")

    dfp[DATE_COL_PRODUCTS] = pd.to_datetime(dfp[DATE_COL_PRODUCTS], errors="coerce")
    dft[DATE_COL_TOTAL] = pd.to_datetime(dft[DATE_COL_TOTAL], errors="coerce")

    # 2) Periods
    today = datetime.today()
    ytd_start = datetime(today.year, 1, 1)
    ly_start  = datetime(today.year - 1, 1, 1)
    ly_end    = datetime(today.year - 1, today.month, today.day)

    def compute_period(df: pd.DataFrame, start_dt, end_dt):
        d = df[(df[DATE_COL_PRODUCTS] >= start_dt) & (df[DATE_COL_PRODUCTS] <= end_dt)]
        out = {}
        for code, (name, unit) in PRODUCTS.items():
            qty_raw = _safe_sum(d.get(code))
            vcol = _detect_value_col(code, d.columns)
            val_raw = _safe_sum(d.get(vcol)) if vcol else 0.0
            val = val_raw * VALUE_SCALE          # бодит $-д хөрвүүлэлт
            qty_scaled = qty_raw * QTY_SCALE     # тайланд 1000× өсгөсөн (сая/мянган)
            out[code] = {"name": name, "unit": unit, "val": val, "qty_raw": qty_raw, "qty": qty_scaled}
        return out

    ytd = compute_period(dfp, ytd_start, today)
    lytd = compute_period(dfp, ly_start,  ly_end)

    # 3) Headline from total (latest row)
    dft_sorted = dft.sort_values(DATE_COL_TOTAL)
    last_row = dft_sorted.dropna(subset=[DATE_COL_TOTAL]).iloc[-1]

    headline_date = last_row[DATE_COL_TOTAL]
    col_export_this_year = (
        _find_col(dft.columns, must_have=["экспорт", "энэ", "жил"])
        or _find_col(dft.columns, must_have=["экспорт", "одо", "байдал"])
    )
    col_export_prev_same_day = (
        _find_col(dft.columns, must_have=["экспорт", "өнгөрсөн", "жил"])
        or _find_col(dft.columns, must_have=["экспорт", "мөн", "өдөр"])
        or _find_col(dft.columns, must_have=["экспорт", "өмнөх", "жил"])
    )

    export_this = pd.to_numeric(last_row[col_export_this_year], errors="coerce") if col_export_this_year else None
    export_prev = pd.to_numeric(last_row[col_export_prev_same_day], errors="coerce") if col_export_prev_same_day else None
    headline_yoy = _yoy(export_this, export_prev)

    # 4) Build text
    lines = []
    # Нөхцөл байдал — Нийт Экспорт
    lines.append("Экспорт - нөхцөл байдал:")
    lines.append("")
    lines.append(
        f"Экспортын хувьд {headline_date:%m} дугаар сарын {headline_date:%d}-ний өдрийн байдлаар "
        f"{_fmt_money_words(export_this)} хүрч, өмнөх оны мөн үеэс "
        f"{abs(headline_yoy):.1f} хувиар {'өссөн' if (headline_yoy and headline_yoy>0) else 'багассан'} байна."
    )
    lines.append("")
    # Үнийн дүн (сая $)
    lines.append(f"Экспортын үнийн дүн: {today.month:02d} дугаар сарын {today.day:02d}-ний өдрийн байдлаар")
    lines.append("")
    for code in ["2603","2601","2701","2709"]:
        p = ytd[code]; lp = lytd[code]
        val_yoy = _yoy(p['val'], lp['val'])
        sign_pct = "" if pd.isna(val_yoy) else f"{val_yoy:+.0f}%"
        lines.append(f"{p['name']} {_fmt_in_millions_abs(p['val'])}, {sign_pct} {_trend_word(val_yoy)}")
    lines.append("")
    # Биет хэмжээ
    lines.append(f"Экспортын биет хэмжээ: {today.month:02d} дугаар сарын {today.day:02d}-ний өдрийн байдлаар")
    lines.append("")
    for code in ["2603","2601","2701","2709"]:
        p = ytd[code]; lp = lytd[code]
        qty_yoy = _yoy(p['qty'], lp['qty'])
        sign_pct = "" if pd.isna(qty_yoy) else f"{qty_yoy:+.0f}%"
        lines.append(f"{p['name']} {_fmt_qty_words(p['qty'], p['unit'])}, {sign_pct} {_trend_word(qty_yoy)}")
    lines.append("")
    # Дундаж үнэ ($/тонн)
    lines.append(f"Дундаж үнэ ($/тонн): {today.month:02d} дугаар сарын {today.day:02d}-ний өдрийн байдлаар")
    lines.append("")
    for code in ["2603","2601","2701","2709"]:
        p = ytd[code]
        avg_now = _avg_price_usd_per_ton(p["val"], p["qty_raw"])
        yoy_avg = float("nan")
        if code in lytd and lytd[code]["qty_raw"]:
            prev_avg = _avg_price_usd_per_ton(lytd[code]["val"], lytd[code]["qty_raw"])
            yoy_avg = _yoy(avg_now, prev_avg)
        sign_pct = "" if pd.isna(yoy_avg) else f"{yoy_avg:+.0f}%"
        lines.append(f"{p['name']} {_fmt_price_1d(avg_now)} $/тонн, {sign_pct} {_trend_word(yoy_avg)}")

    text = "\n\n".join(lines)

    payload = {
        "date": today.strftime("%Y-%m-%d"),
        "headline": {
            "as_of": headline_date.strftime("%Y-%m-%d"),
            "export_this_year_usd": float(export_this) if export_this is not None and not pd.isna(export_this) else None,
            "export_prev_same_day_usd": float(export_prev) if export_prev is not None and not pd.isna(export_prev) else None,
            "yoy_pct": float(headline_yoy) if not pd.isna(headline_yoy) else None
        },
        "ytd": ytd,
        "ytd_last_year": lytd,
        "text": text
    }
    return payload

@router.get("/report/export-products")
def report_export_products():
    data = _export_products_summary_text_json()
    return JSONResponse(data)

# ================== COMBINED (БОТОД БЭЛЭН НЭГТГЭСЭН ТЕКСТ) ==================
@router.get("/report/daily")
def report_daily():
    ex = _exchange_summary_text_json()
    pr = _export_products_summary_text_json()
    combined_text = (
        ex["text"]
        + "\n\n" + ("—" * 40) + "\n\n"
        + pr["text"]
    )
    return JSONResponse({
        "date": datetime.today().strftime("%Y-%m-%d"),
        "exchange": ex,
        "export_products": pr,
        "text": combined_text  # чатбот дээр шууд үзүүлэх товч текст
    })

# ================== OPTIONAL: EMAIL SEND (secured by header) ==================
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")  # "a@b.mn,c@d.mn"
EMAIL_TOKEN = os.getenv("EMAIL_TOKEN", "")  # энгийн хамгаалалт

@router.post("/report/email")
def send_email_report(x_auth_token: str = Header(default="")):
    if not EMAIL_TOKEN or x_auth_token != EMAIL_TOKEN:
        raise HTTPException(401, "Unauthorized")
    daily = report_daily().body  # bytes
    # body-г дахин тооцоолно:
    data = report_daily().media.get("body") if hasattr(report_daily(), "media") else None  # fallback safeguard
    # Аюулгүй арга: шууд дахин дуудаж parse хийе
    payload = _exchange_summary_text_json()
    payload2 = _export_products_summary_text_json()
    text = payload["text"] + "\n\n" + ("—" * 40) + "\n\n" + payload2["text"]

    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_TO
    msg["Subject"] = f"Өдөр тутмын экспорт/бирж тайлан - {datetime.today():%Y-%m-%d}"
    msg.attach(MIMEText(text, "plain", _charset="utf-8"))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_APP_PASSWORD)
        recipients = [e.strip() for e in EMAIL_TO.split(",") if e.strip()]
        server.sendmail(EMAIL_SENDER, recipients, msg.as_string())

    return {"status": "ok"}
