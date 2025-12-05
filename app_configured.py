from __future__ import annotations
import os
import io
import re
import json
import contextlib
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd
import pytz
from dateutil import parser as dateparser, relativedelta

from fastapi import FastAPI, Header, HTTPException, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from app_reports import router as reports_router
from app_dashboard import router as dashboard_router
from google import genai
from jsonschema import validate, ValidationError

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from dotenv import load_dotenv
load_dotenv()


# ---------------- ENV & CONSTANTS ----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.getenv("DATA_DIR", BASE_DIR)
EXCEL_PATH = os.getenv("EXCEL_PATH", os.path.join(DATA_DIR, "Daily Data.xlsx"))

COLUMN_SYNS_FILE = os.getenv("COLUMN_SYNS_FILE", os.path.join(DATA_DIR, "column_synonyms.json"))
FILTERS_MAP_FILE = os.getenv("FILTERS_MAP_FILE", os.path.join(DATA_DIR, "filters_map.json"))
INTENT_SCHEMA_FILE = os.getenv("INTENT_SCHEMA_FILE", os.path.join(DATA_DIR, "intent_schema.json"))
INTENT_PROMPTS_FILE = os.getenv("INTENT_PROMPTS_FILE", os.path.join(DATA_DIR, "intent_prompts.json"))
INTENT_EXAMPLES_FILE = os.getenv(
    "INTENT_EXAMPLES_FILE",
    os.path.join(DATA_DIR, "intent_examples.json"),
)

TIMEZONE = os.getenv("TIMEZONE", "Asia/Ulaanbaatar")
API_KEY = os.getenv("API_KEY", "secret123")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

TZ = pytz.timezone(TIMEZONE)


class JSONResponseUTF8(JSONResponse):
    media_type = "application/json; charset=utf-8"


app = FastAPI(
    title="Excel Data LLM (Gemini)",
    version="2025.11.13",
    default_response_class=JSONResponseUTF8,
)

# Frontend-үүдээ энд жагсаана:
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    # Хэрвээ Netlify дээр тавих бол доор hostname-аа нэмнэ:
    "https://medchatly.netlify.app/",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,          # эсвэл ["*"] гэж туршилтад болно
    allow_credentials=True,
    allow_methods=["*"],            # GET, POST, ... бүгд
    allow_headers=["*"],            # Content-Type, x-api-key гэх мэт
)

# ---------------- AUTH ----------------
async def require_key(request: Request, x_api_key: Optional[str] = Header(None)) -> None:
    key = x_api_key or request.query_params.get("api_key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ---------------- LLM CLIENT ----------------
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY missing in environment")

gclient = genai.Client(api_key=GEMINI_API_KEY)

# ---------------- STATE (CACHED) ----------------
DATA: Dict[str, Dict[str, Any]] = {}
ALLOWED_SHEETS: List[str] = []
LAST_RELOAD_AT: Optional[str] = None

COLUMN_SYNS: Dict[str, Any] = {}
FILTERS_MAP: Dict[str, Any] = {}
INTENT_SCHEMA: Dict[str, Any] = {}
INTENT_PROMPTS: Dict[str, str] = {}
INTENT_EXAMPLES: List[Dict[str, Any]] = []


# ---------------- HELPERS ----------------
def _norm(s: Any) -> str:
    return str(s).strip().casefold()


def _load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _parse_date_cols(df: pd.DataFrame) -> Optional[pd.Series]:
    # 1) Огноо / date нэртэй баганууд
    cand_names = [c for c in df.columns if re.search(r"(огноо|date|өдөр)", str(c), flags=re.I)]
    for c in cand_names:
        ds = pd.to_datetime(df[c], errors="coerce")
        if ds.notna().any():
            return ds

    # 2) он/сар/өдөр тусдаа
    cols = {"он": None, "сар": None, "өдөр": None}
    for c in df.columns:
        n = _norm(c)
        if n == "он":
            cols["он"] = c
        if n == "сар":
            cols["сар"] = c
        if n == "өдөр":
            cols["өдөр"] = c
    if all(cols.values()):
        ds = pd.to_datetime(
            dict(
                year=pd.to_numeric(df[cols["он"]], errors="coerce"),
                month=pd.to_numeric(df[cols["сар"]], errors="coerce"),
                day=pd.to_numeric(df[cols["өдөр"]], errors="coerce"),
            ),
            errors="coerce",
        )
        return ds

    # 3) fallback: бүх багануудаас цаг агуулсан эсэхийг шалгана
    for c in df.columns:
        ds = pd.to_datetime(df[c], errors="coerce")
        if ds.notna().any():
            return ds

    return None


def load_excel(path: str) -> Dict[str, Dict[str, Any]]:
    all_sheets = pd.read_excel(path, sheet_name=None)
    out: Dict[str, Dict[str, Any]] = {}
    for s_name, df0 in all_sheets.items():
        df = df0.copy()
        ds = _parse_date_cols(df)
        if ds is None:
            continue
        df["_DATE"] = ds.dt.date
        df["_YEAR"] = ds.dt.year
        df["_MONTH"] = ds.dt.month
        out[str(s_name)] = {"df": df, "headers": list(df.columns)}
    return out


def perform_reload() -> Dict[str, Any]:
    global DATA, ALLOWED_SHEETS, LAST_RELOAD_AT
    global COLUMN_SYNS, FILTERS_MAP, INTENT_SCHEMA, INTENT_PROMPTS, INTENT_EXAMPLES

    if not os.path.exists(EXCEL_PATH):
        raise RuntimeError(f"Excel not found: {EXCEL_PATH}")

    # Excel ачаалах
    DATA = load_excel(EXCEL_PATH)
    ALLOWED_SHEETS = list(DATA.keys())
    LAST_RELOAD_AT = datetime.now(TZ).isoformat()

    # JSON config-ууд ачаалах
    COLUMN_SYNS = _load_json(
        COLUMN_SYNS_FILE,
        {
            "default": {},
            "sheet_overrides": {},
            "units": {},
        },
    )
    FILTERS_MAP = _load_json(
        FILTERS_MAP_FILE,
        {
            "product": {"sheet_column": {}, "synonyms": {}},
            "segment": {"sheet_column": {}, "synonyms": {}},
            "country": {"sheet_column": {}, "synonyms": {}},
        },
    )
    INTENT_SCHEMA = _load_json(INTENT_SCHEMA_FILE, {})
    INTENT_PROMPTS = _load_json(
        INTENT_PROMPTS_FILE,
        {
            "missing_sheet": "Ямар sheet дээрх өгөгдөл сонирхож байгаагаа тодруулна уу.",
            "missing_metric": "Ямар төрлийн үзүүлэлт (тоо, USD, MNT, үнэ г.м.) асууж байгааг тодруулна уу.",
            "missing_date": "Хэдний оны хэдэн сарын/өдрийн байдлаар сонирхож байна вэ?",
            "invalid_chart": "Chart төрөл line/bar/none/box/area-гаас сонгоно уу.",
            "missing_op": "Ямар төрлийн тооцоо вэ? value, avg_rows, avg_months, yoy, avg_weighted-с сонгоно уу.",
        },
    )

    # ⇩⇩⇩ intent_examples.json ачаална ⇩⇩⇩
    INTENT_EXAMPLES = _load_json(INTENT_EXAMPLES_FILE, [])
    if not isinstance(INTENT_EXAMPLES, list):
        INTENT_EXAMPLES = []

    return {
        "ok": True,
        "sheets": ALLOWED_SHEETS,
        "at": LAST_RELOAD_AT,
        "intent_examples": len(INTENT_EXAMPLES),
    }

# ---------------- INTENT (LLM + schema) ----------------
def build_intent_prompt(q: str) -> str:
    """
    LLM-д асуултыг Intent болгон хөрвүүлэх заавар + жишээ (few-shot).
    ЗОРИЛГО: Gemini яг ийм JSON бүтэцтэй intent гаргадаг болгох.
    """
    today = datetime.now(TZ).date().isoformat()
    sheets_str = ", ".join([f'"{s}"' for s in (ALLOWED_SHEETS or ["ALL"])])

    # ---- 1. Суурь заавар (rules) ----
    instr = (
        "ЧАМД НЭГ МОНГОЛ ХЭЛ ДЭЭРХ АСУУЛТ ӨГНӨ.\n"
        "ЧИ ЗӨВХӨН JSON ОБЪЕКТ БУЦААНА. ӨӨР ТЕКСТ БИЧИХГҮЙ.\n"
        "JSON бүтэц нь дараах Keys-тай байна:\n"
        "  - sheet : аль sheet-ээс авахыг заана. ALLOWED_SHEETS дотроос нэгийг сонго.\n"
        "  - metric : үзүүлэлтийн түлхүүр (ж: qty_ton, value_usd, value_mnt, price_usd, value_today_usd, value_7d_avg, value_month_avg, qty_cum ...).\n"
        "  - op : \"value\" | \"avg_rows\" | \"avg_months\" | \"yoy\" | \"avg_weighted\".\n"
        "  - period : \"day\" эсвэл \"month\".\n"
        "  - date : \"YYYY-MM-DD\" формат.\n"
        "  - months : бүх тохиолдолд заавал integer; зөвхөн op=\"avg_months\" үед ашиглана. БУСАД ҮЕД ЗҮГЭЭР ДҮФОЛТ 3 байж болно.\n"
        "  - filters : object (ж: {\"product\": \"нүүрс\"} гэх мэт бүтээгдэхүүн, сегмент гэх мэт фильтр тавина).\n"
        "  - chart : \"line\" | \"bar\" | \"none\" | \"box\" | \"area\".\n"
        "\n"
        "ALLOWED_SHEETS дараах байна:\n"
        f"  ALLOWED_SHEETS = [{sheets_str}]\n"
        "\n"
        "Онцгой дүрмүүд:\n"
        f"  - \"өнөөдөр\" гэж байвал date = \"{today}\".\n"
        f"  - \"өчигдөр\" гэж байвал date = өнөөдөр - 1 өдөр.\n"
        "  - \"энэ сар\", \"[YYYY оны] [N] сар\" гэж байвал period=\"month\" гэж ойлгож, date = тухайн сарын 1-ний өдөр болго.\n"
        "  - \"сарын дундаж\" гэж байвал op=\"avg_rows\", period=\"month\".\n"
        "  - \"сарын нийлбэр\", \"сарын нийт\", \"нийт дүн\" гэх мэт байвал op=\"value\", period=\"month\".\n"
        "  - \"сүүлийн N сар\" гэвэл op=\"avg_months\", months=N.\n"
        "  - \"мөн үе\", \"өмнөх оны мөн үе\" гэвэл op=\"yoy\".\n"
        "  - Хэрвээ chart төрлийг дурдоогүй байвал chart=\"line\" гэж үз.\n"
        "\n"
        "Нэмэлт нарийн дүрэм:\n"
        "  1) Хэрвээ асуултанд \"нийт экспорт\" гэж байвал ихэвчлэн sheet=\"Нийт Экспорт\".\n"
        "  2) Хэрвээ \"нийт импорт\" байвал sheet=\"Нийт Импорт\".\n"
        "  3) Хэрвээ \"нүүрсний экспорт\", \"зэсийн экспорт\", \"төмрийн экспорт\", \"газрын тосны экспорт\" гэвэл sheet=\"Экспорт бүтээгдэхүүнээр\" гэж сонгож,\n"
        "     filters.product-ийг \"нүүрс\" / \"зэс\" / \"төмөр\" / \"газрын тос\" гэж оноо.\n"
        "  4) \"Хүнсний бүтээгдэхүүний импорт\", \"нефтийн бүтээгдэхүүний импорт\" гэх мэт байвал sheet=\"Импорт бүтээгдэхүүнээр\" гэж сонгож,\n"
        "     filters.product-ийг \"хүнсний бүтээгдэхүүн\", \"нефтийн бүтээгдэхүүн\", \"автомашин, машин техник\", \"бусад\" гэх мэтээр оноо.\n"
        "  5) Нийт экспорт / импортын sheet дээрх \"сарын өссөн\" эсвэл \"сарын дүн\" гэж асуусан бол op=\"value\", period=\"month\" гэж ойлго.\n"
        "  6) Хэрвээ асуулт сар/жил/өдрийн тодорхой огноо хэлсэн бол тэрийг бүгдийг \"date\" дээр зөв YYYY-MM-DD болгож өг.\n"
        "\n"
        "ГОЛ НЬ: ФИНАЛ ГАРАЛТ НЬ ЗӨВХӨН JSON ОБЪЕКТ БАЙНА. `\"intent\"` гэх нэмэлт wrapper, тайлбар, markdown, текст БҮҮ БИЧ.\n"
        "ЗӨВХӨН ИНТЕНТИЙН JSON.\n"
        "\n"
    )

    # ---- 2. Few-shot жишээнүүд ----
    fewshot_block = ""
    if INTENT_EXAMPLES:
        max_examples = min(15, len(INTENT_EXAMPLES))
        selected = INTENT_EXAMPLES[:max_examples]

        lines = ["ЖИШЭЭ АСУУЛТУУД БА ТЭДНИЙ INTENT JSON:\n"]
        for ex in selected:
            q_ex = ex.get("question", "")
            intent_ex = ex.get("intent", {})
            intent_str = json.dumps(intent_ex, ensure_ascii=False)
            lines.append(f"Q: {q_ex}\nINTENT: {intent_str}\n")

        fewshot_block = "\n".join(lines) + "\n"

    final_prompt = (
        instr
        + fewshot_block
        + "ОДООХ АСУУЛТ:\n"
        f"Q: {q}\n"
        "ЗӨВХӨН INTENT-ИЙН JSON-ЫГ БУЦАА. ӨӨР ЮУ Ч БИЧИХГҮЙ.\n"
    )

    return final_prompt

def llm_json(prompt: str) -> Dict[str, Any]:
    """
    Gemini-ээс STRICT JSON авах. Алдаа гарвал консол дээр лог бичээд
    default intent буцаана.
    """
    try:
        resp = gclient.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config={"response_mime_type": "application/json"},
        )

        txt = getattr(resp, "text", None) or getattr(resp, "output_text", None)
        if not isinstance(txt, str):
            raise ValueError(f"Gemini response has no text: {resp}")

        return json.loads(txt.strip())
    except Exception as e:
        print("Gemini error in llm_json:", repr(e))

        today = datetime.now(TZ).date().isoformat()
        sheet = ALLOWED_SHEETS[0] if ALLOWED_SHEETS else None
        return {
            "sheet": sheet,
            "metric": "value_usd",
            "op": "value",
            "period": "month",
            "date": today,
            "months": 3,
            "filters": {},
            "chart": "line",
        }

def llm_chat(prompt: str) -> str:
    """
    Ерөнхий текстэн чат / тайлбар авахад ашиглана.
    """
    try:
        resp = gclient.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
        )
        txt = getattr(resp, "text", None) or getattr(resp, "output_text", None)
        if not isinstance(txt, str):
            raise ValueError(f"Gemini chat response has no text: {resp}")
        return txt.strip()
    except Exception as e:
        print("Gemini error in llm_chat:", repr(e))
        return "AI загвараас хариу авахад алдаа гарлаа. Дараа дахин оролдоно уу."

def validate_intent(intent: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    if not INTENT_SCHEMA:
        return True, None
    try:
        validate(instance=intent, schema=INTENT_SCHEMA)
        return True, None
    except ValidationError as e:
        return False, str(e)


# ---------------- COLUMN RESOLUTION ----------------
def find_metric_column(sheet: str, metric_key: str) -> Optional[str]:
    info = DATA.get(sheet)
    if not info:
        return None
    headers = info["headers"]
    hn = [_norm(h) for h in headers]

    # Нийт Экспорт / Нийт Импорт дээр тусгай дүрэм
    if sheet == "Нийт Экспорт":
        if metric_key == "value_mnt":
            for h in headers:
                n = _norm(h)
                if "экспорт" in n and "энэ жил" in n and any(x in n for x in ["төг", "төгрөг", "mnt", "сая"]):
                    return h
            for h in headers:
                n = _norm(h)
                if "экспорт" in n and "энэ жил" in n:
                    return h
        if metric_key == "value_usd":
            for h in headers:
                n = _norm(h)
                if "экспорт" in n and "энэ жил" in n and any(x in n for x in ["usd", "ам.доллар", "ам доллар"]):
                    return h
            for h in headers:
                n = _norm(h)
                if "экспорт" in n and "энэ жил" in n:
                    return h

    if sheet == "Нийт Импорт":
        if metric_key == "value_mnt":
            for h in headers:
                n = _norm(h)
                if "импорт" in n and "энэ жил" in n and any(x in n for x in ["төг", "төгрөг", "mnt", "сая"]):
                    return h
            for h in headers:
                n = _norm(h)
                if "импорт" in n and "энэ жил" in n:
                    return h
        if metric_key == "value_usd":
            for h in headers:
                n = _norm(h)
                if "импорт" in n and "энэ жил" in n and any(x in n for x in ["usd", "ам.доллар", "ам доллар"]):
                    return h
            for h in headers:
                n = _norm(h)
                if "импорт" in n and "энэ жил" in n:
                    return h

    # 1) sheet overrides
    over = (COLUMN_SYNS.get("sheet_overrides", {}) or {}).get(sheet, {})
    if metric_key in over:
        for cand in over[metric_key]:
            for h, hraw in zip(hn, headers):
                if _norm(cand) == h or _norm(cand) in h:
                    return hraw

    # 2) default synonyms
    syns = (COLUMN_SYNS.get("default") or {}).get(metric_key, [])
    for s in syns:
        s_n = _norm(s)
        for h, hraw in zip(hn, headers):
            if s_n == h or s_n in h:
                return hraw

    # 3) heuristic
    mkey = _norm(metric_key)
    for h, hraw in zip(hn, headers):
        if mkey and mkey in h:
            return hraw

    cur_hints: list[str] = []
    if "usd" in mkey or "ам.доллар" in mkey or "ам доллар" in mkey:
        cur_hints = ["usd", "ам.доллар", "ам доллар", "$"]
    elif "mnt" in mkey or "төг" in mkey or "төгрөг" in mkey:
        cur_hints = ["mnt", "төг", "төгрөг"]

    if cur_hints:
        for h, hraw in zip(hn, headers):
            if any(x in h for x in cur_hints):
                return hraw

    df = info["df"]
    num_cols: List[str] = []
    date_like_tokens = ("огноо", "date", "өдөр", "он", "year", "сар", "month")
    for c in headers:
        if c in ["_DATE", "_YEAR", "_MONTH"]:
            continue
        if any(tok in _norm(c) for tok in date_like_tokens):
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        if s.notna().sum() >= max(5, int(0.3 * len(s))):
            num_cols.append(c)

    return num_cols[0] if num_cols else None

def _pick_export_product_column(headers: list[str], metric_key: str, filters: Dict[str, Any]) -> Optional[str]:
    """
    Экспорт бүтээгдэхүүнээр sheet:
    product фильтерээс хамаараад 2601 / 2603 / 2701 / 2709 аль баганыг сонгохыг шийднэ.
    """
    sheet_products = {
        "төмөр": "2601",
        "төмрийн хүдэр": "2601",
        "төмрийн хүдэр, баяжмал": "2601",
        "төмрийн": "2601",

        "зэс": "2603",
        "зэсийн баяжмал": "2603",
        "зэсийн": "2603",

        "нүүрс": "2701",
        "нүүрсний": "2701",

        "газрын тос": "2709",
        "тос": "2709",
    }

    product = (filters or {}).get("product")
    if not product:
        return None

    p = _norm(product)
    code = None

    for name, c in sheet_products.items():
        if _norm(name) in p:
            code = c
            break

    if not code and re.fullmatch(r"\d{4}", product.strip()):
        code = product.strip()

    if not code:
        return None

    if metric_key in ("qty", "qty_ton"):
        target = code
    elif metric_key == "qty_cum":
        target = f"Өссөн {code}"
    elif metric_key in ("value", "value_usd"):
        target = f"{code}-Үнийн дүн"
    elif metric_key in ("price", "price_usd"):
        target = f"Үнэ {code}"
    else:
        target = code

    target_n = _norm(target)
    for h in headers:
        if _norm(h) == target_n or target in h:
            return h
    return None

def _pick_import_product_column(headers: list[str], metric_key: str, filters: Dict[str, Any]) -> Optional[str]:
    """
    Импорт бүтээгдэхүүнээр sheet:
    product фильтерээс хамаараад
      Нийт импорт / Хүнсний бүтээгдэхүүн / Нефтийн бүтээгдэхүүн / Автомашин, машин техник / Бусад
    аль баганыг авахыг шийднэ.
    """
    base_names = {
        "нийт импорт": "Нийт импорт",
        "бүх импорт": "Нийт импорт",
        "хүнсний бүтээгдэхүүн": "Хүнсний бүтээгдэхүүн",
        "хүнсний": "Хүнсний бүтээгдэхүүн",
        "нефтийн бүтээгдэхүүн": "Нефтийн бүтээгдэхүүн",
        "нефть": "Нефтийн бүтээгдэхүүн",
        "шатахуун": "Нефтийн бүтээгдэхүүн",
        "fuel": "Нефтийн бүтээгдэхүүн",
        "автомашин, машин техник": "Автомашин, машин техник",
        "автомашин": "Автомашин, машин техник",
        "машин техник": "Автомашин, машин техник",
        "vehicle": "Автомашин, машин техник",
        "бусад": "Бусад",
        "other": "Бусад",
    }

    product = (filters or {}).get("product")
    if not product:
        return None

    p = _norm(product)
    base = None
    for key, name in base_names.items():
        if _norm(key) in p:
            base = name
            break

    if not base:
        return None

    if metric_key in ("value", "value_usd"):
        col_name = base
    elif metric_key == "value_today_usd":
        col_name = f"{base} ТухайнӨ"
    elif metric_key == "value_7d_avg":
        col_name = f"{base} 7 өдрийн дундаж"
    elif metric_key == "value_month_avg":
        col_name = f"{base} Сарын дундаж"
    else:
        col_name = base

    col_n = _norm(col_name)
    for h in headers:
        if _norm(h) == col_n or col_name in h:
            return h
    return None


# ---------------- FILTERS ----------------
def apply_filters(df: pd.DataFrame, sheet: str, filters: Dict[str, str]) -> pd.DataFrame:
    if not filters:
        return df

    for key, val in filters.items():
        col = None
        values = [val]

        map_entry = FILTERS_MAP.get(key)
        if isinstance(map_entry, dict):
            col = (map_entry.get("sheet_column") or {}).get(sheet)
            syns = (map_entry.get("synonyms") or {}).get(val, [])
            values = [val] + syns

        if col and col in df.columns:
            regex = "|".join([re.escape(v) for v in values if v])
            df = df[df[col].astype(str).str.casefold().str.contains(regex.casefold(), na=False)]
        else:
            if key in df.columns:
                df = df[df[key].astype(str).str.casefold().str.contains(str(val).casefold(), na=False)]
    return df


# ---------------- AGGREGATIONS ----------------
def value_op(df: pd.DataFrame, period: str, col: str, ref_day: date) -> Optional[float]:
    if period == "day":
        hit = df[df["_DATE"] == ref_day]
    else:
        hit = df[(df["_YEAR"] == ref_day.year) & (df["_MONTH"] == ref_day.month)]
    if hit.empty:
        return None
    return float(pd.to_numeric(hit[col], errors="coerce").sum())


def avg_rows_op(df: pd.DataFrame, period: str, col: str, ref_day: date) -> Optional[float]:
    if period == "day":
        hit = df[df["_DATE"] == ref_day]
    else:
        hit = df[(df["_YEAR"] == ref_day.year) & (df["_MONTH"] == ref_day.month)]
    if hit.empty:
        return None
    s = pd.to_numeric(hit[col], errors="coerce").dropna()
    return float(s.mean()) if not s.empty else None


def avg_months_op(df: pd.DataFrame, col: str, ref_day: date, months: int) -> Optional[float]:
    vals: List[float] = []
    cur = date(ref_day.year, ref_day.month, 1)
    for _ in range(months):
        mhit = df[(df["_YEAR"] == cur.year) & (df["_MONTH"] == cur.month)]
        if not mhit.empty:
            s = pd.to_numeric(mhit[col], errors="coerce").dropna()
            if s.empty:
                vals.append(0.0)
            else:
                vals.append(float(s.sum()))
        cur = (cur - relativedelta.relativedelta(months=1)).replace(day=1)
    if not vals:
        return None
    return float(sum(vals) / len(vals))

def ytd_sum_op(df: pd.DataFrame, col: str, year: int, upto_month: int) -> Optional[float]:
    hit = df[(df["_YEAR"] == year) & (df["_MONTH"] <= upto_month)]
    if hit.empty:
        return None
    s = pd.to_numeric(hit[col], errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.sum())

def yoy_op(
    df: pd.DataFrame,
    period: str,
    col: str,
    ref_day: date,
    sheet: str,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if period == "month" and sheet in ("Нийт Экспорт", "Нийт Импорт"):
        cur_val = last_value_in_month(df, col, ref_day)
    else:
        cur_val = value_op(df, period, col, ref_day)

    if period == "day":
        with contextlib.suppress(ValueError):
            prev_day = ref_day.replace(year=ref_day.year - 1)
        if "prev_day" not in locals():
            prev_day = ref_day - timedelta(days=365)

        if sheet in ("Нийт Экспорт", "Нийт Импорт"):
            prev_val = last_value_in_month(df, col, prev_day)
        else:
            prev_val = value_op(df, "day", col, prev_day)
    else:
        prev_day = ref_day - relativedelta.relativedelta(years=1)
        if sheet in ("Нийт Экспорт", "Нийт Импорт"):
            prev_val = last_value_in_month(df, col, prev_day)
        else:
            prev_val = value_op(df, "month", col, prev_day)

    pct = None if (prev_val in (None, 0)) else ((cur_val or 0) - prev_val) / prev_val * 100.0
    return cur_val, prev_val, pct


def last_value_in_month(df: pd.DataFrame, col: str, ref_day: date) -> Optional[float]:
    hit = df[(df["_YEAR"] == ref_day.year) & (df["_MONTH"] == ref_day.month)]
    if hit.empty:
        return None
    s = pd.to_numeric(hit[col], errors="coerce")
    hit = hit.assign(__v=s).dropna(subset=["__v"])
    if hit.empty:
        return None
    if "_DATE" in hit.columns:
        hit = hit.sort_values("_DATE")
    return float(hit["__v"].iloc[-1])


# ---------------- CHARTS ----------------
def _render_base64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    import base64
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("utf-8")


def render_series_chart(dates: List[date], values: List[float], title: str, chart_type: str = "line") -> str:
    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    x = pd.to_datetime(pd.Series(dates))
    y = pd.Series(values, dtype="float64")
    if chart_type == "bar":
        ax.bar(x, y)
    elif chart_type == "area":
        ax.plot(x, y)
        ax.fill_between(x, y, step=None, alpha=0.2)
    else:
        ax.plot(x, y, marker="o")
    ax.set_title(title)
    ax.set_xlabel("Огноо")
    ax.set_ylabel("Утга")
    ax.grid(True, alpha=0.3)
    return _render_base64(fig)


def render_box_chart(values: List[float], title: str) -> str:
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    ax.boxplot(values, vert=True, patch_artist=True, labels=["Recent"])
    ax.set_title(title)
    ax.set_ylabel("Утга")
    ax.grid(True, axis="y", alpha=0.3)
    return _render_base64(fig)


# ---------------- INTENT BUILDING ----------------
def pick_sheet(q: str) -> Optional[str]:
    qn = _norm(q)
    for s in ALLOWED_SHEETS:
        if _norm(s) in qn:
            return s
    return ALLOWED_SHEETS[0] if ALLOWED_SHEETS else None


def looks_numeric_question(q: str) -> bool:
    qn = _norm(q)

    if re.search(r"\d", qn):
        return True

    keywords = [
        "хэд", "дүн", "нийт", "тонн", "хэмжээ", "үнэ",
        "экспорт", "импорт", "уул уурхайн бирж",
        "ханш", "өгөгдөл", "статистик", "тайлан", "үзүүлэлт",
        "өсөлт", "бууралт", "yoy", "сарын дундаж"
    ]
    if any(k in qn for k in keywords):
        return True

    if any(tok in qn for tok in ["он", "сар", "өдөр"]):
        return True

    return False

def build_intent_from_llm(q: str) -> Dict[str, Any]:
    q = q.strip()
    qn = _norm(q)

    raw_intent: Dict[str, Any] = {}
    try:
        prompt = build_intent_prompt(q)
        raw_intent = llm_json(prompt) or {}
        if not isinstance(raw_intent, dict):
            raw_intent = {}
    except Exception as e:
        print("llm_json error:", repr(e))
        raw_intent = {}

    intent: Dict[str, Any] = dict(raw_intent)

    sheet = intent.get("sheet")
    if sheet not in ALLOWED_SHEETS:
        if any(k in qn for k in ["нүүрсний экспорт", "нүүрсний экс", " 2701"]):
            if "Экспорт бүтээгдэхүүнээр" in ALLOWED_SHEETS:
                sheet = "Экспорт бүтээгдэхүүнээр"
        elif any(k in qn for k in ["зэсийн экспорт", "2603"]):
            if "Экспорт бүтээгдэхүүнээр" in ALLOWED_SHEETS:
                sheet = "Экспорт бүтээгдэхүүнээр"
        elif any(k in qn for k in ["төмрийн экспорт", "2601"]):
            if "Экспорт бүтээгдэхүүнээр" in ALLOWED_SHEETS:
                sheet = "Экспорт бүтээгдэхүүнээр"
        elif any(k in qn for k in ["газрын тосны экспорт", "2709"]):
            if "Экспорт бүтээгдэхүүнээр" in ALLOWED_SHEETS:
                sheet = "Экспорт бүтээгдэхүүнээр"
        elif any(k in qn for k in ["нийт экспорт", "экспортын нийт", "экспортын дүн"]):
            if "Нийт Экспорт" in ALLOWED_SHEETS:
                sheet = "Нийт Экспорт"
        elif any(k in qn for k in ["нийт импорт", "импортын нийт", "импортын дүн"]):
            if "Нийт Импорт" in ALLOWED_SHEETS:
                sheet = "Нийт Импорт"
        elif any(k in qn for k in ["бирж", "уул уурхайн бирж"]):
            if "Уул уурхайн биржийн арилжаа" in ALLOWED_SHEETS:
                sheet = "Уул уурхайн биржийн арилжаа"
        elif "импорт бүтээгдэхүүнээр" in qn:
            if "Импорт бүтээгдэхүүнээр" in ALLOWED_SHEETS:
                sheet = "Импорт бүтээгдэхүүнээр"

        if sheet not in ALLOWED_SHEETS:
            sheet = pick_sheet(q)

    intent["sheet"] = sheet

    metric = intent.get("metric")
    allowed_metrics = {
        "qty_ton",
        "value_mnt",
        "value_usd",
        "price_usd",
        "value_today_usd",
        "value_7d_avg",
        "value_month_avg",
        "qty_cum",
    }

    if not metric or metric not in allowed_metrics:
        if any(k in qn for k in ["тонн", "хэмжээ", "volume", "qty"]):
            metric = "qty_ton"
        elif any(k in qn for k in ["үнэ", "price", "ханш"]):
            metric = "price_usd"
        elif any(k in qn for k in ["төг", "төгрөг", "mnt"]):
            metric = "value_mnt"
        else:
            metric = "value_usd"

    intent["metric"] = metric

    period = intent.get("period")
    if period not in ("day", "month"):
        if "сар" in qn and "өдөр" not in qn:
            period = "month"
        else:
            period = "day"
    intent["period"] = period

    def _parse_query_date(text: str) -> date:
        t = _norm(text)

        m = re.search(r"(\d{4})\s*оны?\s*(\d{1,2})\s*сар", t)
        if m:
            y = int(m.group(1))
            mth = int(m.group(2))
            return date(y, mth, 1)

        today = datetime.now(TZ).date()
        if "өчигдөр" in t:
            return today - timedelta(days=1)
        if "өнөөдөр" in t:
            return today

        with contextlib.suppress(Exception):
            return dateparser.parse(text, fuzzy=True).date()

        return today

    ref_day = intent.get("date")
    if isinstance(ref_day, str):
        with contextlib.suppress(Exception):
            ref_day = dateparser.parse(ref_day, fuzzy=True).date()
    if not isinstance(ref_day, date):
        ref_day = _parse_query_date(q)

    ytd_mode = False
    ytd_months: Optional[int] = None
    ytd_year: int = ref_day.year

    m_ytd = re.search(r"(\d{4})\s*оны?\s*эхний\s*(\d+)\s*сар", qn)
    if m_ytd:
        ytd_year = int(m_ytd.group(1))
        ytd_months = int(m_ytd.group(2))
        ytd_mode = True
    else:
        if "эхний хагас жил" in qn:
            y_m = re.search(r"(\d{4})\s*оны?", qn)
            if y_m:
                ytd_year = int(y_m.group(1))
            ytd_months = 6
            ytd_mode = True
        elif "эхний улирал" in qn:
            y_m = re.search(r"(\d{4})\s*оны?", qn)
            if y_m:
                ytd_year = int(y_m.group(1))
            ytd_months = 3
            ytd_mode = True

    if ytd_mode and ytd_months and ytd_months > 0:
        ref_day = date(ytd_year, ytd_months, 1)
        intent["date"] = ref_day.isoformat()
        intent["period"] = "month"
        intent["op"] = "value"
        intent["months"] = ytd_months
    else:
        intent["date"] = ref_day.isoformat()

    filters = intent.get("filters") or {}
    if "product" not in filters:
        if any(k in qn for k in ["нүүрсний", "нүүрсийн", " нүүрс"]):
            filters["product"] = "нүүрс"
        elif any(k in qn for k in ["зэсийн", " зэс"]):
            filters["product"] = "зэс"
        elif any(k in qn for k in ["төмрийн", " төмөр"]):
            filters["product"] = "төмөр"
        elif any(k in qn for k in ["газрын тос", "тосны экспорт", " 2709"]):
            filters["product"] = "газрын тос"
        elif "хүнсний" in qn:
            filters["product"] = "хүнсний бүтээгдэхүүн"
        elif "нефтийн" in qn or "шатахуун" in qn:
            filters["product"] = "нефтийн бүтээгдэхүүн"
        elif "автомашин" in qn or "машин техник" in qn:
            filters["product"] = "автомашин, машин техник"
        elif "бусад" in qn and "нийт" not in qn:
            filters["product"] = "бусад"

    intent["filters"] = filters

    op = intent.get("op")
    if op not in ("value", "avg_rows", "avg_months", "yoy", "avg_weighted"):
        if any(k in qn for k in ["сүүлийн", "last", "past"]) and "сар" in qn:
            op = "avg_months"
        elif any(k in qn for k in ["дундаж", "average"]):
            op = "avg_rows"
        elif any(k in qn for k in ["мөн үе", "yoy", "өмнөх жил"]):
            op = "yoy"
        else:
            op = "value"

    if ytd_mode:
        op = "value"
    intent["op"] = op

    if op == "avg_months":
        m = intent.get("months")
        if not isinstance(m, int) or m <= 0:
            m2 = re.search(r"сүүлийн\s+(\d+)\s*сар", qn)
            if m2:
                m = int(m2.group(1))
            else:
                m = 3
        intent["months"] = m
    else:
        if not ytd_mode:
            intent.pop("months", None)

    chart = intent.get("chart") or "line"
    if chart not in ("line", "bar", "none", "box", "area"):
        chart = "line"
    intent["chart"] = chart

    return intent

# ---------------- CORE COMPUTE ----------------
def compute_from_intent(intent: Dict[str, Any], topn: int = 50) -> Dict[str, Any]:
    sheet = intent.get("sheet")
    if sheet not in DATA:
        return {"error": f"Sheet not found: {sheet}"}

    info = DATA[sheet]
    df = info["df"].copy()
    headers = info["headers"]

    metric_key = (intent.get("metric") or "value_usd").strip()
    if metric_key == "qty":
        metric_key = "qty_ton"
    intent["metric"] = metric_key

    filters = intent.get("filters") or {}

    col: Optional[str] = None

    if sheet == "Экспорт бүтээгдэхүүнээр":
        col = _pick_export_product_column(headers, metric_key, filters)

    if sheet == "Импорт бүтээгдэхүүнээр" and col is None:
        col = _pick_import_product_column(headers, metric_key, filters)

    if col is None:
        col = find_metric_column(sheet, metric_key)

    if not col:
        return {
            "error": "Тоон багана олдсонгүй.",
            "sheet": sheet,
            "metric_key": metric_key,
            "headers": headers,
        }

    df = apply_filters(df, sheet, filters)

    period = intent.get("period", "month")
    ref_day = intent.get("date")
    if isinstance(ref_day, str):
        with contextlib.suppress(Exception):
            ref_day = dateparser.parse(ref_day, fuzzy=True).date()
    if not isinstance(ref_day, date):
        ref_day = datetime.now(TZ).date()

    op = intent.get("op", "value")
    months = int(intent.get("months", 0) or 0)
    chart_type = intent.get("chart", "line")

    title_base = f"{sheet} • {col}"

    if (
        op == "value"
        and period == "month"
        and months > 1
        and sheet == "Экспорт бүтээгдэхүүнээр"
        and metric_key in ("qty_ton", "value_usd")
    ):
        year = ref_day.year
        val = ytd_sum_op(df, col, year, months)
        agg_label = f"{year} оны эхний {months} сарын нийлбэр"

        title = f"{title_base} • {agg_label}"

        xs: List[date] = []
        ys: List[float] = []
        for m in range(1, months + 1):
            mhit = df[(df["_YEAR"] == year) & (df["_MONTH"] == m)]
            s = pd.to_numeric(mhit[col], errors="coerce").dropna()
            ys.append(float(s.sum()) if not s.empty else 0.0)
            xs.append(date(year, m, 1))

        chart = None
        if chart_type != "none":
            if chart_type == "box":
                chart = render_box_chart(ys, title + " — Box")
            else:
                chart = render_series_chart(xs, ys, title, chart_type)

        table = (
            df[(df["_YEAR"] == year) & (df["_MONTH"] <= months)]
            .sort_values("_DATE", ascending=False)
            .head(topn)[["_DATE", col]]
            .rename(columns={"_DATE": "Огноо", col: "Утга"})
        )

        return {
            "value": None if val is None else float(val),
            "unit": col,
            "title": title,
            "chart": chart,
            "table": table.to_dict(orient="records"),
        }

    if op == "value":
        is_cumulative_month = False

        if period == "month" and sheet in ("Нийт Экспорт", "Нийт Импорт"):
            is_cumulative_month = True

        if period == "month" and sheet == "Импорт бүтээгдэхүүнээр" and metric_key in ("value_usd", "value_mnt"):
            is_cumulative_month = True

        if period == "month" and sheet == "Экспорт бүтээгдэхүүнээр" and metric_key == "qty_cum":
            is_cumulative_month = True

        if is_cumulative_month:
            val = last_value_in_month(df, col, ref_day)
            agg_label = "сарын эцсийн үлдэгдэл"
        else:
            val = value_op(df, period, col, ref_day)
            agg_label = "нийлбэр"

        title = f"{title_base} • {ref_day.isoformat()} ({'сар' if period == 'month' else 'өдөр'})"

        if period == "month":
            xs, ys = [], []
            cur = date(ref_day.year, ref_day.month, 1)
            for _ in range(12):
                xs.append(cur)
                if sheet in ("Нийт Экспорт", "Нийт Импорт"):
                    ys.append(last_value_in_month(df, col, cur) or 0)
                else:
                    ys.append(value_op(df, "month", col, cur) or 0)
                cur = (cur - relativedelta.relativedelta(months=1)).replace(day=1)
            xs, ys = list(reversed(xs)), list(reversed(ys))
        else:
            xs = sorted(df["_DATE"].dropna().unique())[-90:]
            ys = [float(pd.to_numeric(df[df["_DATE"] == d][col], errors="coerce").sum()) for d in xs]

        chart = None
        if chart_type != "none":
            if chart_type == "box":
                recent_days = sorted(df["_DATE"].dropna().unique())[-90:]
                vals = [float(pd.to_numeric(df[df["_DATE"] == d][col], errors="coerce").sum()) for d in recent_days]
                chart = render_box_chart(vals, title + " — Box")
            else:
                chart = render_series_chart(xs, ys, title, chart_type)

        table = (
            df.sort_values("_DATE", ascending=False)
            .head(topn)[["_DATE", col]]
            .rename(columns={"_DATE": "Огноо", col: "Утга"})
        )

        return {
            "value": None if val is None else float(val),
            "unit": col,
            "title": title + (f" ({agg_label})" if period == "month" else ""),
            "chart": chart,
            "table": table.to_dict(orient="records"),
        }

    if op == "avg_rows":
        val = avg_rows_op(df, period, col, ref_day)
        title = f"{title_base} (дундаж) • {ref_day.isoformat()} ({'сар' if period == 'month' else 'өдөр'})"

        if period == "month":
            xs, ys = [], []
            cur = date(ref_day.year, ref_day.month, 1)
            for _ in range(12):
                xs.append(cur)
                mhit = df[(df["_YEAR"] == cur.year) & (df["_MONTH"] == cur.month)]
                s = pd.to_numeric(mhit[col], errors="coerce").dropna()
                ys.append(float(s.mean()) if not s.empty else 0)
                cur = (cur - relativedelta.relativedelta(months=1)).replace(day=1)
            xs, ys = list(reversed(xs)), list(reversed(ys))
        else:
            xs = sorted(df["_DATE"].dropna().unique())[-90:]
            ys = []
            for d in xs:
                s = pd.to_numeric(df[df["_DATE"] == d][col], errors="coerce").dropna()
                ys.append(float(s.mean()) if not s.empty else 0)

        chart = None
        if chart_type != "none":
            if chart_type == "box":
                recent_days = sorted(df["_DATE"].dropna().unique())[-90:]
                vals: List[float] = []
                for d in recent_days:
                    s = pd.to_numeric(df[df["_DATE"] == d][col], errors="coerce").dropna()
                    vals.extend([float(x) for x in s.values])
                chart = render_box_chart(vals, title + " — Box")
            else:
                chart = render_series_chart(xs, ys, title, chart_type)

        table = (
            df.sort_values("_DATE", ascending=False)
            .head(topn)[["_DATE", col]]
            .rename(columns={"_DATE": "Огноо", col: "Утга"})
        )

        return {
            "value": None if val is None else float(val),
            "unit": f"avg({col})",
            "title": title,
            "chart": chart,
            "table": table.to_dict(orient="records"),
        }

    if op == "avg_months":
        val = avg_months_op(df, col, ref_day, max(months, 1))
        title = f"{title_base} • сүүлийн {max(months, 1)} сарын дундаж"
        xs, ys = [], []
        cur = date(ref_day.year, ref_day.month, 1)
        for _ in range(max(months, 1)):
            xs.append(cur)
            ys.append(value_op(df, "month", col, cur) or 0)
            cur = (cur - relativedelta.relativedelta(months=1)).replace(day=1)
        xs, ys = list(reversed(xs)), list(reversed(ys))

        chart = None
        if chart_type != "none":
            if chart_type == "box":
                chart = render_box_chart(ys, title + " — Box")
            else:
                chart = render_series_chart(xs, ys, title, chart_type)

        table = (
            pd.DataFrame({"Огноо": [d.isoformat() for d in xs], "Утга": ys})
            .iloc[::-1]
            .head(topn)
            .to_dict(orient="records")
        )

        return {
            "value": None if val is None else float(val),
            "unit": f"avg_{max(months, 1)}m_sum({col})",
            "title": title,
            "chart": chart,
            "table": table,
        }

    if op == "yoy":
        cur_v, prev_v, pct = yoy_op(df, period, col, ref_day, sheet)
        title = f"{title_base} • YoY ({ref_day.isoformat()})"

        if period == "month":
            xs, ys = [], []
            curd = date(ref_day.year, ref_day.month, 1)
            for _ in range(24):
                xs.append(curd)
                ys.append(value_op(df, "month", col, curd) or 0)
                curd = (curd - relativedelta.relativedelta(months=1)).replace(day=1)
            xs, ys = list(reversed(xs)), list(reversed(ys))
        else:
            xs = sorted(df["_DATE"].dropna().unique())[-180:]
            ys = [float(pd.to_numeric(df[df["_DATE"] == d][col], errors="coerce").sum()) for d in xs]

        chart = None
        if chart_type != "none":
            if chart_type == "box":
                chart = render_box_chart(ys[-90:], title + " — Box")
            else:
                chart = render_series_chart(xs, ys, title, chart_type)

        table = (
            df.sort_values("_DATE", ascending=False)
            .head(topn)[["_DATE", col]]
            .rename(columns={"_DATE": "Огноо", col: "Утга"})
        )

        return {
            "current": None if cur_v is None else float(cur_v),
            "previous": None if prev_v is None else float(prev_v),
            "pct": None if pct is None else float(pct),
            "unit": col,
            "title": title,
            "chart": chart,
            "table": table.to_dict(orient="records"),
        }

    return {"error": f"Unknown op: {op}"}

def fmt_number(x: Any) -> str:
    try:
        x = float(x)
        if abs(x) >= 1_000_000_000:
            return f"{x / 1_000_000_000:.2f}B"
        if abs(x) >= 1_000_000:
            return f"{x / 1_000_000:.2f}M"
        return f"{x:,.2f}"
    except Exception:
        return str(x)


# ---------------- MODELS ----------------
class AskRequest(BaseModel):
    question: str
    topn: int = 50

# 🔹 ChatbotWidget-д зориулсан request model
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None


# ---------------- ROUTES ----------------
@app.get("/")
def root():
    return {
        "ok": True,
        "excel": EXCEL_PATH,
        "version": app.version,
        "last_reload": LAST_RELOAD_AT,
        "sheets": ALLOWED_SHEETS,
    }


@app.post("/reload")
async def reload(dep: None = Depends(require_key)):
    return perform_reload()


@app.post("/ask")
async def ask(body: AskRequest, dep: None = Depends(require_key)):
    if not DATA:
        perform_reload()

    q = body.question.strip()

    if not looks_numeric_question(q):
        chat_prompt = f"""
Та бол Монгол хэл дээр ярьдаг туслах чатбот.

Таны гол зорилго:
- Энэ API нь Excel дээрх экспорт, импорт, ханш, уул уурхайн биржийн өгөгдөл дээр
  тоон шинжилгээ хийж өгдөг гэдгийг тайлбарлаж өгч болно.
- Хэрэглэгчийн ерөнхий асуултад (энэ системийг яаж ашиглах, юу асууж болох гэх мэт)
  ойлгомжтой, найрсаг байдлаар хариул.
- Хэрвээ хэрэглэгч тодорхой тоон асуулт асуусан бол
  "энэ асуултыг шууд тоон шинжилгээнд ашиглаж болно" гэж зөвлөх маягаар чиглүүлж болно.
- Код, API, техникийн талаар асуувал товч, ойлгомжтой техник тайлбар өгч болно.

Хэрэглэгчийн асуулт:
{q}
"""
        answer = llm_chat(chat_prompt)
        return {"answer": answer, "intent": None, "result": None}

    intent = build_intent_from_llm(q)

    valid, err = validate_intent(intent)
    if not valid:
        msg = err or "Intent invalid"
        if "sheet" in msg and "is not one of" in msg:
            prompt = INTENT_PROMPTS.get("missing_sheet") or msg
        elif "'metric'" in msg and "is a required property" in msg:
            prompt = INTENT_PROMPTS.get("missing_metric") or msg
        elif "'date'" in msg and "is a required property" in msg:
            prompt = INTENT_PROMPTS.get("missing_date") or msg
        else:
            prompt = msg
        return {
            "answer": prompt,
            "intent": intent,
            "result": {"error": "invalid_intent", "detail": err},
        }

    result = compute_from_intent(intent, topn=body.topn)
    if "error" in result:
        return {
            "answer": "Өгөгдөл олдсонгүй эсвэл тохиргоо бүрдээгүй.",
            "intent": intent,
            "result": result,
        }

    if intent.get("op") == "yoy":
        cur = result.get("current")
        prev = result.get("previous")
        pct = result.get("pct")
        base_answer = (
            f"{result['title']} → Одоогийн: {fmt_number(cur)} | "
            f"Өмнөх: {fmt_number(prev)} | Өөрчлөлт: "
            f"{'—' if pct is None else f'{pct:.2f}%'}"
        )
    else:
        base_answer = f"{result['title']} = {fmt_number(result.get('value'))}"

    try:
        table_preview = (result.get("table") or [])[:10]

        explain_prompt = f"""
Та эдийн засгийн тоон мэдээлэл тайлбарладаг Монгол хэл дээр ярьдаг туслах.

Доорх нь хэрэглэгчийн асуулт, intent JSON, мөн Excel-ээс гарсан тоон үр дүн (result JSON) байна.
Эдгээр дээр үндэслэн хэрэглэгчид ойлгомжтой, товч тайлбар бич.

Шаардлага:
- Тоон дүнг мянга/саяын таслалаар харагдах байдлаар хэл (жи: 1,234,567 гэх мэт).
- Хэрэглэгчийн асуусан огноо (date) болон sheet, product-ийн нэрийг дурд.
- Хэрвээ YoY (pct талбар байгаа) бол хэдэн хувийн өсөлт/бууралтыг тодорхой бич.
- Хэт урт тайлбар биш, 3–6 өгүүлбэр байхад хангалттай.
- Үндсэн тоо, чиг хандлагыг онцолж тайлбарла.

Хэрэглэгчийн асуулт:
{q}

Intent JSON:
{json.dumps(intent, ensure_ascii=False)}

Result (үндсэн талбарууд):
value/current: {result.get('value') or result.get('current')}
previous: {result.get('previous')}
pct: {result.get('pct')}
title: {result.get('title')}
unit: {result.get('unit')}

Table (эхний хэдэн мөр):
{json.dumps(table_preview, ensure_ascii=False)}

Эдгээрийг ашиглаад хэрэглэгчид чиг хандлага, дүнг тайлбарла.
"""
        answer = llm_chat(explain_prompt)
    except Exception as e:
        print("explain_prompt error:", repr(e))
        answer = base_answer

    return {"answer": answer, "intent": intent, "result": result}


# 🔹 ChatbotWidget-д зориулсан /chat endpoint
@app.post("/chat")
async def chat(body: ChatRequest, dep: None = Depends(require_key)):
    """
    ChatbotWidget-ээс ирсэн message-ийг /ask pipeline-руу дамжуулж,
    { answer, meta, result } форматтай буцаана.
    """
    if not DATA:
        perform_reload()

    q = (body.message or "").strip()
    ask_body = AskRequest(question=q, topn=50)
    ask_resp = await ask(ask_body, dep)

    if isinstance(ask_resp, dict):
        data = ask_resp
    else:
        try:
            data = json.loads(getattr(ask_resp, "body", b"{}").decode("utf-8"))
        except Exception:
            data = {"answer": "Дотоод алдаа.", "intent": None, "result": None}

    return {
        "answer": data.get("answer"),
        "meta": {
            "intent": data.get("intent"),
        },
        "result": data.get("result"),
    }

app.include_router(reports_router)
app.include_router(dashboard_router)

# ---------------- STARTUP ----------------
@app.on_event("startup")
async def startup():
    perform_reload()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app_configured:app", host="0.0.0.0", port=8010, reload=False)