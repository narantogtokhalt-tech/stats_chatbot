# app_dashboard.py
import os
from typing import Any, Dict, List

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

router = APIRouter(tags=["dashboard"])

# ------------ CONFIG ------------
EXCEL_PATH = os.getenv("DAILY_DATA_XLSX", r"D:\DataAnalystAgent\Daily Data.xlsx")

SHEET_EXPORT_TOTAL = "Нийт Экспорт"
SHEET_IMPORT = "Нийт Экспорт"
SHEET_PRODUCTS = "Экспорт бүтээгдэхүүнээр"
SHEET_EXCHANGE = "Уул уурхайн биржийн арилжаа"
SHEET_COAL_CNY = "Нүүрсний үнэ"

COL_EXPORT_DATE = "огноо"
COL_IMPORT_DATE = "date"
COL_PRODUCTS_DATE = "date"
COL_EXCHANGE_DATE = "Арилжаа явагдсан өдөр"

# Экспорт бүтээгдэхүүн код→нэр
EXPORT_PRODUCTS_META: Dict[str, Dict[str, str]] = {
    "2601": {"code": "2601", "name": "Төмрийн хүдэр, баяжмал", "unit": "тн"},
    "2603": {"code": "2603", "name": "Зэсийн баяжмал", "unit": "тн"},
    "2701": {"code": "2701", "name": "Нүүрс", "unit": "тн"},
    "2709": {"code": "2709", "name": "Газрын тос", "unit": "тн"},
}

# Биржийн төрлүүд
EXCHANGE_TYPES = {
    "Нүүрс": {
        "key": "coal",
        "name": "Нүүрс",
        "unit_scaled": "сая тн",
        "div": 1_000_000,
    },
    "Төмрийн хүдэр, баяжмал": {
        "key": "iron_ore",
        "name": "Төмрийн хүдэр, баяжмал",
        "unit_scaled": "сая тн",
        "div": 1_000_000,
    },
    "Жонш": {
        "key": "fluorspar",
        "name": "Жонш",
        "unit_scaled": "мян. тн",
        "div": 1_000,
    },
    "Зэсийн баяжмал": {
        "key": "copper_conc",
        "name": "Зэсийн баяжмал",
        "unit_scaled": "мян. тн",
        "div": 1_000,
    },
}


# ------------ COMMON HELPERS ------------

def _ensure_excel_exists():
    if not os.path.exists(EXCEL_PATH):
        raise HTTPException(500, f"Excel not found: {EXCEL_PATH}")


def _read_sheet(sheet_name: str) -> pd.DataFrame:
    _ensure_excel_exists()
    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name=sheet_name)
    except Exception as e:
        raise HTTPException(500, f"Failed to read sheet '{sheet_name}': {e}")
    return df


def _nan_to_none(v: Any):
    try:
        x = pd.to_numeric(v, errors="coerce")
    except Exception:
        return None
    return None if pd.isna(x) else float(x)


def _pct(curr: Any, prev: Any):
    try:
        curr = float(curr)
        prev = float(prev)
    except Exception:
        return None
    if prev == 0 or pd.isna(prev):
        return None
    return (curr - prev) / prev * 100.0


def _find_col(cols, must_have=None, any_of=None):
    """
    Колон нэрийг доогуур үсгээр харьцуулж,
    must_have дахь бүх keyword-ийг агуулсан эхний баганыг буцаана.
    """
    must_have = must_have or []
    any_of = any_of or []
    lower = [str(c).strip().lower() for c in cols]
    for orig, low in zip(cols, lower):
        if all(k in low for k in must_have) and (not any_of or any(k in low for k in any_of)):
            return orig
    return None


# =====================================================================================
# A. НИЙТ ЭКСПОРТ – энэ жил / өнгөрсөн жил / YoY  (аль хэдийн зөв ажиллаж байсан)
# =====================================================================================

@router.get("/dashboard/export/total")
def dashboard_export_total():
    """
    Нийт экспортын толгой үзүүлэлт:
    - хамгийн сүүлийн огноо
    - энэ жилийн экспорт
    - өнгөрсөн оны мөн өдрийн дүн
    - YoY %
    """
    df = _read_sheet(SHEET_EXPORT_TOTAL)

    if COL_EXPORT_DATE not in df.columns:
        raise HTTPException(500, f"'{COL_EXPORT_DATE}' багана sheet={SHEET_EXPORT_TOTAL} дээр байх ёстой")

    df[COL_EXPORT_DATE] = pd.to_datetime(df[COL_EXPORT_DATE], errors="coerce")
    df = df.dropna(subset=[COL_EXPORT_DATE]).sort_values(COL_EXPORT_DATE)

    last = df.iloc[-1]

    col_this = (
        _find_col(df.columns, must_have=["экспорт", "энэ", "жил"])
        or _find_col(df.columns, must_have=["экспорт", "одо", "байдал"])
    )
    col_prev = (
        _find_col(df.columns, must_have=["экспорт", "өнгөрсөн", "жил"])
        or _find_col(df.columns, must_have=["экспорт", "мөн", "өдөр"])
        or _find_col(df.columns, must_have=["экспорт", "өмнөх", "жил"])
    )

    if not col_this or not col_prev:
        raise HTTPException(
            500,
            "Нийт экспортын 'энэ жил' / 'өнгөрсөн жил' багануудыг олж чадсангүй. Column нэрээ шалгана уу.",
        )

    v_this = pd.to_numeric(last[col_this], errors="coerce")
    v_prev = pd.to_numeric(last[col_prev], errors="coerce")

    date_val = last[COL_EXPORT_DATE]

    return {
        "date": date_val.strftime("%Y-%m-%d"),
        "export_this_year": _nan_to_none(v_this),
        "export_prev_same_day": _nan_to_none(v_prev),
        "yoy_pct": _pct(v_this, v_prev),
    }


# =====================================================================================
# B. НИЙТ ИМПОРТ – экспортынхтай ижил structure
# =====================================================================================

@router.get("/dashboard/import/total")
def dashboard_import_total():
    """
    Нийт импортын толгой үзүүлэлт:
    'Нийт Экспорт' sheet дээрх:
      - Импорт Энэ жил
      - Импорт Өнгөрсөн жил
    багануудыг ашиглана.
    """
    # Импортын headline-ийг Нийт Экспорт sheet-ээс авна
    df = _read_sheet(SHEET_EXPORT_TOTAL)

    if COL_EXPORT_DATE not in df.columns:
        raise HTTPException(
            500,
            f"'{COL_EXPORT_DATE}' багана sheet={SHEET_EXPORT_TOTAL} дээр байх ёстой",
        )

    df[COL_EXPORT_DATE] = pd.to_datetime(df[COL_EXPORT_DATE], errors="coerce")
    df = df.dropna(subset=[COL_EXPORT_DATE]).sort_values(COL_EXPORT_DATE)

    last = df.iloc[-1]

    # "Импорт Энэ жил", "Импорт Өнгөрсөн жил" багануудыг автоматаар олно
    col_this = (
        _find_col(df.columns, must_have=["импорт", "энэ", "жил"])
        or _find_col(df.columns, must_have=["импорт", "одо", "байдал"])
    )
    col_prev = (
        _find_col(df.columns, must_have=["импорт", "өнгөрсөн", "жил"])
        or _find_col(df.columns, must_have=["импорт", "мөн", "өдөр"])
        or _find_col(df.columns, must_have=["импорт", "өмнөх", "жил"])
    )

    if not col_this or not col_prev:
        raise HTTPException(
            500,
            "Импортын 'Энэ жил' / 'Өнгөрсөн жил' багануудыг олж чадсангүй. "
            "Column нэр ('Импорт Энэ жил', 'Импорт Өнгөрсөн жил')-ээ шалгана уу.",
        )

    v_this = pd.to_numeric(last[col_this], errors="coerce")
    v_prev = pd.to_numeric(last[col_prev], errors="coerce")

    date_val = last[COL_EXPORT_DATE]

    return {
        "date": date_val.strftime("%Y-%m-%d"),
        "import_this_year": _nan_to_none(v_this),
        "import_prev_same_day": _nan_to_none(v_prev),
        "yoy_pct": _pct(v_this, v_prev),
    }

# =====================================================================================
# C. УУЛ УУРХАЙН БИРЖ – 2025-01-01–ээс өнөөг хүртэлх өссөн дүн (cumulative)
# =====================================================================================

@router.get("/dashboard/exchange/timeline")
def dashboard_exchange_2025_cumulative():
    """
    Уул уурхайн биржийн арилжааны 2025 оны 1/1-ээс
    хамгийн сүүлийн огноо хүртэлх биет хэмжээний өссөн дүн:

    - Нүүрс
    - Төмрийн хүдэр, баяжмал
    - Жонш
    - Зэсийн баяжмал

    JSON:
    {
      "from": "2025-01-01",
      "to": "2025-12-02",
      "commodities": [
        { "key": "coal", "name": "Нүүрс", "total_ton": ..., "total_scaled": ..., "unit_scaled": "сая тн" },
        ...
      ]
    }
    """
    df = _read_sheet(SHEET_EXCHANGE)

    if COL_EXCHANGE_DATE not in df.columns:
        raise HTTPException(500, f"'{COL_EXCHANGE_DATE}' багана sheet={SHEET_EXCHANGE} дээр байх ёстой")
    if "Төрөл" not in df.columns or "Хэмжээ /тонн/" not in df.columns:
        raise HTTPException(500, "'Төрөл' эсвэл 'Хэмжээ /тонн/' багана алга байна")

    df[COL_EXCHANGE_DATE] = pd.to_datetime(df[COL_EXCHANGE_DATE], errors="coerce")
    df = df.dropna(subset=[COL_EXCHANGE_DATE])

    start = pd.Timestamp("2025-01-01")
    df_2025 = df[df[COL_EXCHANGE_DATE] >= start]

    if df_2025.empty:
        return {
            "from": start.strftime("%Y-%m-%d"),
            "to": None,
            "commodities": [],
        }

    last_date = df_2025[COL_EXCHANGE_DATE].max()

    commodities: List[Dict[str, Any]] = []
    for t_name, meta in EXCHANGE_TYPES.items():
        key = meta["key"]
        div = meta["div"]
        unit_scaled = meta["unit_scaled"]

        sub = df_2025[df_2025["Төрөл"] == t_name]
        total_raw = pd.to_numeric(sub["Хэмжээ /тонн/"], errors="coerce").sum(min_count=1)

        total_ton = _nan_to_none(total_raw)                  # тонн
        total_scaled = _nan_to_none(total_raw / div) if total_raw is not None else None  # сая/мянган тн

        commodities.append(
            {
                "key": key,
                "name": meta["name"],
                "total_ton": total_ton,
                "total_scaled": total_scaled,
                "unit_scaled": unit_scaled,
            }
        )

    return {
        "from": start.strftime("%Y-%m-%d"),
        "to": last_date.strftime("%Y-%m-%d"),
        "commodities": commodities,
    }


# ============================
#   ХАНШИЙН ӨӨРЧЛӨЛТ (ТӨГ)
# ============================
@router.get("/dashboard/fx/latest")
def dashboard_fx_latest():
    SHEET_FX = "Ханшийн өөрчлөлт"
    DATE_COL = "date"
    RATE_COL = "Албан ханш"

    # 1) Excel унших
    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_FX)
    except:
        raise HTTPException(500, f"Sheet '{SHEET_FX}' not found")

    # 2) Date parse
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL])

    # 3) Сүүлийн өдөр (огноогоор сортлоно)
    last_row = df.sort_values(DATE_COL).iloc[-1]
    last_date = last_row[DATE_COL]
    last_rate = float(last_row[RATE_COL])

    # 4) Өмнөх оны мөн өдөр
    last_year_date = last_date.replace(year=last_date.year - 1)

    # хамгийн ойр таарах огноог хайна
    prev_df = df.loc[df[DATE_COL] == last_year_date]

    if prev_df.empty:
        # яг таарахгүй бол ойрхон өдөр хайна
        prev_df = df.loc[
            (df[DATE_COL] >= last_year_date - pd.Timedelta(days=3)) &
            (df[DATE_COL] <= last_year_date + pd.Timedelta(days=3))
        ].sort_values(DATE_COL)

    if prev_df.empty:
        prev_rate = None
        yoy = None
    else:
        prev_rate = float(prev_df.iloc[0][RATE_COL])
        yoy = ((last_rate - prev_rate) / prev_rate) * 100 if prev_rate else None

    return {
        "date": last_date.strftime("%Y-%m-%d"),
        "rate": last_rate,
        "prev_year_date": last_year_date.strftime("%Y-%m-%d"),
        "prev_year_rate": prev_rate,
        "yoy_pct": round(yoy, 2) if yoy is not None else None,
    }




# =====================================================================================
# D. ЭКСПОРТ БҮТЭЭГДЭХҮҮНЭЭР – он бүрийн САРЫН НИЙЛБЭР (monthly sum)
# =====================================================================================

@router.get("/dashboard/export/products-timeline")
def dashboard_export_products_monthly():
    """
    Экспорт бүтээгдэхүүнээр sheet-ээс:
    - он бүрийн сар сарын нийлбэр (2601, 2603, 2701, 2709)
    JSON:
    {
      "products": [...],
      "monthly": [
        { "year": 2019, "month": 1, "period": "2019-01", "2601": ..., "2603": ..., "2701": ..., "2709": ... },
        ...
      ]
    }
    """
    df = _read_sheet(SHEET_PRODUCTS)

    if COL_PRODUCTS_DATE not in df.columns:
        raise HTTPException(500, f"'{COL_PRODUCTS_DATE}' багана sheet={SHEET_PRODUCTS} дээр байх ёстой")

    df[COL_PRODUCTS_DATE] = pd.to_datetime(df[COL_PRODUCTS_DATE], errors="coerce")
    df = df.dropna(subset=[COL_PRODUCTS_DATE])

    # Жил, сар гаргаж group-by
    df["year"] = df[COL_PRODUCTS_DATE].dt.year
    df["month"] = df[COL_PRODUCTS_DATE].dt.month

    product_codes = list(EXPORT_PRODUCTS_META.keys())

    # numeric болгож аваад сумм хийх үед NaN-уудыг зөв 처리лахын тулд:
    for code in product_codes:
        df[code] = pd.to_numeric(df.get(code), errors="coerce")

    grouped = (
        df.groupby(["year", "month"])[product_codes]
        .sum(min_count=1)
        .reset_index()
        .sort_values(["year", "month"])
    )

    monthly: List[Dict[str, Any]] = []
    for _, row in grouped.iterrows():
        year = int(row["year"])
        month = int(row["month"])
        entry: Dict[str, Any] = {
            "year": year,
            "month": month,
            "period": f"{year}-{month:02d}",
        }
        for code in product_codes:
            entry[code] = _nan_to_none(row.get(code))
        monthly.append(entry)

    products_meta_list = list(EXPORT_PRODUCTS_META.values())

    return JSONResponse(
        {
            "products": products_meta_list,
            "monthly": monthly,
        }
    )

# =====================================================================================
# E. ЭКСПОРТ БҮТЭЭГДЭХҮҮН – ҮНИЙН ДҮНГ САР САРААР (2601-Үнийн дүн, ...)
# =====================================================================================

def _detect_value_col_for_code(df: pd.DataFrame, code: str):
    """
    Жишээ багана нэр:
      - '2601-Үнийн дүн'
      - '2601 - Үнийн дүн'
    гэх мэт байж болох тул колон нэр дотроос
    code + 'үнийн' + 'дүн' гэсэн keyword-үүдийг агуулсан баганыг хайна.
    """
    return _find_col(
        df.columns,
        must_have=[code, "үнийн", "дүн"],
    )


@router.get("/dashboard/export/products-value-monthly")
def dashboard_export_products_value_monthly():
    """
    'Экспорт бүтээгдэхүүнээр' sheet-ээс:
      - 2601-Үнийн дүн
      - 2603-Үнийн дүн
      - 2701-Үнийн дүн
      - 2709-Үнийн дүн
    багануудын ҮНИЙН ДҮН-ийг ЖИЛ БҮРЭЭР нь сумлаж JSON буцаана.

    JSON structure:
    {
      "products": [
        { "code": "2601", "name": "...", "unit": "сая ам.доллар" },
        ...
      ],
      "monthly": [
        {
          "year": 2024,
          "period": "2024",
          "2601": ...,
          "2603": ...,
          "2701": ...,
          "2709": ...
        },
        ...
      ]
    }
    """
    df = _read_sheet(SHEET_PRODUCTS)

    if COL_PRODUCTS_DATE not in df.columns:
        raise HTTPException(
            500,
            f"'{COL_PRODUCTS_DATE}' багана sheet={SHEET_PRODUCTS} дээр байх ёстой",
        )

    # Огноо parse
    df[COL_PRODUCTS_DATE] = pd.to_datetime(df[COL_PRODUCTS_DATE], errors="coerce")
    df = df.dropna(subset=[COL_PRODUCTS_DATE])

    if df.empty:
        return JSONResponse({"products": [], "monthly": []})

    # Жил
    df["year"] = df[COL_PRODUCTS_DATE].dt.year.astype(int)

    product_codes = list(EXPORT_PRODUCTS_META.keys())

    # Тухайн code бүрийн "Үнийн дүн" колон нэрийг олно
    value_cols: Dict[str, str] = {}
    for code in product_codes:
        vcol = _detect_value_col_for_code(df, code)
        if vcol is None:
            continue
        value_cols[code] = vcol
        df[vcol] = pd.to_numeric(df[vcol], errors="coerce")

    if not value_cols:
        raise HTTPException(
            500,
            "2601-Үнийн дүн / 2603-Үнийн дүн / 2701-Үнийн дүн / 2709-Үнийн дүн "
            "багануудын аль нь ч олдсонгүй. Column нэрээ шалгана уу.",
        )

    yearly_rows: List[Dict[str, Any]] = []

    # ЖИЛЭЭР group хийж, үнэ тус бүрийг сумлана
    for year, g in df.groupby("year"):
        row: Dict[str, Any] = {
            "year": int(year),
            "period": f"{int(year)}",
        }
        for code, vcol in value_cols.items():
            total_val = g[vcol].sum(min_count=1)
            row[code] = _nan_to_none(total_val)
        yearly_rows.append(row)

    # Жилээр эрэмбэлнэ
    yearly_rows = sorted(yearly_rows, key=lambda r: r["year"])

    # products metadata – нэгжийг "сая ам.доллар" гэж үзье
    products_meta_list: List[Dict[str, Any]] = []
    for code in product_codes:
        if code in EXPORT_PRODUCTS_META and code in value_cols:
            meta = EXPORT_PRODUCTS_META[code].copy()
            meta["unit"] = "сая ам.доллар"
            products_meta_list.append(meta)

    return JSONResponse(
        {
            "products": products_meta_list,
            "monthly": yearly_rows,
        }
    )

@router.get("/dashboard/coal-cny/latest")
def dashboard_coal_cny_latest():
    DATE_COL = "Огноо"

    COL_16 = '16. Түүхий коксжих нүүрс, НӨАТ-гүй /"Ганц мод" боомт/'
    COL_18 = '18. Угаасан коксжих нүүрс, НӨАТ-гүй /"Ганц мод" боомт/'
    COL_19 = '19. Австралийн коксжих нүүрс, НӨАТ-тэй /"Жинтан" боомт/'

    COL_MAP = {
        COL_16: "Түүхий коксжих нүүрс, НӨАТ-гүй (Ганц мод)",
        COL_18: "Угаасан коксжих нүүрс, НӨАТ-гүй (Ганц мод)",
        COL_19: "Австралийн коксжих нүүрс, НӨАТ-тэй (Жинтан)",
    }

    # 1) Sheet унших
    df = _read_sheet(SHEET_COAL_CNY)

    if DATE_COL not in df.columns:
        raise HTTPException(
            500,
            f"'{DATE_COL}' багана sheet={SHEET_COAL_CNY} дээр байх ёстой",
        )

    # 2) Огноо parse + sort
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL]).sort_values(DATE_COL)

    if df.empty:
        raise HTTPException(500, f"Sheet '{SHEET_COAL_CNY}' дээр мэдээлэл алга")

    # 3) Сүүлийн өдөр
    last_row = df.iloc[-1]
    last_date = last_row[DATE_COL]

    # 4) Өмнөх оны мөн өдөр
    prev_year_date = last_date.replace(year=last_date.year - 1)
    prev_df = df[df[DATE_COL] == prev_year_date]

    # яг таарахгүй бол ±3 хоногийн дотор ойрхон өдрийг хайна
    if prev_df.empty:
        prev_df = df[
            (df[DATE_COL] >= prev_year_date - pd.Timedelta(days=3))
            & (df[DATE_COL] <= prev_year_date + pd.Timedelta(days=3))
        ].sort_values(DATE_COL)

    items = []

    for col, clean_name in COL_MAP.items():
        latest = pd.to_numeric(last_row.get(col), errors="coerce")

        if prev_df.empty:
            prev = None
            yoy = None
        else:
            prev = pd.to_numeric(prev_df.iloc[0].get(col), errors="coerce")
            yoy = ((latest - prev) / prev * 100) if (prev and latest) else None

        items.append(
            {
                "name": clean_name,
                "latest": float(latest) if latest is not None else None,
                "prev_year": float(prev) if prev is not None else None,
                "yoy_pct": round(yoy, 2) if yoy is not None else None,
            }
        )

    return {
        "date": last_date.strftime("%Y-%m-%d"),
        "items": items,
    }

# ============================
#   Sxcoal price – USD & CNY ханш
# ============================

@router.get("/dashboard/sxcoal/fx-latest")
def dashboard_sxcoal_fx_latest():
    SHEET = "Sxcoal price"  # эсвэл SHEET_SXCOAL_PRICE ашиглаж болно
    DATE_COL = "Огноо"
    USD_COL = "Ам.доллар ханш"
    CNY_COL = "Юань ханш"

    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET)
    except Exception:
        raise HTTPException(500, f"Sheet '{SHEET}' not found")

    if DATE_COL not in df.columns:
        raise HTTPException(500, f"'{DATE_COL}' багана sheet={SHEET} дээр байх ёстой")

    # 1) Огноо parse + sort
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL]).sort_values(DATE_COL)

    if df.empty:
        raise HTTPException(500, f"Sxcoal price sheet '{SHEET}' is empty")

    # 2) Сүүлийн өдөр
    last_row = df.iloc[-1]
    last_date = last_row[DATE_COL]

    usd_latest = pd.to_numeric(last_row.get(USD_COL), errors="coerce")
    cny_latest = pd.to_numeric(last_row.get(CNY_COL), errors="coerce")

    # 3) Өмнөх оны мөн өдөр
    prev_year_date = last_date.replace(year=last_date.year - 1)

    prev_df = df[df[DATE_COL] == prev_year_date]

    # яг таарсан өдөр байхгүй бол ±3 хоногийн дотор хамгийн ойрхон өдрийг хайна
    if prev_df.empty:
        prev_df = df[
            (df[DATE_COL] >= prev_year_date - pd.Timedelta(days=3)) &
            (df[DATE_COL] <= prev_year_date + pd.Timedelta(days=3))
        ].sort_values(DATE_COL)

    if prev_df.empty:
        usd_prev = None
        cny_prev = None
    else:
        prev_row = prev_df.iloc[0]
        usd_prev = pd.to_numeric(prev_row.get(USD_COL), errors="coerce")
        cny_prev = pd.to_numeric(prev_row.get(CNY_COL), errors="coerce")

    # 4) YoY %
    usd_yoy = _pct(usd_latest, usd_prev) if usd_latest is not None and usd_prev is not None else None
    cny_yoy = _pct(cny_latest, cny_prev) if cny_latest is not None and cny_prev is not None else None

    return {
        "date": last_date.strftime("%Y-%m-%d"),
        "usd": {
            "latest": _nan_to_none(usd_latest),
            "prev_year": _nan_to_none(usd_prev),
            "yoy_pct": usd_yoy,
        },
        "cny": {
            "latest": _nan_to_none(cny_latest),
            "prev_year": _nan_to_none(cny_prev),
            "yoy_pct": cny_yoy,
        },
    }

