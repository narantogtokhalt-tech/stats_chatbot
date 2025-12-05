import json
from datetime import date

INTENT_EXAMPLES_PATH = "intent_examples.json"

def make_intent(
    sheet,
    metric,
    op="value",
    period="month",
    date_str="2024-09-01",
    months=3,
    filters=None,
    chart="line",
):
    return {
        "sheet": sheet,
        "metric": metric,
        "op": op,
        "period": period,
        "date": date_str,
        "months": months,
        "filters": filters or {},
        "chart": chart,
    }

examples = []

def add(question, intent):
    examples.append({"question": question, "intent": intent})

# ------------------------
# 1. Нийт Экспорт (value_usd / value_mnt, month/day, yoy, avg_months)
# ------------------------

years = [2023, 2024, 2025]
months_list = [1, 3, 6, 9, 12]

for y in years:
    for m in months_list:
        d = f"{y}-{m:02d}-01"
        # USD
        q = f"{y} оны {m} сарын нийт экспортын дүн USD-ээр хэд вэ?"
        add(
            q,
            make_intent(
                sheet="Нийт Экспорт",
                metric="value_usd",
                op="value",
                period="month",
                date_str=d,
            ),
        )
        # MNT
        q = f"{y} оны {m} сарын нийт экспортын дүн төгрөгөөр хэд вэ?"
        add(
            q,
            make_intent(
                sheet="Нийт Экспорт",
                metric="value_mnt",
                op="value",
                period="month",
                date_str=d,
            ),
        )

# сарын дундаж / yoy / сүүлийн 3 сар
for y in [2023, 2024]:
    for m in [3, 6, 9, 12]:
        d = f"{y}-{m:02d}-01"
        # сарын дундаж USD
        q = f"{y} оны {m} сарын нийт экспортын сарын дундаж USD-ээр хэд вэ?"
        add(
            q,
            make_intent(
                sheet="Нийт Экспорт",
                metric="value_usd",
                op="avg_rows",
                period="month",
                date_str=d,
            ),
        )
        # yoy
        q = f"{y} оны {m} сарын нийт экспортын дүн өмнөх оны мөн үеэс хэдэн хувь өөрчлөгдсөн бэ?"
        add(
            q,
            make_intent(
                sheet="Нийт Экспорт",
                metric="value_usd",
                op="yoy",
                period="month",
                date_str=d,
            ),
        )
        # сүүлийн 3 сар
        q = f"{y} оны {m} сарын байдлаар сүүлийн 3 сарын нийт экспортын дундаж USD-ээр хэд вэ?"
        add(
            q,
            make_intent(
                sheet="Нийт Экспорт",
                metric="value_usd",
                op="avg_months",
                period="month",
                date_str=d,
                months=3,
            ),
        )

# ------------------------
# 2. Нийт Импорт (экспорттой ижил, импортын текстээр)
# ------------------------

for y in years:
    for m in months_list:
        d = f"{y}-{m:02d}-01"
        # USD
        q = f"{y} оны {m} сарын нийт импортын дүн USD-ээр хэд вэ?"
        add(
            q,
            make_intent(
                sheet="Нийт Экспорт",
                metric="value_usd",
                op="value",
                period="month",
                date_str=d,
            ),
        )
        # MNT
        q = f"{y} оны {m} сарын нийт импортын дүн төгрөгөөр хэд вэ?"
        add(
            q,
            make_intent(
                sheet="Нийт Экспорт",
                metric="value_mnt",
                op="value",
                period="month",
                date_str=d,
            ),
        )

for y in [2023, 2024]:
    for m in [3, 6, 9, 12]:
        d = f"{y}-{m:02d}-01"
        # сарын дундаж
        q = f"{y} оны {m} сарын нийт импортын сарын дундаж USD-ээр хэд вэ?"
        add(
            q,
            make_intent(
                sheet="Нийт Экспорт",
                metric="value_usd",
                op="avg_rows",
                period="month",
                date_str=d,
            ),
        )
        # yoy
        q = f"{y} оны {m} сарын нийт импортын дүн өмнөх оны мөн үеэс хэдэн хувь өөрчлөгдсөн бэ?"
        add(
            q,
            make_intent(
                sheet="Нийт Экспорт",
                metric="value_usd",
                op="yoy",
                period="month",
                date_str=d,
            ),
        )
        # сүүлийн 6 сар
        q = f"{y} оны {m} сарын байдлаар сүүлийн 6 сарын нийт импортын дундаж USD-ээр хэд вэ?"
        add(
            q,
            make_intent(
                sheet="Нийт Экспорт",
                metric="value_usd",
                op="avg_months",
                period="month",
                date_str=d,
                months=6,
            ),
        )

# ------------------------
# 3. Экспорт бүтээгдэхүүнээр (нүүрс/зэс/төмөр/газрын тос)
# ------------------------

products = {
    "нүүрс": "2701",
    "зэс": "2603",
    "төмөр": "2601",
    "газрын тос": "2709",
}

for pname, code in products.items():
    # сарын нийлбэр (тонн)
    for y in [2023, 2024, 2025]:
        for m in [1, 3, 6, 9, 12]:
            d = f"{y}-{m:02d}-01"
            q = f"{y} оны {m} сарын {pname}ийн экспортын хэмжээ хэдэн тонн байсан бэ?"
            add(
                q,
                make_intent(
                    sheet="Экспорт бүтээгдэхүүнээр",
                    metric="qty_ton",
                    op="value",
                    period="month",
                    date_str=d,
                    filters={"product": pname},
                ),
            )

            # USD-ээр дүн
            q = f"{y} оны {m} сарын {pname}ийн экспортын нийт дүн USD-ээр хэд вэ?"
            add(
                q,
                make_intent(
                    sheet="Экспорт бүтээгдэхүүнээр",
                    metric="value_usd",
                    op="value",
                    period="month",
                    date_str=d,
                    filters={"product": pname},
                ),
            )

# 7 өдрийн дундаж, сарын дундаж, үнэ
for pname, code in products.items():
    for y in [2023, 2024]:
        for m in [3, 6, 9, 12]:
            d = f"{y}-{m:02d}-15"
            # 7 өдрийн дундаж тонн
            q = f"{y} оны {m} сарын {pname}ийн 7 өдрийн дундаж экспорт (тонн)-оор хэд вэ?"
            add(
                q,
                make_intent(
                    sheet="Экспорт бүтээгдэхүүнээр",
                    metric="qty_7d_avg",
                    op="avg_rows",
                    period="day",
                    date_str=d,
                    filters={"product": pname},
                ),
            )
            # сарын дундаж тонн
            q = f"{y} оны {m} сарын {pname}ийн сарын дундаж экспортын хэмжээ хэдэн тонн вэ?"
            add(
                q,
                make_intent(
                    sheet="Экспорт бүтээгдэхүүнээр",
                    metric="qty_month_avg",
                    op="avg_rows",
                    period="month",
                    date_str=f"{y}-{m:02d}-01",
                    filters={"product": pname},
                ),
            )
            # үнээс жигнэсэн дундаж (Үнэ USD/тонн)
            q = f"{y} оны {m} сард {pname}ийн дундаж экспортын үнэ USD/тонноор хэд байсан бэ?"
            add(
                q,
                make_intent(
                    sheet="Экспорт бүтээгдэхүүнээр",
                    metric="price_usd",
                    op="avg_rows",
                    period="month",
                    date_str=f"{y}-{m:02d}-01",
                    filters={"product": pname},
                ),
            )

# өссөн дүн (cumulative)
for pname, code in products.items():
    for y in [2024, 2025]:
        for m in [3, 9]:
            d = f"{y}-{m:02d}-01"
            q = f"{y} оны {m} сарын байдлаар {pname}ийн экспортын сарын өссөн хэмжээ хэдэн тонн болсон бэ?"
            add(
                q,
                make_intent(
                    sheet="Экспорт бүтээгдэхүүнээр",
                    metric="qty_cum",
                    op="value",
                    period="month",
                    date_str=d,
                    filters={"product": pname},
                ),
            )

# ------------------------
# 4. Импорт бүтээгдэхүүнээр (segment-үүдээр)
# ------------------------

import_segments = [
    "нийт импорт",
    "хүнсний бүтээгдэхүүн",
    "нефтийн бүтээгдэхүүн",
    "автомашин, машин техник",
    "бусад",
]

for seg in import_segments:
    for y in [2023, 2024, 2025]:
        for m in [1, 3, 6, 9, 12]:
            d = f"{y}-{m:02d}-01"
            q = f"{y} оны {m} сарын {seg}-ийн импортын дүн USD-ээр хэд вэ?"
            add(
                q,
                make_intent(
                    sheet="Импорт бүтээгдэхүүнээр",
                    metric="value_usd",
                    op="value",
                    period="month",
                    date_str=d,
                    filters={"product": seg},
                ),
            )

# тухайн өдрийн импорт
for seg in import_segments:
    for y in [2024, 2025]:
        for m in [2, 5, 8, 11]:
            d = f"{y}-{m:02d}-15"
            q = f"{d.replace('-', ' оны ').replace(' ', ' ')}-нд {seg}-ийн импортын дүн USD-ээр хэд байсан бэ?"
            add(
                q,
                make_intent(
                    sheet="Импорт бүтээгдэхүүнээр",
                    metric="value_today_usd",
                    op="value",
                    period="day",
                    date_str=d,
                    filters={"product": seg},
                ),
            )

# 7 өдрийн дундаж / сарын дундаж импорт
for seg in import_segments:
    for y in [2023, 2024]:
        for m in [3, 6, 9, 12]:
            d_mid = f"{y}-{m:02d}-15"
            d_month = f"{y}-{m:02d}-01"

            q = f"{y} оны {m} сарын {seg}-ийн 7 өдрийн дундаж импортын дүн хэд вэ?"
            add(
                q,
                make_intent(
                    sheet="Импорт бүтээгдэхүүнээр",
                    metric="value_7d_avg",
                    op="avg_rows",
                    period="day",
                    date_str=d_mid,
                    filters={"product": seg},
                ),
            )

            q = f"{y} оны {m} сарын {seg}-ийн сарын дундаж импортын дүн хэд вэ?"
            add(
                q,
                make_intent(
                    sheet="Импорт бүтээгдэхүүнээр",
                    metric="value_month_avg",
                    op="avg_rows",
                    period="month",
                    date_str=d_month,
                    filters={"product": seg},
                ),
            )

# ------------------------
# 5. Уул уурхайн биржийн арилжаа (тонн, үнэ, үнийн дүн)
# ------------------------

for y in [2023, 2024, 2025]:
    for m in [1, 3, 6, 9, 12]:
        d = f"{y}-{m:02d}-01"
        # сарын нийт тонн
        q = f"{y} оны {m} сард уул уурхайн бирж дээр нийт хэдэн тонн бүтээгдэхүүн арилжаалагдсан бэ?"
        add(
            q,
            make_intent(
                sheet="Уул уурхайн биржийн арилжаа",
                metric="qty_ton",
                op="value",
                period="month",
                date_str=d,
            ),
        )
        # сарын нийт үнийн дүн USD
        q = f"{y} оны {m} сард уул уурхайн биржийн нийт хэлцлийн үнийн дүн USD-ээр хэд байсан бэ?"
        add(
            q,
            make_intent(
                sheet="Уул уурхайн биржийн арилжаа",
                metric="deal_value_usd",
                op="value",
                period="month",
                date_str=d,
            ),
        )
        # дундаж хэлцлийн үнэ USD/тонн
        q = f"{y} оны {m} сард уул уурхайн биржийн дундаж хэлцлийн үнэ USD/тонноор хэд байсан бэ?"
        add(
            q,
            make_intent(
                sheet="Уул уурхайн биржийн арилжаа",
                metric="deal_price_usd",
                op="avg_rows",
                period="month",
                date_str=d,
            ),
        )

# YoY жишээ бирж
for y in [2024, 2025]:
    for m in [3, 9]:
        d = f"{y}-{m:02d}-01"
        q = f"{y} оны {m} сард уул уурхайн биржийн нийт үнийн дүн өмнөх оны мөн үеэс хэдэн хувь өөрчлөгдсөн бэ?"
        add(
            q,
            make_intent(
                sheet="Уул уурхайн биржийн арилжаа",
                metric="value_usd",
                op="yoy",
                period="month",
                date_str=d,
            ),
        )

# ------------------------
# QA
# ------------------------

print(f"Generated examples: {len(examples)} (target >= 250)")

with open(INTENT_EXAMPLES_PATH, "w", encoding="utf-8") as f:
    json.dump(examples, f, ensure_ascii=False, indent=2)

print(f"Saved to {INTENT_EXAMPLES_PATH}")