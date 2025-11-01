import os
import io
import re
import json
import pandas as pd
from flask import Flask, render_template, request, send_file, jsonify
import ollama
from difflib import SequenceMatcher
from datetime import datetime
from openpyxl import Workbook

# ------------------------------------------------
# Flask setup
# ------------------------------------------------
app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["OUTPUT_FOLDER"] = "outputs"
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
os.makedirs(app.config["OUTPUT_FOLDER"], exist_ok=True)

# ------------------------------------------------
# Helpers
# ------------------------------------------------
def normalize(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def text_sim(a, b):
    a, b = normalize(a), normalize(b)
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() if a and b else 0

def find_col(df, keys):
    for k in keys:
        for c in df.columns:
            if k in str(c):
                return c
    return None

# Persian/Arabic digits → ASCII
PERSIAN_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
ARABIC_DIGITS  = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

def normalize_digits(s):
    s = str(s)
    return s.translate(PERSIAN_DIGITS).translate(ARABIC_DIGITS)

# ------------------------------------------------
# Column detection
# ------------------------------------------------
def detect_columns(df):
    name_col   = find_col(df, ["نام", "Name"]) or "نام"
    gender_col = find_col(df, ["جنس", "Gender"]) or "جنسیت"
    mil_col    = find_col(df, ["נظام", "نظام", "خدمت", "Military"]) or "وضعیت نظام وظیفه"
    age_col    = find_col(df, ["سن", "Age"]) or "سن"
    exp_col    = find_col(df, ["سابقه", "Experience"]) or "سابقه کار"
    city_col   = find_col(df, ["شهر", "City", "Location"]) or "شهر"

    for col in [name_col, gender_col, mil_col, age_col, exp_col, city_col]:
        if col not in df.columns:
            df[col] = "نامشخص"

    return {
        "name": name_col,
        "gender": gender_col,
        "military": mil_col,
        "age": age_col,
        "exp": exp_col,
        "city": city_col,
    }

# ------------------------------------------------
# Data cleaning
# ------------------------------------------------
def clean_dataframe(df):
    drop_cols = [c for c in df.columns if ("خلاصه" in c) or ("summary" in str(c).lower())]
    df = df.drop(columns=drop_cols, errors="ignore").copy()
    cols = detect_columns(df)
    return df, cols

# ------------------------------------------------
# Local filters
# ------------------------------------------------
def extract_first_int(x):
    s = normalize_digits(normalize(x))
    m = re.search(r"\d+", s)
    return int(m.group()) if m else None

def extract_first_float(x):
    s = normalize_digits(normalize(x))
    m = re.search(r"(\d+(\.\d+)?)", s)
    return float(m.group(1)) if m else None

def apply_local_filters(df, cols, age_range, exp_range, city, gender_filter, military_filter):
    df = df.copy()
    if gender_filter != "همه":
        df = df[df[cols["gender"]] == gender_filter]
    if gender_filter == "مرد" and military_filter != "همه":
        df = df[df[cols["military"]].astype(str).str.strip() == military_filter]

    if age_range not in ["any", "همه"]:
        bounds = {"18-25": (18, 25), "25-32": (25, 32),
                  "32-40": (32, 40), "40+": (40, 200)}
        lo, hi = bounds.get(age_range, (0, 200))
        df["_AGE_"] = df[cols["age"]].apply(extract_first_int)
        # pandas versions differ on 'inclusive'; this keeps your prior behavior
        df = df[df["_AGE_"].between(lo, hi, inclusive="both")]
        df.drop(columns=["_AGE_"], inplace=True, errors="ignore")

    if exp_range not in ["any", "همه"]:
        e_bounds = {"-1": (0, 1), "1-3": (1, 3), "3-6": (3, 6),
                    "6-10": (6, 10), "10-20": (10, 20), "20+": (20, 200)}
        lo, hi = e_bounds.get(exp_range, (0, 200))
        df["_EXP_"] = df[cols["exp"]].apply(extract_first_float)
        df = df[df["_EXP_"].between(lo, hi, inclusive="both")]
        df.drop(columns=["_EXP_"], inplace=True, errors="ignore")

    if city not in ["any", "همه"]:
        df = df[df[cols["city"]].astype(str).str.contains(str(city), case=False, na=False)]

    return df

# ------------------------------------------------
# Keyword prefilter (NEW)
# ------------------------------------------------
def parse_keywords(s):
    s = normalize(s)
    if not s:
        return []
    # split by comma (English/Persian) or multiple spaces
    parts = re.split(r"[,\u060C]+|\s{2,}", s)
    out = []
    for p in parts:
        out.extend([x.strip() for x in p.split() if x.strip()])
    # de-dup while preserving order
    seen = set()
    dedup = []
    for w in out:
        lw = w.lower()
        if lw not in seen:
            seen.add(lw)
            dedup.append(w)
    return dedup

def get_textual_columns(df):
    keys = ["عنوان", "title", "position", "role",
            "مهارت", "skill", "skills",
            "شرح", "description", "responsibilit", "توضیح", "summary"]
    cols = []
    for c in df.columns:
        cl = str(c).lower()
        if any(k in cl for k in keys):
            cols.append(c)
    return cols or list(df.columns)

def keyword_hits_in_text(text, keywords):
    t = normalize(text).lower()
    return sum(1 for kw in keywords if kw.lower() in t) if t and keywords else 0

def filter_by_keywords(df, keywords, text_cols, min_mode="auto"):
    """
    Keep rows that reach a minimum keyword hit threshold.
    - 'any': at least 1 hit
    - 'all': require all keywords
    - 'auto': if <3 keywords → 1 hit; else ceil(60% of keywords)
    Adds __kw_hits column with total hits.
    """
    df = df.copy()
    if not keywords:
        df["__kw_hits"] = 0
        return df

    totals = []
    for _, r in df.iterrows():
        total = 0
        for c in text_cols:
            total += keyword_hits_in_text(r.get(c, ""), keywords)
        totals.append(total)
    df["__kw_hits"] = totals

    if min_mode == "all" and keywords:
        threshold = len(keywords)
    elif min_mode == "any" or len(keywords) < 3:
        threshold = 1
    else:
        threshold = max(1, int((len(keywords) * 0.6 + 0.999)))  # ceil

    return df[df["__kw_hits"] >= threshold]

# ------------------------------------------------
# AI logic
# ------------------------------------------------
def select_relevant_columns(df, cols):
    preferred = []
    for key_list in [
        [cols["name"]],
        [find_col(df, ["عنوان", "title", "Title", "Position"]) or ""],
        [find_col(df, ["مهارت", "Skills", "skills"]) or ""],
        [cols["age"]], [cols["exp"]], [cols["city"]],
        [cols["gender"]], [cols["military"]],
        ["__kw_hits"] if "__kw_hits" in df.columns else []
    ]:
        for c in key_list:
            if c and c in df.columns and c not in preferred:
                preferred.append(c)

    desc_cols = [c for c in df.columns if any(k in str(c) for k in ["شرح", "responsibilit", "description", "توضیح"])]
    for c in desc_cols:
        if c not in preferred:
            preferred.append(c)
    return df[preferred].copy(), preferred or [cols["name"]]

def cap_rows_for_ai(df, job_description, max_rows=300):
    if len(df) <= max_rows:
        return df
    text_cols = [c for c in df.columns if any(k in str(c).lower() for k in
                  ["عنوان", "title", "skill", "مهارت", "شرح", "description", "responsibilit", "role"])]
    if not text_cols:
        return df.head(max_rows)
    scores = []
    for _, r in df.iterrows():
        s = max(text_sim(job_description, r.get(c, "")) for c in text_cols)
        scores.append(s)
    df["_s"] = scores
    df = df.sort_values("_s", ascending=False).drop(columns="_s")
    return df.head(max_rows)

def build_prompt(header_cols, df_csv, job_description, top_n, prefer_kw=False):
    # Keep column names as-is (including __kw_hits) to avoid CSV parse mismatches
    header_line = ",".join(header_cols + ["دلیل انتخاب"])
    extra = ""
    if prefer_kw and "__kw_hits" in df_csv:
        extra = "\n- اگر ستون __kw_hits وجود دارد، به امتیاز کلیدواژه وزن بده و افراد با امتیاز بالاتر را ترجیح بده."
    return f"""
شما یک متخصص منابع انسانی هستید.
شرح شغل:
{job_description}

لیست کاندیداها در فایل زیر آمده است.
فقط {top_n} نفر برتر را انتخاب کن و در ستون «دلیل انتخاب» برای هر نفر یک پاراگراف حدود ۵۰ کلمه بنویس که توضیح دهد چرا این شخص در میان سایرین انتخاب شده است (براساس مهارت‌ها، تجربه، شهر، تحصیلات و ارتباط با نیاز شغل). خروجی فقط CSV باشد.{extra}

هدر مورد انتظار:
{header_line}

CSV داده‌ها:
{df_csv}
""".strip()

def try_parse_csv(text, expected_first_col):
    lines = [l for l in text.splitlines() if l.strip()]
    try:
        df = pd.read_csv(io.StringIO(text))
        if df.columns[0].strip() == expected_first_col:
            return df
    except Exception:
        pass
    for i, l in enumerate(lines):
        if expected_first_col in l.split(",")[0]:
            block = "\n".join(lines[i:])
            try:
                df = pd.read_csv(io.StringIO(block))
                if df.columns[0].strip() == expected_first_col:
                    return df
            except Exception:
                pass
    raise ValueError("CSV parse failed")

def fallback_rank(df, job_description, top_n):
    if df.empty:
        return df
    text_cols = [c for c in df.columns if any(k in str(c).lower() for k in
        ["عنوان","شرح","مهارت","position","title","skills","description","role","مسئولیت"])]
    df["__score"] = [max(text_sim(job_description, r.get(c,"")) for c in text_cols)
                     for _, r in df.iterrows()] if text_cols else [0]*len(df)
    scores = df["__score"].tolist()
    out = df.sort_values("__score", ascending=False).drop(columns="__score").head(top_n)
    out["دلیل انتخاب"] = [
        f"این فرد به دلیل تطابق بالا با نیاز شغل، تجربه مرتبط و مهارت‌های کلیدی انتخاب شده است. میزان شباهت متنی با شرح شغل حدود {round(scores[i]*100,1)}٪ است."
        for i in range(len(out))
    ]
    return out.dropna(how="all").reset_index(drop=True)

def query_gemma(df, job_description, top_n, prefer_kw=False):
    if df.empty:
        return df
    df_trimmed, cols_kept = select_relevant_columns(df, detect_columns(df))
    df_trimmed = cap_rows_for_ai(df_trimmed, job_description)
    csv_in = df_trimmed.to_csv(index=False)
    prompt = build_prompt(cols_kept, csv_in, job_description, top_n, prefer_kw=prefer_kw)

    try:
        resp = ollama.chat(
            model="gemma3:1b",
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.2}
        )
        text = resp["message"]["content"]
        out = try_parse_csv(text, expected_first_col=cols_kept[0])
        if "دلیل انتخاب" not in out.columns:
            out["دلیل انتخاب"] = [
                f"این فرد بر اساس ارزیابی مدل gemma3 برای نقش مورد نظر مناسب تشخیص داده شد."
                for _ in out.index
            ]
        return (
            out.dropna(how="all")
               .replace("nan", "")
               .reset_index(drop=True)
               .head(top_n)
        )
    except Exception as e:
        print("⚠️ LLM error:", e)
        return fallback_rank(df_trimmed, job_description, top_n)

# ------------------------------------------------
# Excel export
# ------------------------------------------------
def save_with_summary(df, summary_dict, output_path):
    wb = Workbook()
    ws = wb.active
    ws.title = "نتایج رتبه‌بندی"
    ws.append(["مشخصات درخواست"])
    for k, v in summary_dict.items():
        ws.append([f"{k}: {v}"])
    ws.append([])
    if not df.empty:
        ws.append(list(df.columns))
        for row in df.itertuples(index=False):
            ws.append(list(row))
    else:
        ws.append(["هیچ کاندیدایی مطابق فیلترها یافت نشد."])
    wb.save(output_path)

# ------------------------------------------------
# Voice → NLP parsing helpers & route (NEW)
# ------------------------------------------------
def map_age_bucket(lo, hi):
    if lo is None and hi is None:
        return "any"
    lo = lo or 0
    hi = hi or 200
    mid = (lo + hi) / 2
    if 18 <= lo <= 25 and hi <= 25: return "18-25"
    if 25 <= lo <= 32 and hi <= 32: return "25-32"
    if 32 <= lo <= 40 and hi <= 40: return "32-40"
    if hi >= 40: return "40+"
    if mid < 25: return "18-25"
    if mid < 32: return "25-32"
    if mid < 40: return "32-40"
    return "40+"

def map_exp_bucket(lo, hi):
    if lo is None and hi is None:
        return "any"
    lo = lo or 0
    hi = hi or 200
    ranges = [("-1",0,1), ("1-3",1,3), ("3-6",3,6), ("6-10",6,10), ("10-20",10,20), ("20+",20,200)]
    for key,a,b in ranges:
        if lo >= a and hi <= b:
            return key
    mid = (lo + hi) / 2
    if mid < 1: return "-1"
    if mid < 3: return "1-3"
    if mid < 6: return "3-6"
    if mid < 10: return "6-10"
    if mid < 20: return "10-20"
    return "20+"

def extract_json_block(text):
    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m:
        raise ValueError("No JSON block found")
    return json.loads(m.group(0))

@app.route("/nlp/parse", methods=["POST"])
def nlp_parse():
    data = request.get_json(silent=True) or {}
    utter = normalize_digits((data.get("utterance") or "").strip())
    if not utter:
        return jsonify({"error": "empty utterance"}), 400

    sys_hint = """
شما یک سیستم استخراج اطلاعات استخدامی هستید. متن گفت‌وگوی کارفرما را به فیلدهای ساختاری تبدیل کن.
فقط JSON معتبر برگردان. کلیدها:
- job_description: string
- age_range: one of ["any","18-25","25-32","32-40","40+"]
- exp_range: one of ["any","-1","1-3","3-6","6-10","10-20","20+"]
- city: string or "any"
- gender_filter: one of ["همه","مرد","زن"]
- military_filter: one of ["همه","دارد","ندارد"]
- must_keywords: array of strings (optional)
- top_candidates: number (optional)
اگر چیزی مشخص نبود، مقدار مناسب "any" یا "همه" قرار بده.
فقط JSON خالص چاپ کن.
""".strip()

    user_text = f"متن کارفرما:\n{utter}"

    parsed = None
    # Try LLM first
    try:
        resp = ollama.chat(
            model="gemma3:1b",
            messages=[
                {"role": "system", "content": sys_hint},
                {"role": "user", "content": user_text}
            ],
            options={"temperature": 0.2}
        )
        txt = resp["message"]["content"]
        obj = extract_json_block(txt)
        parsed = {
            "job_description": obj.get("job_description") or utter,
            "age_range": obj.get("age_range","any"),
            "exp_range": obj.get("exp_range","any"),
            "city": obj.get("city","any"),
            "gender_filter": obj.get("gender_filter","همه"),
            "military_filter": obj.get("military_filter","همه"),
            "must_keywords": obj.get("must_keywords") or [],
            "top_candidates": obj.get("top_candidates") or None
        }
    except Exception:
        # Fallback regex parser
        s = utter

        age_lo = age_hi = None
        m = re.search(r"سن\s*(?:بین)?\s*(\d{1,3})\s*(?:تا|-)\s*(\d{1,3})", s)
        if m:
            age_lo, age_hi = int(m.group(1)), int(m.group(2))

        exp_lo = exp_hi = None
        m = re.search(r"سابقه(?:\s*کاری)?\s*(\d{1,2})\s*(?:تا|-)\s*(\d{1,2})", s)
        if m:
            exp_lo, exp_hi = float(m.group(1)), float(m.group(2))

        city = "any"
        m = re.search(r"(?:در|توی)\s+([آ-یA-Za-z]+)", s)
        if m:
            city = m.group(1)

        gender = "همه"
        if "خانم" in s or "زن" in s: gender = "زن"
        if "آقا" in s or "مرد" in s: gender = "مرد"

        military = "همه"
        if "معاف" in s or "ندارد" in s: military = "ندارد"
        if "پایان خدمت" in s or "کارت" in s or "دارد" in s: military = "دارد"
        if "فرقی نمی‌کنه" in s or "فرقی نمیکنه" in s: military = "همه"

        parsed = {
            "job_description": utter,
            "age_range": map_age_bucket(age_lo, age_hi),
            "exp_range": map_exp_bucket(exp_lo, exp_hi),
            "city": city,
            "gender_filter": gender,
            "military_filter": military,
            "must_keywords": [],
            "top_candidates": None
        }

    # guardrails
    allowed_age = {"any","18-25","25-32","32-40","40+"}
    allowed_exp = {"any","-1","1-3","3-6","6-10","10-20","20+"}
    allowed_gender = {"همه","مرد","زن"}
    allowed_mil = {"همه","دارد","ندارد"}

    if parsed["age_range"] not in allowed_age: parsed["age_range"]="any"
    if parsed["exp_range"] not in allowed_exp: parsed["exp_range"]="any"
    if parsed["gender_filter"] not in allowed_gender: parsed["gender_filter"]="همه"
    if parsed["military_filter"] not in allowed_mil: parsed["military_filter"]="همه"

    return jsonify(parsed), 200

# ------------------------------------------------
# Flask routes
# ------------------------------------------------
@app.route("/", methods=["GET", "POST"])
def index():
    top_candidates, output_filename, summary_data = None, None, None
    if request.method == "POST":
        job_description = request.form.get("job_description", "").strip()
        age_range = request.form.get("age_range", "همه")
        exp_range = request.form.get("exp_range", "همه")
        city = request.form.get("city", "همه")
        gender_filter = request.form.get("gender_filter", "همه")
        military_filter = request.form.get("military_filter", "همه")
        must_keywords_raw = request.form.get("must_keywords", "")
        must_keywords = parse_keywords(must_keywords_raw)
        try:
            top_n = int(request.form.get("top_candidates", 10))
        except:
            top_n = 10

        file = request.files.get("excel_file")
        if not file:
            return render_template("index.html")

        path = os.path.join(app.config["UPLOAD_FOLDER"], file.filename)
        file.save(path)
        raw_df = pd.read_excel(path, engine="openpyxl")
        cleaned_df, cols = clean_dataframe(raw_df)

        # Step 1: deterministic filters
        filtered_df = apply_local_filters(cleaned_df, cols, age_range, exp_range, city, gender_filter, military_filter)

        # Step 2: keyword prefilter (if any keywords provided)
        prefer_kw = False
        if must_keywords:
            text_cols = get_textual_columns(filtered_df)
            filtered_df = filter_by_keywords(filtered_df, must_keywords, text_cols, min_mode="auto")
            prefer_kw = True  # nudge LLM to consider __kw_hits

        # Step 3: Gemma ranking
        ranked = query_gemma(filtered_df, job_description, top_n, prefer_kw=prefer_kw)
        top_candidates = ranked.to_dict(orient="records")

        # UI/Excel summary
        summary_data = {
            "توضیح شغل": job_description,
            "رده سنی": age_range,
            "سابقه کار": exp_range,
            "شهر": city,
            "جنسیت": gender_filter,
            "خدمت سربازی": military_filter if gender_filter == "مرد" else "—",
            "کلمات کلیدی ضروری": ", ".join(must_keywords) if must_keywords else "—",
            "تعداد نفرات برتر": top_n
        }

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"top_candidates_{ts}.xlsx"
        output_path = os.path.join(app.config["OUTPUT_FOLDER"], output_filename)
        save_with_summary(ranked, summary_data, output_path)

    return render_template("index.html",
                           top_candidates=top_candidates,
                           output_filename=output_filename,
                           summary_data=summary_data)

@app.route("/download/<path:filename>")
def download(filename):
    path = os.path.join(app.config["OUTPUT_FOLDER"], filename)
    return send_file(path, as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True)
