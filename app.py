import os
import io
import re
import pandas as pd
from flask import Flask, render_template, request, send_file
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

# ------------------------------------------------
# Column detection
# ------------------------------------------------
def detect_columns(df):
    name_col   = find_col(df, ["نام", "Name"]) or "نام"
    gender_col = find_col(df, ["جنس", "Gender"]) or "جنسیت"
    mil_col    = find_col(df, ["نظام", "خدمت", "Military"]) or "وضعیت نظام وظیفه"
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
    s = normalize(x)
    m = re.search(r"\d+", s)
    return int(m.group()) if m else None

def extract_first_float(x):
    s = normalize(x)
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

def build_prompt(header_cols, df_csv, job_description, top_n):
    header_line = ",".join(header_cols + ["دلیل انتخاب"])
    return f"""
شما یک متخصص منابع انسانی هستید.
شرح شغل:
{job_description}

لیست کاندیداها در فایل زیر آمده است.
فقط {top_n} نفر برتر را انتخاب کن و در ستون «دلیل انتخاب» برای هر نفر یک پاراگراف حدود ۵۰ کلمه بنویس که توضیح دهد چرا این شخص در میان سایرین انتخاب شده است (براساس مهارت‌ها، تجربه، شهر، تحصیلات و ارتباط با نیاز شغل). خروجی فقط CSV باشد.

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

def query_gemma(df, job_description, top_n):
    if df.empty:
        return df
    df_trimmed, cols_kept = select_relevant_columns(df, detect_columns(df))
    df_trimmed = cap_rows_for_ai(df_trimmed, job_description)
    csv_in = df_trimmed.to_csv(index=False)
    prompt = build_prompt(cols_kept, csv_in, job_description, top_n)

    try:
        resp = ollama.chat(model="gemma3:1b", messages=[{"role": "user", "content": prompt}])
        text = resp["message"]["content"]
        out = try_parse_csv(text, expected_first_col=cols_kept[0])
        if "دلیل انتخاب" not in out.columns:
            out["دلیل انتخاب"] = [f"این فرد بر اساس ارزیابی مدل gemma3 برای نقش مورد نظر مناسب تشخیص داده شد." for _ in out.index]
        return out.dropna(how="all").replace("nan", "").reset_index(drop=True).head(top_n)
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
        filtered_df = apply_local_filters(cleaned_df, cols, age_range, exp_range, city, gender_filter, military_filter)
        ranked = query_gemma(filtered_df, job_description, top_n)
        top_candidates = ranked.to_dict(orient="records")

        summary_data = {
            "توضیح شغل": job_description,
            "رده سنی": age_range,
            "سابقه کار": exp_range,
            "شهر": city,
            "جنسیت": gender_filter,
            "خدمت سربازی": military_filter if gender_filter == "مرد" else "—",
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
