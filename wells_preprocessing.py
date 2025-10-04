import os, re, sys, argparse, tempfile, subprocess, logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import pandas as pd
from dotenv import load_dotenv
import mysql.connector as mysql
from datetime import datetime
for name in ("pdfminer", "pdfminer.pdfinterp", "pdfminer.layout", "pdfminer.pdfpage", "pdfminer.cmapdb"):
    logging.getLogger(name).setLevel(logging.ERROR)

load_dotenv()

def db_conn():
    return mysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB", "wells_db"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        autocommit=True,
    )

TABLE = os.getenv("MYSQL_TABLE", "wells")
INSERT_SQL = f"""
INSERT INTO {TABLE}
(operator_company, well_name_number, api_number, job_type, address,
 longitude, latitude, date_stimulated, stimulated_formation,
 top_ft, bottom_ft, stimulation_stages, volume_value, volume_units,
 treatment_type, acid_percent, lbs_proppant,
 max_treatment_pressure_psi, max_treatment_rate_bbls_per_min, details)
VALUES (%s,%s,%s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,%s,%s,
        %s,%s,%s,
        %s,%s,%s)
"""


def ensure_table(conn):
    cur = conn.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
      id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

      operator_company VARCHAR(255),
      well_name_number VARCHAR(255),
      api_number VARCHAR(32),
      job_type VARCHAR(128),
      address TEXT,
      longitude DECIMAL(10,6),
      latitude DECIMAL(9,6),
      date_stimulated VARCHAR(32),
      stimulated_formation VARCHAR(128),
      top_ft DECIMAL(10,2),
      bottom_ft DECIMAL(10,2),
      stimulation_stages INT,
      volume_value DECIMAL(12,2),
      volume_units VARCHAR(16),
      treatment_type VARCHAR(128),
      acid_percent DECIMAL(5,2),
      lbs_proppant DECIMAL(12,2),
      max_treatment_pressure_psi DECIMAL(12,2),
      max_treatment_rate_bbls_per_min DECIMAL(12,2),
      details TEXT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    cur.close()


# OCR
def _have(cmd: str) -> bool:
    from shutil import which
    return which(cmd) is not None


def ocr_pdf_if_needed(src_pdf: Path) -> Path:
    if not _have("ocrmypdf"):
        return src_pdf
    out = Path(tempfile.gettempdir()) / f"ocr_{src_pdf.stem}.pdf"
    try:
        subprocess.run(
            ["ocrmypdf", "--skip-text", "--fast-web-view", "1", "--rotate-pages", "--deskew",
             str(src_pdf), str(out)],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        return out
    except Exception:
        return src_pdf


def extract_text_pages(pdf_path: Path) -> List[str]:
    texts = []
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            for p in pdf.pages:
                try:
                    t = p.extract_text(x_tolerance=2, y_tolerance=2) or ""
                except Exception:
                    t = ""
                texts.append(t)
        if any(texts): return texts
    except Exception:
        pass
    try:
        from PyPDF2 import PdfReader
        r = PdfReader(str(pdf_path))
        for p in r.pages:
            try: texts.append(p.extract_text() or "")
            except Exception: texts.append("")
    except Exception:
        pass
    return texts

STOP_AT = re.compile(
    r"\b(Qtr(?:-?Qtr)?|Quarter(?:-Quarter)?|Section|Township|Range|County|"
    r"Operator|Field|Telephone|API\b|Address|Lat|Lon|Longitude|Latitude|Top|Bottom|Stages?)\b",
    re.I
)

# Labeling patterns
LP = {
    "operator_company": re.compile(r"\b(?:Operator|Well\s*Operator|Operator\/Company)\b\s*[:：-]?\s*", re.I),
    "api_number": re.compile(r"\bAPI\s*(?:No\.?|#)?\s*[:：-]?\s*", re.I),
    "address": re.compile(r"\b(?:Field\s*Address|Address)\b\s*[:：-]?\s*", re.I),
    "date_stimulated": re.compile(r"\b(?:Date\s*(?:Stimulated|of\s*Stimulation)|Treatment\s*Date)\b\s*[:：-]?\s*", re.I),
    "stimulated_formation": re.compile(r"\b(?:Stimulated\s*Formation|Formation)\b\s*[:：-]?\s*", re.I),
    "job_type": re.compile(r"\b(?:Job\s*Type|Treatment\s*Type|Type\s*Treatment)\b\s*[:：-]?\s*", re.I),
    "top_ft": re.compile(r"\bTop(?:\s*\((?:ft|feet)\))?\b\s*[:：-]?\s*", re.I),
    "bottom_ft": re.compile(r"\bBottom(?:\s*\((?:ft|feet)\))?\b\s*[:：-]?\s*", re.I),
    "stimulation_stages": re.compile(r"\b(?:Stimulation\s*)?Stages?\b\s*[:：-]?\s*", re.I),
    "volume": re.compile(r"\b(?:Total\s*)?(?:Fluid|Volume)\b\s*[:：-]?\s*", re.I),
    "acid_percent": re.compile(r"\bAcid\s*%?\b\s*[:：-]?\s*", re.I),
    "lbs_proppant": re.compile(r"\b(?:Lbs?\.?\s*Proppant|Proppant)\b\s*[:：-]?\s*", re.I),
    "max_pressure": re.compile(r"\b(?:Max(?:imum)?\s*)?(?:Treat(?:ment)?\s*)?Pressure(?:\s*\((?:PSI|psi)\))?\b\s*[:：-]?\s*", re.I),
    "max_rate": re.compile(r"\b(?:Max(?:imum)?\s*)?(?:Treat(?:ment)?\s*)?Rate(?:\s*\((?:BBLS?\/Min|BPM)\))?\b\s*[:：-]?\s*", re.I),
}

# WELL labels with CAPTURED value (only everything to the right of the label)
WELL_LABELS = [
    re.compile(r"\bWell\s*Name\s*and\s*Number\s*or\s*Facility\s*Name\b\s*[:：-]?\s*(?P<val>.+)$", re.I),
    re.compile(r"\bWell\s*or\s*Facility\s*Name\b\s*[:：-]?\s*(?P<val>.+)$", re.I),
    re.compile(r"\bWell\s*Name\s*and\s*Number\b\s*[:：-]?\s*(?P<val>.+)$", re.I),
    re.compile(r"\bWell\s*(?:Name\s*)?(?:and|&|/)\s*(?:Number|No\.?)\b\s*[:：-]?\s*(?P<val>.+)$", re.I),
]

RGX = {
    "lat_dec": re.compile(r"\bLat(?:itude)?\s*[:\-]?\s*([\-+]?\d{1,2}\.\d+)\b", re.I),
    "lon_dec": re.compile(r"\bLon(?:gitude)?\s*[:\-]?\s*([\-+]?\d{1,3}\.\d+)\b", re.I),
    "date": re.compile(r"\b([0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4})\b"),
    "acid_inline": re.compile(r"\b(\d{1,3}(?:\.\d+)?)\s*%\s*acid\b", re.I),
    "vol_inline": re.compile(r"\b([0-9,\.]+)\s*(bbls?|barrels?|gal(?:lons)?)\b", re.I),
    "rate_inline": re.compile(r"\b([0-9,\.]+)\s*(?:BBLS?\/Min|BPM)\b", re.I),
    "press_inline": re.compile(r"\b([0-9,\.]+)\s*psi\b", re.I),
    "stages_inline": re.compile(r"\bstages?\b\s*[:\-]?\s*([0-9]{1,3})", re.I),
}

ADDRESS_LIKE = re.compile(r"\b[A-Za-z][A-Za-z .'\-]+,\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?\b")
STREET_HINT  = re.compile(r"\b(P\.?O\.?\s*Box|PO Box|Suite|Ste\.?|Apt\.?|Ave|Avenue|St\.?|Street|Rd\.?|Road|Dr\.?|Drive|Blvd\.?|Boulevard|Ln\.?|Lane|Hwy|Highway)\b", re.I)

# Markers that indicate we've moved past the well name on the same line
TRIM_AFTER = re.compile(
    r"(?:"
    r"\bQtr(?:-?Qtr)?\b|"           # Qtr or Qtr-Qtr
    r"\bQuarter(?:-Quarter)?\b|"
    r"\bSec(?:tion)?\b|"            # Sec or Section
    r"\bTownship\b|"
    r"\bRange\b|"
    r"\bField\b|"
    r"\bPool\b|"
    r"\bCounty\b|"
    r"\bFootages?\b|"
    r"\bBefore\b|"                  # handles "Before/After" headers
    r"\bAfter\b|"
    r"\bT\s*\d{1,3}\s*[NS]\b|"      # T 153 N  (with or without spaces)
    r"\bT\d{1,3}[NS]\b|"
    r"\bR\s*\d{1,3}\s*[EW]\b|"
    r"\bR\d{1,3}[EW]\b"
    r")",
    re.I
)

def cut_after_markers(s: str) -> str:
    if not s:
        return s
    m = TRIM_AFTER.search(s)
    return s[:m.start()].strip(" .:-") if m else s.strip(" .:-")

def page_lines(text: str) -> List[str]:
    t = re.sub(r"\r", "\n", text or "")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{2,}", "\n", t)
    return [ln.strip() for ln in t.split("\n") if ln.strip()]

def trim_spillover(val: str) -> str:
    # Use same-line marker trimming (stronger than STOP_AT).
    return cut_after_markers(val)

def extract_value_after_label(lines: List[str], pats: List[re.Pattern], start_idx: int = 0, max_next: int = 2) -> Tuple[Optional[str], int]:
    for pat in pats:
        i = start_idx
        while i < len(lines):
            ln = lines[i]
            m = pat.search(ln)
            if not m:
                i += 1
                continue
            val = (m.group("val") or "").strip(" :.-")
            if not val:
                look = None
                for j in range(1, max_next+1):
                    if i + j < len(lines):
                        cand = lines[i+j].strip()
                        if STOP_AT.search(cand): break
                        look = cand; break
                val = (look or "").strip(" :.-")
            val = trim_spillover(val)
            return (val or None, i)
    return (None, -1)

def _is_plausible_well_name(s: str) -> bool:
    if not s: return False
    s = s.strip(" :.-").strip()
    if len(s) < 3 or len(s) > 120: return False
    if not re.search(r"[A-Za-z]", s): return False
    if not re.search(r"\d", s): return False
    if ADDRESS_LIKE.search(s): return False
    if STREET_HINT.search(s): return False
    bad_tokens = (
        "production","rate","hour","spacing","unit","description",
        "address","county","city","state","zip","operator","api",
        "field","telephone","section","township","range","qtr",
        "bismarck","otr"
    )
    low = s.lower()
    if any(bt in low for bt in bad_tokens): return False
    return True

def extract_well_name(lines: List[str]) -> Optional[str]:
    idx = 0
    label_again = re.compile(r"well\s*(?:name\s*)?(?:and|&|/)?\s*(?:number|no\.?)|facility\s*name", re.I)
    while True:
        val, where = extract_value_after_label(lines, WELL_LABELS, start_idx=idx, max_next=0)
        if where < 0:
            break
        # Prefer the next few lines BELOW the label
        for j in range(1, 6):
            if where + j >= len(lines): break
            cand = lines[where + j].strip()
            if not cand: continue
            if STOP_AT.search(cand) or label_again.search(cand): break
            if re.search(r"\bsee\b", cand, re.I): continue
            cand = cut_after_markers(cand)              # trim same-line spillover
            if _is_plausible_well_name(cand): 
                return cand
        # Fallback: value to the RIGHT of the label (same line)
        if val and not re.search(r"\bsee\b", val, re.I):
            val = cut_after_markers(val)                # trim same-line spillover
            if _is_plausible_well_name(val):
                return val
        idx = where + 1
    # Final heuristic: early lines containing letters+digits (not address-like)
    label_prefix = re.compile(
        r'^(?:Well\s*Name\s*and\s*Number\s*or\s*Facility\s*Name|'
        r'Well\s*or\s*Facility\s*Name|'
        r'Well\s*Name\s*and\s*Number)\s*[:：-]?\s*', re.I)
    for ln in lines[:80]:
        s = label_prefix.sub("", ln).strip()
        s = cut_after_markers(s)
        if _is_plausible_well_name(s) and re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", s):
            return s
    return None

def extract_value_near_label(lines: List[str], label_pat: re.Pattern, start_idx: int = 0, max_next: int = 2) -> Tuple[Optional[str], int]:
    """Generic helper for other fields (not Well Name)."""
    for i in range(start_idx, len(lines)):
        ln = lines[i]
        m = label_pat.search(ln)
        if not m: 
            continue
        val = ln[m.end():].strip(" :.-")
        if not val:
            for j in range(1, max_next+1):
                if i + j < len(lines):
                    cand = lines[i+j].strip()
                    if STOP_AT.search(cand): break
                    val = cand; break
        val = trim_spillover(val)
        return (val or None, i)
    return (None, -1)

def canonicalize_api(value: Optional[str]) -> Optional[str]:
    if not value: return None
    s = value.strip()
    m = re.search(r'\b(\d{2}\s*-\s*\d{3}\s*-\s*\d{4,5}(?:\s*-\s*\d{2,4})?)\b', s)
    if m:
        digits = re.sub(r'\D+', '', m.group(1))
    else:
        m2 = re.search(r'\b(\d{10,14})\b', s)
        if not m2: return None
        digits = m2.group(1)
    if len(digits) < 10: return None
    base = f"{digits[:2]}-{digits[2:5]}-{digits[5:10]}"
    ext = digits[10:]
    return base if not ext else f"{base}-{ext}"

def only_num(s: Optional[str]) -> Optional[str]:
    if not s: return None
    m = re.search(r"([0-9,\.]+)", s)
    return re.sub(r"[,\s]", "", m.group(1)) if m else None

def num_and_unit(s: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not s: return None, None
    m = re.search(r"([0-9,\.]+)\s*([A-Za-z/]+)?", s)
    if not m: return None, None
    return re.sub(r"[,\s]", "", m.group(1)), (m.group(2) or "").lower() or None

# Dates: normalization for any 'date' column
DATE_PATS = [
    re.compile(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b"),
    re.compile(r"\b(\d{4}[/-]\d{1,2}[/-]\d{1,2})\b"),
    re.compile(
        r"\b((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t|tember)?|Oct(?:ober)?|Nov(?:ember)?|"
        r"Dec(?:ember)?)\s+\d{1,2},?\s+\d{2,4})\b",
        re.I
    ),
]
def normalize_date_token(tok: str) -> str:
    tok = tok.strip().replace("  ", " ")
    fmts = ["%m/%d/%Y","%m/%d/%y","%m-%d-%Y","%m-%d-%y","%Y-%m-%d","%Y/%m/%d",
            "%B %d, %Y","%b %d, %Y","%B %d %Y","%b %d %Y"]
    for f in fmts:
        try:
            dt = datetime.strptime(tok, f)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    return tok
def short_date_from_text(s: Optional[str]) -> Optional[str]:
    if not s: return None
    for pat in DATE_PATS:
        m = pat.search(s)
        if m:
            return normalize_date_token(m.group(1))
    return None
COL_LIMITS = {
    "operator_company": 255,
    "well_name_number": 255,
    "api_number": 32,
    "job_type": 128,
    "date_stimulated": 32,
    "stimulated_formation": 128,
    "volume_units": 16,
    "treatment_type": 128,
}
def clip_len(s, n):
    if s is None: return None
    s = str(s).strip()
    return s[:n]
def normalize_all_date_fields(rec: Dict[str, Optional[str]], doc_text: str) -> None:
    for k, v in list(rec.items()):
        if "date" in k.lower():
            tok = short_date_from_text(v) or short_date_from_text(doc_text)
            rec[k] = tok[:COL_LIMITS.get(k, 32)] if tok else None

def parse_pdf(pdf_path: Path) -> Dict[str, Optional[str]]:
    out = {k: None for k in [
        "operator_company","well_name_number","api_number","job_type","address",
        "longitude","latitude","date_stimulated","stimulated_formation",
        "top_ft","bottom_ft","stimulation_stages","volume_value","volume_units",
        "treatment_type","acid_percent","lbs_proppant",
        "max_treatment_pressure_psi","max_treatment_rate_bbls_per_min","details"
    ]}

    ocrd = ocr_pdf_if_needed(pdf_path)
    pages = extract_text_pages(ocrd)
    all_text = "\n".join(pages)
    lines = page_lines(all_text)

    out["well_name_number"] = extract_well_name(lines)

    for key, pat in {
        "operator_company": LP["operator_company"],
        "address": LP["address"],
        "date_stimulated": LP["date_stimulated"],
        "stimulated_formation": LP["stimulated_formation"],
        "job_type": LP["job_type"],
        "top_ft": LP["top_ft"],
        "bottom_ft": LP["bottom_ft"],
        "stimulation_stages": LP["stimulation_stages"],
        "acid_percent": LP["acid_percent"],
        "lbs_proppant": LP["lbs_proppant"],
    }.items():
        v, _ = extract_value_near_label(lines, pat)
        if v:
            if key in ("top_ft","bottom_ft","stimulation_stages","acid_percent","lbs_proppant"):
                out[key] = only_num(v)
            else:
                out[key] = v

    api_line, _ = extract_value_near_label(lines, LP["api_number"])
    out["api_number"] = canonicalize_api(api_line)

    if not out["volume_value"]:
        vv, uu = num_and_unit(extract_value_near_label(lines, LP["volume"])[0])
        out["volume_value"], out["volume_units"] = vv, uu

    if not out["max_treatment_pressure_psi"]:
        out["max_treatment_pressure_psi"] = only_num(extract_value_near_label(lines, LP["max_pressure"])[0])
    if not out["max_treatment_rate_bbls_per_min"]:
        out["max_treatment_rate_bbls_per_min"] = only_num(extract_value_near_label(lines, LP["max_rate"])[0])

    if not out["latitude"]:
        m = RGX["lat_dec"].search(all_text); out["latitude"] = m.group(1) if m else None
    if not out["longitude"]:
        m = RGX["lon_dec"].search(all_text); out["longitude"] = m.group(1) if m else None
    if not out["date_stimulated"]:
        m = RGX["date"].search(all_text); out["date_stimulated"] = m.group(1) if m else None
    if not out["acid_percent"]:
        m = RGX["acid_inline"].search(all_text); out["acid_percent"] = m.group(1) if m else None
    if not out["volume_value"]:
        m = RGX["vol_inline"].search(all_text)
        if m:
            out["volume_value"] = re.sub(r"[,\s]","",m.group(1))
            out["volume_units"] = (m.group(2) or "").lower()
    if not out["max_treatment_rate_bbls_per_min"]:
        m = RGX["rate_inline"].search(all_text); out["max_treatment_rate_bbls_per_min"] = re.sub(r"[,\s]","",m.group(1)) if m else None
    if not out["max_treatment_pressure_psi"]:
        m = RGX["press_inline"].search(all_text); out["max_treatment_pressure_psi"] = re.sub(r"[,\s]","",m.group(1)) if m else None
    if not out["stimulation_stages"]:
        m = RGX["stages_inline"].search(all_text); out["stimulation_stages"] = m.group(1) if m else None

    details = []
    for ptxt in pages:
        plines = page_lines(ptxt)
        for i, ln in enumerate(plines):
            if re.search(r"(Stimul|Treat|Acidiz|Frac|Hydraulic|Proppant|Stage|Pressure|Rate|Volume)", ln, re.I):
                blk = " ".join(plines[i:i+15])
                details.append(re.sub(r"\s+", " ", blk.strip()))
    if details:
        out["details"] = (" | ".join(dict.fromkeys(details)))[:1200]

    normalize_all_date_fields(out, all_text)

    for k, n in COL_LIMITS.items():
        if out.get(k):
            out[k] = clip_len(out[k], n)

    return out

def to_float(x):
    try: return None if x in (None,"") else float(x)
    except: return None

def to_int(x):
    try: return None if x in (None,"") else int(float(x))
    except: return None

def iter_pdfs(root: Path):
    for p in sorted(root.rglob("*")):
        if p.is_file() and p.suffix.lower() == ".pdf":
            if p.name.startswith("ocr_"): 
                continue
            yield p

def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pdf-path", nargs="+", help="One or more PDF files")
    g.add_argument("--pdf-dir", type=str, help="Directory of PDFs (recursive)")
    ap.add_argument("--out-csv", type=str, help="Optional CSV output path")
    args = ap.parse_args()

    conn = db_conn()
    ensure_table(conn)
    cur = conn.cursor()

    if args.pdf_path:
        files = [Path(p) for p in args.pdf_path]
        for p in files:
            if not p.exists():
                print(f"ERR: file not found: {p}", file=sys.stderr); sys.exit(2)
    else:
        root = Path(args.pdf_dir)
        if not root.exists():
            print(f"ERR: dir not found: {root}", file=sys.stderr); sys.exit(2)
        files = list(iter_pdfs(root))

    rows = []
    inserted = 0
    total = len(files)
    for idx, f in enumerate(files, start=1):
        rec = parse_pdf(f)
        rows.append(rec)
        cur.execute(INSERT_SQL, (
            rec.get("operator_company"),
            rec.get("well_name_number"),
            rec.get("api_number"),
            rec.get("job_type"),
            rec.get("address"),
            to_float(rec.get("longitude")),
            to_float(rec.get("latitude")),
            rec.get("date_stimulated"),
            rec.get("stimulated_formation"),
            to_float(rec.get("top_ft")),
            to_float(rec.get("bottom_ft")),
            to_int(rec.get("stimulation_stages")),
            to_float(rec.get("volume_value")),
            (rec.get("volume_units") or None),
            rec.get("treatment_type"),
            to_float(rec.get("acid_percent")),
            to_float(rec.get("lbs_proppant")),
            to_float(rec.get("max_treatment_pressure_psi")),
            to_float(rec.get("max_treatment_rate_bbls_per_min")),
            rec.get("details"),
        ))
        inserted += 1
        print(f"({idx}/{total}) scanned: {f.name}")

    print(f"Inserted total rows: {inserted}")

    if args.out_csv:
        df = pd.DataFrame(rows)
        cols = ["operator_company","well_name_number","api_number","job_type","address",
                "longitude","latitude","date_stimulated","stimulated_formation",
                "top_ft","bottom_ft","stimulation_stages","volume_value","volume_units",
                "treatment_type","acid_percent","lbs_proppant",
                "max_treatment_pressure_psi","max_treatment_rate_bbls_per_min","details"]
        for c in cols:
            if c not in df.columns: df[c] = None
        df = df[cols]
        df.to_csv(args.out_csv, index=False)
        print(f"Saved CSV: {args.out_csv} ({len(df)} rows)")

    cur.close(); conn.close()

if __name__ == "__main__":
    main()