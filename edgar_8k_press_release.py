
# edgar_8k_press_release_v2.py
# ------------------------------------------------------------
# Enhanced: progress bar (tqdm), resume, batching, and skip-downloaded logic.
# Usage examples:
#   python edgar_8k_press_release_v2.py \

#     --universe sp500_constituents.csv --ticker_col Symbol \

#     --start 2019-01-01 --end 2025-12-31 \

#     --out_dir edgar_docs --out_csv press_releases_sp500.csv \

#     --batch_start 0 --batch_size 100

#
#   python edgar_8k_press_release_v2.py --resume_from press_releases_sp500.csv ...

import os, re, time, argparse, json, sys
from datetime import datetime
from typing import Optional, Dict, Any, List
import pandas as pd
import requests
from tqdm import tqdm 

SEC_HEADERS = {
    # IMPORTANT: Replace with your real contact per SEC policy:
    # e.g., "Sally NYU sally@nyu.edu"
    "User-Agent": "SallyYang NYU cy2753@nyu.edu",
    "Accept-Encoding": "gzip, deflate",
}

SLEEP_BETWEEN_REQUESTS = 0.20  # seconds; SEC suggests <=10 req/s. Adjust at your own risk.

def _norm_cik(cik) -> str:
    s = re.sub(r"\D", "", str(cik))
    return s.zfill(10) if s else ""

def _load_universe(path: str, ticker_col: Optional[str], cik_col: Optional[str]) -> pd.DataFrame:
    df = pd.read_csv(path)
    if ticker_col and ticker_col in df.columns:
        df["TICKER"] = df[ticker_col].astype(str).str.strip().str.upper()
    if cik_col and cik_col in df.columns:
        df["CIK"] = df[cik_col].astype(str).apply(_norm_cik)
    if "CIK" not in df.columns:
        df["CIK"] = ""
    if "TICKER" not in df.columns:
        df["TICKER"] = ""
    df = df.loc[(df["CIK"]!="") | (df["TICKER"]!="")].copy()
    return df[["TICKER", "CIK"]].drop_duplicates().reset_index(drop=True)

def _ticker_to_cik_map() -> dict:
    url = "https://www.sec.gov/files/company_tickers.json"
    r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    m = {}
    for _, v in data.items():
        m[str(v.get("ticker","")).upper()] = str(v.get("cik_str","")).zfill(10)
    return m

def _ensure_cik(df: pd.DataFrame) -> pd.DataFrame:
    need = df["CIK"] == ""
    if need.any():
        mapping = _ticker_to_cik_map()
        for idx in df[need].index:
            t = df.at[idx, "TICKER"]
            cik = mapping.get(t.upper(), "")
            df.at[idx, "CIK"] = cik
            time.sleep(SLEEP_BETWEEN_REQUESTS)
    return df[df["CIK"]!=""].copy()

def _company_submissions(cik: str) -> Dict[str, Any]:
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def _find_item_202(items: List[str]) -> bool:
    for it in items:
        if re.search(r"(^|\b)2\.02(\b|$)", it):
            return True
        if re.search(r"item\s*2\.02", it, flags=re.I):
            return True
    return False

def _download(url: str, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    # Skip if already exists (resume)
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return
    r = requests.get(url, headers=SEC_HEADERS, timeout=60)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)

def crawl_8k_item202_for_cik(cik: str, start: str, end: str, out_dir: str, skip_dupes: set) -> List[dict]:
    data = _company_submissions(cik)
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    filingDates = recent.get("filingDate", [])
    primaryDocs = recent.get("primaryDocument", [])
    items_list = recent.get("items", [])

    out_rows = []
    for i, form in enumerate(forms):
        if form != "8-K":
            continue
        fdate = filingDates[i]
        if fdate < start or fdate > end:
            continue
        items_str = items_list[i] if i < len(items_list) else ""
        items = [s.strip() for s in items_str.split("|") if s.strip()]
        if not _find_item_202(items):
            continue
        accession = accessions[i]
        key = f"{cik}:{accession}"
        if key in skip_dupes:
            continue  # already recorded
        primary = primaryDocs[i]
        acc_nodashes = accession.replace("-", "")
        base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_nodashes}"

        # list files
        idx_url = f"{base}/index.json"
        r = requests.get(idx_url, headers=SEC_HEADERS, timeout=30)
        if r.status_code != 200:
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            continue
        files_json = r.json()

        # find exhibit 99.*
        exhibit_name = None
        for f in files_json.get("directory", {}).get("item", []):
            name = f.get("name","").lower()
            desc = (f.get("type","") + " " + f.get("name","")).lower()
            if ("ex99" in name) or ("exhibit99" in name) or re.search(r"ex-?99", name):
                exhibit_name = f.get("name")
                break
            if re.search(r"exhibit\s*99", desc, flags=re.I):
                exhibit_name = f.get("name")
                break

        main_url = f"{base}/{primary}"
        exhibit_url = f"{base}/{exhibit_name}" if exhibit_name else ""

        local_root = os.path.join(out_dir, cik, acc_nodashes)
        main_local = os.path.join(local_root, os.path.basename(primary))
        _download(main_url, main_local)
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        exhibit_local = ""
        if exhibit_url:
            exhibit_local = os.path.join(local_root, os.path.basename(exhibit_name))
            try:
                _download(exhibit_url, exhibit_local)
            except Exception:
                pass

        out_rows.append({
            "cik": cik,
            "accession": accession,
            "filing_date": fdate,
            "doc_url": main_url,
            "exhibit99_url": exhibit_url,
            "main_local": main_local,
            "exhibit99_local": exhibit_local,
        })
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    return out_rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--ticker_col", default=None)
    ap.add_argument("--cik_col", default=None)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out_dir", default="edgar_docs")
    ap.add_argument("--out_csv", default="press_releases.csv")
    ap.add_argument("--resume_from", default=None, help="existing CSV to append/resume from (deduplicate by cik+accession)")
    ap.add_argument("--batch_start", type=int, default=0, help="start index in universe")
    ap.add_argument("--batch_size", type=int, default=None, help="limit number of rows to process")
    args = ap.parse_args()

    # Universe
    df = _load_universe(args.universe, args.ticker_col, args.cik_col)
    df = _ensure_cik(df)
    if df.empty:
        print("No CIKs available after mapping. Provide a CIK or a resolvable TICKER.", file=sys.stderr)
        sys.exit(1)

    # Batching
    if args.batch_size is not None:
        df = df.iloc[args.batch_start: args.batch_start + args.batch_size].copy()
    else:
        df = df.iloc[args.batch_start:].copy()

    # Resume set
    skip_dupes = set()
    if args.resume_from and os.path.exists(args.resume_from):
        old = pd.read_csv(args.resume_from)
        if not old.empty and {"cik","accession"}.issubset(old.columns):
            skip_dupes = set((old["cik"].astype(str)+":"+old["accession"].astype(str)).tolist())

    all_rows = []
    for _, r in tqdm(df.iterrows(), total=len(df), desc="CIKs"):
        cik = r["CIK"]
        try:
            rows = crawl_8k_item202_for_cik(cik, args.start, args.end, args.out_dir, skip_dupes)
            all_rows.extend(rows)
        except Exception as e:
            # continue on errors
            pass

    # Write / append
    new_df = pd.DataFrame(all_rows)
    if os.path.exists(args.out_csv):
        base = pd.read_csv(args.out_csv)
        full = pd.concat([base, new_df], ignore_index=True)
    else:
        full = new_df

    # Deduplicate by cik+accession
    if not full.empty:
        full["dupe_key"] = full["cik"].astype(str) + ":" + full["accession"].astype(str)
        full = full.drop_duplicates("dupe_key").drop(columns=["dupe_key"])
        full.to_csv(args.out_csv, index=False)
        print(f"Saved {len(full)} total rows -> {args.out_csv}")
    else:
        print("No 8-K Item 2.02 filings found in range.")

if __name__ == "__main__":
    main()
