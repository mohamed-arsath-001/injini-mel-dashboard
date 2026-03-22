
import re
from datetime import datetime
from statistics import median

import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
#  Date Parsing  (robust — handles every known Airtable output format)
# ──────────────────────────────────────────────────────────────────────────────

_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

_DATE_FMTS = [
    "%B %Y",                    # September 2023
    "%b %Y",                    # Sep 2023
    "%Y-%m-%d",                 # 2023-09-01
    "%Y-%m-%dT%H:%M:%S.%fZ",   # 2023-09-01T00:00:00.000Z
    "%Y-%m-%dT%H:%M:%SZ",      # 2023-09-01T00:00:00Z
    "%m/%Y",                    # 09/2023
    "%m-%Y",                    # 09-2023
    "%Y/%m",                    # 2023/09
    "%d/%m/%Y",                 # 01/09/2023
]


def parse_reporting_month(val) -> pd.Timestamp:
    """
    Convert whatever Airtable returns as 'Reporting Month' into a datetime.
    Returns pd.NaT if the value cannot be parsed.
    """
    # Unwrap linked-record arrays
    if isinstance(val, list):
        val = val[0] if val else None

    if val is None:
        return pd.NaT

    # Coerce to string
    if not isinstance(val, str):
        try:
            val = str(val)
        except Exception:
            return pd.NaT

    val = val.strip()
    if not val or val.lower() in ("unknown", "n/a", "-", "none"):
        return pd.NaT

    # Try every explicit format first
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(val, fmt).replace(day=1)
        except ValueError:
            continue

    # Regex fallback: find "MonthWord YYYY" anywhere (e.g. "Report 6 - September 2023")
    m = re.search(r"\b([A-Za-z]+)\b\s+(\d{4})\b", val)
    if m:
        word, year = m.group(1).lower(), m.group(2)
        if word in _MONTH_ABBR:
            return datetime(int(year), _MONTH_ABBR[word], 1)
        # Try full month name
        for fmt in ("%B", "%b"):
            try:
                dt = datetime.strptime(m.group(1), fmt)
                return datetime(int(year), dt.month, 1)
            except ValueError:
                continue

    # Sep-23 style
    m2 = re.match(r"^([A-Za-z]{3})-(\d{2})$", val)
    if m2:
        mon = m2.group(1).lower()
        yr = int(m2.group(2)) + 2000
        if mon in _MONTH_ABBR:
            return datetime(yr, _MONTH_ABBR[mon], 1)

    return pd.NaT


# ──────────────────────────────────────────────────────────────────────────────
#  Tiered Growth Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _tiered_sales_growth(series: pd.Series):
    """
    5-tier sales growth per Indicators Definition.
    Returns float (%) or "Insufficient Data".
    """
    clean = series.dropna()
    n = len(clean)

    if n < 6:
        return "Insufficient Data"

    if n < 12:                          # tier ii: 6-11 months
        first = clean.iloc[:3].mean()
        last  = clean.iloc[-3:].mean()
    elif n < 18:                        # tier iii: 12-17 months
        first = clean.iloc[:6].mean()
        last  = clean.iloc[-6:].mean()
    elif n < 24:                        # tier iv: 18-23 months
        first = clean.iloc[:12].mean()
        residual = clean.iloc[12:]
        last = (residual.sum() / len(residual)) * 12 if len(residual) > 0 else 0
    else:                               # tier v: 24+ months
        first = clean.iloc[:12].mean()
        last  = clean.iloc[-12:].mean()

    if first == 0:
        return 0.0
    return round(((last - first) / first) * 100, 1)


def _tiered_profit_growth(series: pd.Series):
    """
    Same tiers as sales growth but uses ABS(denominator) per spec.
    """
    clean = series.dropna()
    n = len(clean)

    if n < 6:
        return "Insufficient Data"

    if n < 12:
        first = clean.iloc[:3].mean()
        last  = clean.iloc[-3:].mean()
    elif n < 18:
        first = clean.iloc[:6].mean()
        last  = clean.iloc[-6:].mean()
    elif n < 24:
        first = clean.iloc[:12].mean()
        residual = clean.iloc[12:]
        last = (residual.sum() / len(residual)) * 12 if len(residual) > 0 else 0
    else:
        first = clean.iloc[:12].mean()
        last  = clean.iloc[-12:].mean()

    denom = abs(first)
    if denom == 0:
        return 0.0
    return round(((last - first) / denom) * 100, 1)


# ──────────────────────────────────────────────────────────────────────────────
#  Main KPI Calculator
# ──────────────────────────────────────────────────────────────────────────────

def calculate_kpis(df: pd.DataFrame) -> dict:
    """
    Returns the full indicator matrix:
        Program_Overview, Venture_Data, Cohort_Summaries,
        Investment_Ledger, Jobs_Summary, Reach_Summary,
        Time_Series, Red_Flags, Cohort_Detail
    """

    # ── 1. Parse dates ─────────────────────────────────────────────────────────
    df = df.copy()
    df["Date"] = df["Reporting Month"].apply(parse_reporting_month)

    # ── 2. Drop truly unknown / blank businesses ───────────────────────────────
    #       (MUST happen before any groupby so these never appear in output)
    df = df[
        df["Business Name"].notna() &
        (df["Business Name"].str.strip() != "") &
        (df["Business Name"].str.lower() != "unknown")
    ].copy()

    # ── 3. Add per-business Month_Index (months since that business's start) ──
    # NOTE: We use direct iteration instead of groupby.apply() because pandas 2.x
    #       drops the groupby key column from the result when using apply().
    df["Month_Index"] = 0
    for biz_name, biz_idx in df.groupby("Business Name").groups.items():
        sorted_idx = df.loc[biz_idx].sort_values("Date").index
        valid_dates = df.loc[sorted_idx, "Date"].dropna()
        if valid_dates.empty:
            continue
        start = valid_dates.min()
        for idx in sorted_idx:
            d = df.at[idx, "Date"]
            if pd.notna(d):
                df.at[idx, "Month_Index"] = (
                    (d.year - start.year) * 12 + (d.month - start.month) + 1
                )

    # ── 4. Program-level extended time series (Month_Index normalised) ─────────
    valid_global = df[df["Date"].notna()].copy()
    program_series: dict = {}
    if not valid_global.empty:
        prog_monthly = valid_global.groupby("Month_Index").agg({
            "Monthly Sales (R)":            "sum",
            "Monthly Net Profit":           "sum",
            "Total Jobs":                   "sum",
            "Female Jobs":                  "sum",
            "Youth Jobs":                   "sum",
            "Total Subscribers Students":   "sum",
            "Total Subscribers Teachers":   "sum",
            "New Subscribers Students":     "sum",
            "New Subscribers Teachers":     "sum",
            "SA Schools":                   "sum",
            "Q1-3 Schools":                 "sum",
        }).sort_index()

        program_series = {
            "months":                  [int(m) for m in prog_monthly.index.tolist()],
            "sales":                   [round(float(v)) for v in prog_monthly["Monthly Sales (R)"]],
            "profit":                  [round(float(v)) for v in prog_monthly["Monthly Net Profit"]],
            "jobs_total":              [round(float(v)) for v in prog_monthly["Total Jobs"]],
            "jobs_female":             [round(float(v)) for v in prog_monthly["Female Jobs"]],
            "jobs_youth":              [round(float(v)) for v in prog_monthly["Youth Jobs"]],
            "reach_learners":          [round(float(v)) for v in prog_monthly["Total Subscribers Students"]],
            "reach_educators":         [round(float(v)) for v in prog_monthly["Total Subscribers Teachers"]],
            "reach_new_learners_cum":  [round(float(v)) for v in prog_monthly["New Subscribers Students"].cumsum()],
            "reach_new_educators_cum": [round(float(v)) for v in prog_monthly["New Subscribers Teachers"].cumsum()],
            "reach_sa_schools":        [round(float(v)) for v in prog_monthly["SA Schools"]],
            "reach_q13_schools":       [round(float(v)) for v in prog_monthly["Q1-3 Schools"]],
        }

    # ── 5. Per-venture calculations ────────────────────────────────────────────
    businesses = df.groupby(["Cohort", "Business Name"])

    venture_data:      list = []
    all_sales_growth:  list = []
    all_profit_growth: list = []
    total_net_jobs:    int  = 0
    grand_total_sales: float = 0.0

    # Program-wide accumulators
    prog_total_jobs    = prog_new_jobs   = prog_female_jobs = 0
    prog_youth_jobs    = prog_female_new = prog_youth_new   = 0
    prog_total_sub     = prog_new_sub    = prog_total_schools = 0
    prog_female_stu    = prog_total_stu  = prog_rural_stu     = 0
    prog_disability_stu = 0

    all_time_series: list = []
    red_flags:       list = []

    for (cohort, business), grp in businesses:
        grp = grp.sort_values("Date")
        n = len(grp)

        # Sales / profit
        total_sales     = float(grp["Monthly Sales (R)"].sum())
        grand_total_sales += total_sales
        sales_growth    = _tiered_sales_growth(grp["Monthly Sales (R)"])
        profit_growth   = _tiered_profit_growth(grp["Monthly Net Profit"])

        if isinstance(sales_growth, (int, float)):
            all_sales_growth.append(sales_growth)
        if isinstance(profit_growth, (int, float)):
            all_profit_growth.append(profit_growth)

        # Jobs — baseline = first row, current = last row
        first_jobs  = float(grp.iloc[0]["Total Jobs"]) if n > 0 else 0.0
        last_jobs   = float(grp.iloc[-1]["Total Jobs"]) if n > 0 else 0.0
        net_jobs    = int(last_jobs - first_jobs) if n >= 2 else 0
        total_net_jobs += net_jobs

        current_female = float(grp.iloc[-1]["Female Jobs"]) if n > 0 else 0.0
        first_female   = float(grp.iloc[0]["Female Jobs"])  if n > 0 else 0.0
        current_youth  = float(grp.iloc[-1]["Youth Jobs"])  if n > 0 else 0.0
        first_youth    = float(grp.iloc[0]["Youth Jobs"])   if n > 0 else 0.0
        new_female     = int(current_female - first_female) if n >= 2 else 0
        new_youth      = int(current_youth  - first_youth)  if n >= 2 else 0

        # % Change (individual): (current − baseline) / baseline — consistent with cohort/program
        jobs_pct = round(((last_jobs - first_jobs) / first_jobs) * 100, 1) if first_jobs != 0 else 0.0

        prog_total_jobs  += int(last_jobs)
        prog_new_jobs    += net_jobs
        prog_female_jobs += int(current_female)
        prog_youth_jobs  += int(current_youth)
        prog_female_new  += new_female
        prog_youth_new   += new_youth

        # Reach
        latest_stu   = float(grp.iloc[-1]["Total Subscribers Students"]) if n > 0 else 0.0
        latest_tea   = float(grp.iloc[-1]["Total Subscribers Teachers"]) if n > 0 else 0.0
        venture_subs = latest_stu + latest_tea
        venture_new_subs = float(grp["New Subscribers Students"].sum()) + \
                           float(grp["New Subscribers Teachers"].sum())
        venture_schools  = float(grp.iloc[-1]["Total Schools"]) if n > 0 else 0.0
        latest_female_stu = float(grp.iloc[-1]["Female Students"])    if n > 0 else 0.0
        latest_rural_stu  = float(grp.iloc[-1]["Rural Students"])     if n > 0 else 0.0
        latest_dis_stu    = float(grp.iloc[-1]["Disability Students"]) if n > 0 else 0.0

        prog_total_sub     += venture_subs
        prog_new_sub       += venture_new_subs
        prog_total_schools += venture_schools
        prog_female_stu    += latest_female_stu
        prog_total_stu     += latest_stu
        prog_rural_stu     += latest_rural_stu
        prog_disability_stu += latest_dis_stu

        # Time-series rows
        for _, row in grp.iterrows():
            if pd.notna(row["Date"]):
                all_time_series.append({
                    "cohort":   cohort,
                    "business": business,
                    "month":    row["Date"].strftime("%Y-%m"),
                    "sales":    float(row["Monthly Sales (R)"]),
                    "profit":   float(row["Monthly Net Profit"]),
                    "jobs":     float(row["Total Jobs"]),
                })

        # Red flags
        flags = []
        if isinstance(sales_growth, (int, float)) and sales_growth < 0:
            flags.append("Negative Sales Growth")
        if isinstance(profit_growth, (int, float)) and profit_growth < 0:
            flags.append("Negative Profit Growth")
        months_span = n if n > 0 else 1
        if venture_subs > 0 and ((venture_new_subs / months_span) * 12) < 8_000:
            flags.append("Low Learner Reach (<8,000/yr)")
        if flags:
            red_flags.append({"Business Name": business, "Cohort": cohort, "Flags": flags})

        venture_data.append({
            "Business Name":   business,
            "Cohort":          cohort,
            "Total Sales (R)": total_sales,
            "Sales Growth %":  sales_growth,
            "Profit Growth %": profit_growth,
            "Latest Jobs":     net_jobs,
            "Jobs Pct Change": jobs_pct,
            "Female Jobs":     int(current_female),
            "Youth Jobs":      int(current_youth),
            "New Female Jobs": new_female,
            "New Youth Jobs":  new_youth,
            "Total Subscribers": int(venture_subs),
            "New Subscribers":   int(venture_new_subs),
            "Total Schools":     int(venture_schools),
            "Months":            n,
            "Red Flags":         flags,
        })

    # ── 6. Program Overview ────────────────────────────────────────────────────
    avg_sales_growth  = round(median(all_sales_growth), 1)  if all_sales_growth  else "Insufficient Data"
    avg_profit_growth = round(median(all_profit_growth), 1) if all_profit_growth else "Insufficient Data"

    # ── 7. Cohort Summaries ────────────────────────────────────────────────────
    cohort_summaries = []
    cohort_growth_data: dict = {}

    for cohort_name, cg in df.groupby("Cohort"):
        biz_count = cg["Business Name"].nunique()

        # Latest snapshot per business (for totals)
        latest = cg.sort_values("Date").groupby("Business Name").last()
        coh_jobs     = float(latest["Total Jobs"].sum())
        coh_baseline = float(cg.sort_values("Date").groupby("Business Name").first()["Total Jobs"].sum())
        coh_jobs_pct = round(((coh_jobs - coh_baseline) / coh_baseline) * 100, 1) if coh_baseline else 0.0

        coh_tot_learners  = float(latest["Total Subscribers Students"].sum())
        coh_tot_educators = float(latest["Total Subscribers Teachers"].sum())
        coh_new_learners  = float(cg["New Subscribers Students"].sum())
        coh_new_educators = float(cg["New Subscribers Teachers"].sum())

        coh_sg, coh_pg, coh_months = [], [], []
        for biz, bg in cg.groupby("Business Name"):
            bs = bg.sort_values("Date")
            sg = _tiered_sales_growth(bs["Monthly Sales (R)"])
            pg = _tiered_profit_growth(bs["Monthly Net Profit"])
            if isinstance(sg, (int, float)): coh_sg.append(sg)
            if isinstance(pg, (int, float)): coh_pg.append(pg)
            coh_months.append(len(bs))

        med_sg = round(median(coh_sg), 1) if coh_sg else "Insufficient Data"
        med_pg = round(median(coh_pg), 1) if coh_pg else "Insufficient Data"
        avg_mo = sum(coh_months) / len(coh_months) if coh_months else 0

        cohort_growth_data[cohort_name] = {"median_sg": med_sg, "exposure": avg_mo}

        cohort_summaries.append({
            "Cohort":              cohort_name,
            "Ventures":            biz_count,
            "Total Sales":         float(cg["Monthly Sales (R)"].sum()),
            "Total Profit":        float(cg["Monthly Net Profit"].sum()),
            "Total Jobs":          int(coh_jobs),
            "Jobs Pct Change":     coh_jobs_pct,
            "Total Learners":      int(coh_tot_learners),
            "Total Educators":     int(coh_tot_educators),
            "New Learners":        int(coh_new_learners),
            "New Educators":       int(coh_new_educators),
            "Median Sales Growth": med_sg,
            "Median Profit Growth": med_pg,
        })

    # ── 8. Program TWA ─────────────────────────────────────────────────────────
    twa_num = twa_den = 0.0
    for cd in cohort_growth_data.values():
        if isinstance(cd["median_sg"], (int, float)):
            twa_num += cd["median_sg"] * cd["exposure"]
            twa_den += cd["exposure"]
    program_twa = round(twa_num / twa_den, 1) if twa_den > 0 else "Insufficient Data"

    # ── 9. Investment Ledger ───────────────────────────────────────────────────
    investment_ledger = []
    for (cohort_name, biz), grp in businesses:
        investment_ledger.append({
            "Business Name":      biz,
            "Cohort":             cohort_name,
            "Total Sales":        float(grp["Monthly Sales (R)"].sum()),
            "Net Profit":         float(grp["Monthly Net Profit"].sum()),
            "Grants & Investments": float(grp["Grants Value"].sum()),
        })

    # ── 10. Jobs Summary ───────────────────────────────────────────────────────
    prog_baseline = sum(
        float(grp.sort_values("Date").iloc[0]["Total Jobs"])
        for (_, _), grp in businesses
    )
    prog_jobs_pct = round(
        ((prog_total_jobs - prog_baseline) / prog_baseline) * 100, 1
    ) if prog_baseline else 0.0

    jobs_summary = {
        "Total Jobs":     int(prog_total_jobs),
        "New Jobs":       int(prog_new_jobs),
        "Jobs Pct Change": prog_jobs_pct,
        "Female Jobs":    int(prog_female_jobs),
        "Youth Jobs":     int(prog_youth_jobs),
        "New Female Jobs": int(prog_female_new),
        "New Youth Jobs":  int(prog_youth_new),
    }

    # ── 11. Reach Summary ──────────────────────────────────────────────────────
    female_pct     = round((prog_female_stu   / prog_total_stu) * 100, 1) if prog_total_stu else 0
    rural_pct      = round((prog_rural_stu    / prog_total_stu) * 100, 1) if prog_total_stu else 0
    disability_pct = round((prog_disability_stu / prog_total_stu) * 100, 1) if prog_total_stu else 0

    reach_summary = {
        "Total Subscribers": int(prog_total_sub),
        "New Subscribers":   int(prog_new_sub),
        "Total Schools":     int(prog_total_schools),
        "Female %":          female_pct,
        "Rural %":           rural_pct,
        "Disability %":      disability_pct,
    }

    # ── 12. Chart time-series (real calendar months) ───────────────────────────
    ts_df = pd.DataFrame(all_time_series)
    cohort_time_series: dict = {}
    program_time_series = {"months": [], "sales": [], "profit": [], "jobs": []}

    if not ts_df.empty:
        for cname in ts_df["cohort"].unique():
            ct = ts_df[ts_df["cohort"] == cname]
            mo = ct.groupby("month").agg({"sales": "sum", "profit": "sum", "jobs": "sum"}).sort_index()
            cohort_time_series[cname] = {
                "months": mo.index.tolist(),
                "sales":  mo["sales"].tolist(),
                "profit": mo["profit"].tolist(),
                "jobs":   mo["jobs"].tolist(),
            }
        prog_mo = ts_df.groupby("month").agg({"sales": "sum", "profit": "sum", "jobs": "sum"}).sort_index()
        program_time_series = {
            "months": prog_mo.index.tolist(),
            "sales":  prog_mo["sales"].tolist(),
            "profit": prog_mo["profit"].tolist(),
            "jobs":   prog_mo["jobs"].tolist(),
        }

    # ── 13. Cohort Detail (per-tab drill-down) ─────────────────────────────────
    cohort_detail: dict = {}

    for cohort_name in ["Cohort 1", "Cohort 2", "Cohort 3", "Cohort 4"]:
        cdf  = df[df["Cohort"] == cohort_name].copy()
        valid = cdf[cdf["Date"].notna()].copy()

        fellows_sales  = []
        fellows_profit = []
        coh_sg_vals    = []
        coh_pg_vals    = []
        coh_month_counts = []

        for biz_name, bg in valid.groupby("Business Name"):
            bs  = bg.sort_values("Date")
            n_b = len(bs)
            sg  = _tiered_sales_growth(bs["Monthly Sales (R)"])
            pg  = _tiered_profit_growth(bs["Monthly Net Profit"])
            if isinstance(sg, (int, float)): coh_sg_vals.append(sg)
            if isinstance(pg, (int, float)): coh_pg_vals.append(pg)
            coh_month_counts.append(n_b)

            months_str = bs["Date"].dt.strftime("%Y-%m").tolist()
            sales_vals  = [round(float(v)) for v in bs["Monthly Sales (R)"].tolist()]
            profit_vals = [round(float(v)) for v in bs["Monthly Net Profit"].tolist()]

            fellows_sales.append({
                "name":   biz_name,
                "growth": sg,
                "months": n_b,
                "data":   [{"x": m, "y": s} for m, s in zip(months_str, sales_vals)],
            })
            fellows_profit.append({
                "name":   biz_name,
                "growth": pg,
                "months": n_b,
                "data":   [{"x": m, "y": p} for m, p in zip(months_str, profit_vals)],
            })

        coh_med_sg = round(median(coh_sg_vals), 1) if coh_sg_vals else "Insufficient Data"
        coh_med_pg = round(median(coh_pg_vals), 1) if coh_pg_vals else "Insufficient Data"
        avg_months = sum(coh_month_counts) / len(coh_month_counts) if coh_month_counts else 0

        # Cohort aggregate (sum of all fellows per calendar month)
        if not valid.empty:
            coh_agg = valid.groupby(valid["Date"].dt.strftime("%Y-%m")).agg({
                "Monthly Sales (R)": "sum",
                "Monthly Net Profit": "sum",
            }).sort_index()
            cohort_aggregate = {
                "months": coh_agg.index.tolist(),
                "sales":  [round(float(v)) for v in coh_agg["Monthly Sales (R)"]],
                "profit": [round(float(v)) for v in coh_agg["Monthly Net Profit"]],
            }
        else:
            cohort_aggregate = {"months": [], "sales": [], "profit": []}

        # Jobs bar chart (monthly aggregate)
        if not valid.empty:
            jm = valid.groupby(valid["Date"].dt.strftime("%Y-%m")).agg({
                "Total Jobs": "sum", "Female Jobs": "sum", "Youth Jobs": "sum",
            }).sort_index()
            jobs_bar = {
                "months": jm.index.tolist(),
                "total":  [round(float(v)) for v in jm["Total Jobs"]],
                "female": [round(float(v)) for v in jm["Female Jobs"]],
                "youth":  [round(float(v)) for v in jm["Youth Jobs"]],
            }
        else:
            jobs_bar = {"months": [], "total": [], "female": [], "youth": []}

        # Per-fellow jobs time series
        fellows_jobs = []
        if not valid.empty:
            for biz_name, bg in valid.groupby("Business Name"):
                bs = bg.sort_values("Date")
                bm = bs.groupby(bs["Date"].dt.strftime("%Y-%m")).agg({
                    "Total Jobs": "sum", "Female Jobs": "sum", "Youth Jobs": "sum",
                }).sort_index()
                fellows_jobs.append({
                    "name":   biz_name,
                    "months": bm.index.tolist(),
                    "total":  [round(float(v)) for v in bm["Total Jobs"]],
                    "female": [round(float(v)) for v in bm["Female Jobs"]],
                    "youth":  [round(float(v)) for v in bm["Youth Jobs"]],
                })

        # Jobs table (per fellow — latest snapshot, % change = baseline→current)
        jobs_table = []
        for biz_name, bg in cdf.groupby("Business Name"):
            bs = bg.sort_values("Date")
            n_b = len(bs)
            ct  = float(bs.iloc[-1]["Total Jobs"])  if n_b > 0 else 0.0
            ft  = float(bs.iloc[0]["Total Jobs"])   if n_b > 0 else 0.0
            new_j = int(ct - ft) if n_b >= 2 else 0
            cf  = float(bs.iloc[-1]["Female Jobs"]) if n_b > 0 else 0.0
            ff  = float(bs.iloc[0]["Female Jobs"])  if n_b > 0 else 0.0
            new_f = int(cf - ff) if n_b >= 2 else 0
            cy  = float(bs.iloc[-1]["Youth Jobs"])  if n_b > 0 else 0.0
            # % change: (current − baseline) / baseline  [consistent with cohort/program]
            pct = round(((ct - ft) / ft) * 100, 1) if ft != 0 else 0.0
            jobs_table.append({
                "name": biz_name, "total": int(ct), "new": new_j,
                "pct_change": pct, "new_female": new_f, "youth": int(cy),
            })

        # Investments table
        investments_table = []
        for _, row in cdf.iterrows():
            gv = float(row.get("Grants Value", 0) or 0)
            if gv > 0:
                investments_table.append({
                    "name":     row["Business Name"],
                    "value":    gv,
                    "investor": str(row.get("Grant Funder", "") or "Not specified"),
                    "month":    str(row.get("Reporting Month", "")),
                })

        # Reach time series (Total subscribers + schools, per calendar month)
        # NOTE: community_learners/educators are intentionally excluded from charts
        #       per Round 2 feedback ("Remove community learners and educators")
        if not valid.empty:
            rm = valid.groupby(valid["Date"].dt.strftime("%Y-%m")).agg({
                "Total Subscribers Students": "sum",
                "Total Subscribers Teachers": "sum",
                "New Subscribers Students":   "sum",
                "New Subscribers Teachers":   "sum",
                "SA Schools":                 "sum",
                "Q1-3 Schools":               "sum",
            }).sort_index()
            reach = {
                "months":          rm.index.tolist(),
                "total_learners":  [round(float(v)) for v in rm["Total Subscribers Students"]],
                "total_educators": [round(float(v)) for v in rm["Total Subscribers Teachers"]],
                "new_learners_cum":  [round(float(v)) for v in rm["New Subscribers Students"].cumsum()],
                "new_educators_cum": [round(float(v)) for v in rm["New Subscribers Teachers"].cumsum()],
                "sa_schools":      [round(float(v)) for v in rm["SA Schools"]],
                "q13_schools":     [round(float(v)) for v in rm["Q1-3 Schools"]],
            }
        else:
            reach = {
                "months": [], "total_learners": [], "total_educators": [],
                "new_learners_cum": [], "new_educators_cum": [],
                "sa_schools": [], "q13_schools": [],
            }

        # Per-fellow reach time series
        fellows_reach = []
        if not valid.empty:
            for biz_name, bg in valid.groupby("Business Name"):
                bs = bg.sort_values("Date")
                bm = bs.groupby(bs["Date"].dt.strftime("%Y-%m")).agg({
                    "Total Subscribers Students": "sum",
                    "Total Subscribers Teachers": "sum",
                    "New Subscribers Students":   "sum",
                    "New Subscribers Teachers":   "sum",
                    "SA Schools":                 "sum",
                    "Q1-3 Schools":               "sum",
                }).sort_index()
                fellows_reach.append({
                    "name":            biz_name,
                    "months":          bm.index.tolist(),
                    "total_learners":  [round(float(v)) for v in bm["Total Subscribers Students"]],
                    "total_educators": [round(float(v)) for v in bm["Total Subscribers Teachers"]],
                    "new_learners_cum":  [round(float(v)) for v in bm["New Subscribers Students"].cumsum()],
                    "new_educators_cum": [round(float(v)) for v in bm["New Subscribers Teachers"].cumsum()],
                    "sa_schools":      [round(float(v)) for v in bm["SA Schools"]],
                    "q13_schools":     [round(float(v)) for v in bm["Q1-3 Schools"]],
                })

        # Growth % table (with flags + months badge)
        growth_table = []
        for biz_name, bg in cdf.groupby("Business Name"):
            bs  = bg.sort_values("Date")
            n_b = len(bs)
            sg  = _tiered_sales_growth(bs["Monthly Sales (R)"])
            pg  = _tiered_profit_growth(bs["Monthly Net Profit"])
            flags = []
            if isinstance(sg, (int, float)):
                if sg < 0:    flags.append("Negative Sales Growth ⚠️")
                elif sg > 20: flags.append("Strong Sales Growth ✨")
            if isinstance(pg, (int, float)):
                if pg < 0:    flags.append("Negative Profit Growth ⚠️")
                elif pg > 20: flags.append("Strong Profit Growth ✨")
            growth_table.append({
                "name":          biz_name,
                "sales_growth":  sg,
                "profit_growth": pg,
                "months":        n_b,          # ← used by HTML for "Xm cover" badge
                "flags":         flags,
            })

        # Users table (subscribers per fellow)
        users_table = []
        for biz_name, bg in cdf.groupby("Business Name"):
            bs  = bg.sort_values("Date")
            n_b = len(bs)
            tot_l = float(bs.iloc[-1]["Total Subscribers Students"]) if n_b > 0 else 0.0
            tot_e = float(bs.iloc[-1]["Total Subscribers Teachers"]) if n_b > 0 else 0.0
            new_l = float(bs["New Subscribers Students"].sum())
            new_e = float(bs["New Subscribers Teachers"].sum())
            u_flags = []
            if n_b > 0 and (tot_l + tot_e) > 0:
                ann = ((new_l + new_e) / n_b) * 12
                if ann < 8_000:
                    u_flags.append("Low Learner Reach (<8,000/yr)")
            users_table.append({
                "name":          biz_name,
                "tot_learners":  int(tot_l),
                "tot_educators": int(tot_e),
                "new_learners":  int(new_l),
                "new_educators": int(new_e),
                "flags":         u_flags,
            })

        # Learner disaggregation table
        disagg_table = []
        for biz_name, bg in cdf.groupby("Business Name"):
            bs  = bg.sort_values("Date")
            n_b = len(bs)
            if n_b > 0:
                lat = bs.iloc[-1]
                disagg_table.append({
                    "name":       biz_name,
                    "female":     int(float(lat.get("Female Students", 0) or 0)),
                    "rural":      int(float(lat.get("Rural Students", 0) or 0)),
                    "disability": int(float(lat.get("Disability Students", 0) or 0)),
                })

        cohort_detail[cohort_name] = {
            "cohort_median_sg":  coh_med_sg,
            "cohort_median_pg":  coh_med_pg,
            "cohort_months":     int(avg_months),
            "fellows_sales":     fellows_sales,
            "fellows_profit":    fellows_profit,
            "fellows_reach":     fellows_reach,
            "fellows_jobs":      fellows_jobs,
            "cohort_aggregate":  cohort_aggregate,
            "growth_table":      growth_table,
            "jobs_bar":          jobs_bar,
            "jobs_table":        jobs_table,
            "investments_table": investments_table,
            "reach":             reach,
            "users_table":       users_table,
            "disaggregation":    disagg_table,
        }

    # ── 14. Assemble and return ────────────────────────────────────────────────
    return {
        "Program_Overview": {
            "Total_Sales_ZAR":         int(grand_total_sales),
            "Net_Jobs_Created":        int(total_net_jobs),
            "Average_Sales_Growth_%":  avg_sales_growth,
            "Average_Profit_Growth_%": avg_profit_growth,
            "Total_Ventures":          int(df["Business Name"].nunique()),
            "Program_TWA":             program_twa,
        },
        "Venture_Data":       venture_data,
        "Cohort_Summaries":   cohort_summaries,
        "Investment_Ledger":  investment_ledger,
        "Jobs_Summary":       jobs_summary,
        "Reach_Summary":      reach_summary,
        "Time_Series": {
            "cohort":            cohort_time_series,
            "program":           program_time_series,
            "program_extended":  program_series,
        },
        "Red_Flags":    red_flags,
        "Cohort_Detail": cohort_detail,
    }


# ── Smoke test ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json
    from data_fetcher import fetch_dashboard_data

    print("Fetching …")
    raw = fetch_dashboard_data()

    # Sanity-check date parsing
    raw["_Date"] = raw["Reporting Month"].apply(parse_reporting_month)
    total      = len(raw)
    parsed_ok  = raw["_Date"].notna().sum()
    nat_count  = total - parsed_ok
    print(f"\nDate parsing: {parsed_ok}/{total} rows parsed  ({nat_count} NaT)")
    if nat_count:
        print("  Sample unparseable values:")
        print(raw.loc[raw["_Date"].isna(), "Reporting Month"].unique()[:10].tolist())

    print("\nCalculating KPIs …")
    result = calculate_kpis(raw)

    ov = result["Program_Overview"]
    print(f"\n--- Program Overview ---")
    print(f"  Ventures:      {ov['Total_Ventures']}")
    print(f"  Sales Growth:  {ov['Average_Sales_Growth_%']}")
    print(f"  Profit Growth: {ov['Average_Profit_Growth_%']}")
    print(f"  Net Jobs:      {ov['Net_Jobs_Created']}")
    print(f"  Program TWA:   {ov['Program_TWA']}")

    print(f"\n--- Cohort Summaries ---")
    for cs in result["Cohort_Summaries"]:
        print(f"  {cs['Cohort']}: {cs['Ventures']} ventures, "
              f"SG={cs['Median Sales Growth']}, "
              f"PG={cs['Median Profit Growth']}")
