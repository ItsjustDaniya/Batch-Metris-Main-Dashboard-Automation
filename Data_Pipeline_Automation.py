import os
import time
import json
import requests
import numpy as np
import pandas as pd
import gspread
from datetime import datetime
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor

# -------------------- START TIMER --------------------
start_time = time.time()

# -------------------- ENV & AUTH --------------------
METABASE_API_KEY = os.getenv("METABASE_API_KEY")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
SHEET_KEY = os.getenv("BATCH_METRICS_SHEET_KEY")

if not METABASE_API_KEY or not service_account_json:
    raise ValueError("❌ Missing environment variables. Check GitHub secrets.")
if not SHEET_KEY:
    raise ValueError("❌ BATCH_METRICS_SHEET_KEY is not set. Check GitHub secrets.")

# -------------------- GOOGLE AUTH --------------------
service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc = gspread.authorize(creds)

# -------------------- METABASE AUTH --------------------
METABASE_HEADERS = {
    'Content-Type': 'application/json',
    'x-api-key': METABASE_API_KEY
}
print("✅ Using Metabase API key auth")

# -------------------- UTILITIES --------------------
def mb_post(card_url):
    """POST to a Metabase card URL and return the response."""
    r = requests.post(card_url, headers=METABASE_HEADERS, timeout=120)
    r.raise_for_status()
    return r

def write_sheet(sheet_key, worksheet_name, df):
    """Clear and write a DataFrame to a Google Sheet."""
    df = add_segment_cols(df, worksheet_name)
    print(f"🔄 Updating sheet: {worksheet_name} ({len(df)} rows)")
    for attempt in range(1, 6):
        try:
            sheet = gc.open_by_key(sheet_key)
            ws = sheet.worksheet(worksheet_name)
            ws.clear()
            set_with_dataframe(ws, df, include_index=False, include_column_header=True)
            print(f"✅ Successfully updated: {worksheet_name}")
            return
        except Exception as e:
            print(f"[Sheets] Attempt {attempt} failed for {worksheet_name}: {e}")
            if attempt < 5:
                time.sleep(20)
            else:
                print(f"❌ All attempts failed for {worksheet_name}.")
                raise

def clean_to_int(series):
    """Clean a series to integers, handling commas and floats."""
    return pd.to_numeric(
        series.astype(str)
              .str.replace(',', '')
              .str.replace(r'\.0$', '', regex=True)
              .str.strip(),
        errors='coerce'
    ).fillna(0).astype(int)

# -------------------- GEM / PROGRAM SEGMENTS (Oct 2026) --------------------
# Every tab gets a `gem_label` (GEM / Non-GEM / Not tagged) and a `Program`
# (PCDS / Adv+ / Advantage / Agentic AI / Xcelerate / Other) column so Looker can filter on them.
# Adv+ students carry the 'Advantage +' label (722) in card 6289, so they now count as enrolled.
ENROLLED_LABELS = ['Enrolled', 'Advantage +']
NOT_TAGGED = 'Not tagged'
BATCH_NAME_COLUMNS = ['au_batch_name', 'Au_name', 'Admin_unit_name', 'admin_unit_name', 'AU_batch', 'Batch', 'batch']


def program_from_batch(name):
    """Program from an AU batch name, e.g. 'Newton Advantage Plus - Data Analytics May 2026' -> 'Adv+'."""
    n = str(name or '').lower()
    if not n or n == 'nan':
        return 'Other'
    if 'advantage plus' in n or 'advantage +' in n or 'adv+' in n:
        return 'Adv+'
    if 'advantage' in n:
        return 'Advantage'
    if 'agentic' in n:
        return 'Agentic AI'
    if 'xcelerate' in n:
        return 'Xcelerate'
    if 'data science' in n or 'pcds' in n:
        return 'PCDS'
    return 'Other'


def add_segment_cols(df, worksheet_name=''):
    """Tidy gem_label and put a Program column at the end of every tab."""
    if df is None or df.empty:
        return df
    df = df.copy()
    if 'gem_label' in df.columns:
        df['gem_label'] = df['gem_label'].replace('', np.nan).fillna(NOT_TAGGED)
    if 'program' in df.columns and 'Program' not in df.columns:      # from the Metabase cards
        df = df.rename(columns={'program': 'Program'})
    if 'Program' not in df.columns:
        name_col = next((c for c in BATCH_NAME_COLUMNS if c in df.columns), None)
        if name_col is None:
            print(f"   ℹ️ {worksheet_name}: no batch name column — Program not added")
            return df
        df['Program'] = df[name_col].apply(program_from_batch)
    df['Program'] = df['Program'].fillna('Other')
    return df[[c for c in df.columns if c != 'Program'] + ['Program']]


_USER_SEGMENTS = None

def get_user_segments():
    """user_id -> latest AU batch name + gem_label, from card 6289 (fetched once per run)."""
    global _USER_SEGMENTS
    if _USER_SEGMENTS is None:
        r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6289/query/json')
        g = pd.DataFrame(r.json())
        g['user_id'] = clean_to_int(g['user_id'])
        g['gem_label'] = g['gem_label'].fillna(NOT_TAGGED)
        sort_col = next((c for c in ['au_start_date', 'AU_start_date'] if c in g.columns), None)
        if sort_col:
            g = g.sort_values(sort_col, ascending=False)          # latest AU wins
        _USER_SEGMENTS = g.drop_duplicates('user_id')[['user_id', 'au_batch_name', 'gem_label']]
    return _USER_SEGMENTS


def fetch_enrolled_df():
    """Fetch enrolled students (PCDS and Adv+) from Metabase card 6289."""
    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6289/query/json')
    df_au = pd.DataFrame(r.json())
    df_au = df_au[['user_id', 'label', 'au_batch_name', 'gem_label']]
    df_au = df_au[df_au['label'].isin(ENROLLED_LABELS)]
    df_au['user_id'] = clean_to_int(df_au['user_id'])
    df_au = df_au.rename(columns={'au_batch_name': 'admin_unit_name'})
    # Fill null gem_label so it's never dropped from grouping downstream
    df_au['gem_label'] = df_au['gem_label'].fillna(NOT_TAGGED)
    df_au = df_au.drop_duplicates(['user_id', 'admin_unit_name'])
    return df_au


def valid_contest_keys(df, min_entries):
    """Contests that count, as (admin_unit_name, module_name, contest_date).
    Same rule as before: more than `min_entries` entries in that AU batch. Adv+ batches are
    small (8-40 students) and sit the same contest as PCDS, so an Adv+ batch's contest also
    counts when that module + date is a valid contest for another batch."""
    k = ['admin_unit_name', 'module_name', 'contest_date']
    counts = df.groupby(k).size().reset_index(name='batch_entries')
    valid = counts[counts['batch_entries'] > min_entries]
    is_adv = counts['admin_unit_name'].apply(program_from_batch) == 'Adv+'
    shared = counts[is_adv].merge(valid[['module_name', 'contest_date']].drop_duplicates(),
                                  on=['module_name', 'contest_date'])
    return pd.concat([valid[k], shared[k]]).drop_duplicates()

# -------------------- SECTION 1: ASSIGNMENT --------------------
def run_assignment():
    print("\n📌 Running: Assignment")
    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/9136/query/json')
    df = pd.DataFrame(r.json())
    write_sheet(SHEET_KEY, "Assignment", df)

# -------------------- SECTION 2: ATTENDANCE --------------------
def run_attendance():
    print("\n📌 Running: Attendance")
    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/9155/query/json')
    df = pd.DataFrame(r.json())
    write_sheet(SHEET_KEY, "Attendance", df)

# -------------------- SECTION 3: TA --------------------
def run_ta():
    print("\n📌 Running: TA")
    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/9294/query/json')
    df = pd.DataFrame(r.json())
    write_sheet(SHEET_KEY, "TA", df)

# -------------------- SECTION 4: LECTURE RATING --------------------
def run_lecture_rating():
    print("\n📌 Running: Lecture Rating")
    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/9298/query/json')
    df = pd.DataFrame(r.json())
    write_sheet(SHEET_KEY, "Lecture_Rating", df)

# -------------------- SECTION 5: PLAYLIST --------------------
def run_playlist():
    print("\n📌 Running: Playlist")
    r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/9292/query/json')
    df = pd.DataFrame(r.json())
    write_sheet(SHEET_KEY, "playlist", df)

# -------------------- SECTION 6: PLACEMENT PHASE --------------------
def run_placement_phase():
    print("\n📌 Running: Placement Phase")

    workbook = gc.open('Batch-wise-Metrics')
    ws = workbook.worksheet('Prog<>Placement')
    data = ws.get_all_values()
    df = pd.DataFrame(data)
    df.columns = df.iloc[0]
    df = df.iloc[1:]

    df['Enrolled Status'] = df['Enrolled Status'].astype(str).str.strip()
    df['Phase'] = df['Phase'].astype(str).str.strip()

    denominator_mask = (
        df['Enrolled Status'].str.lower().isin(['currently enrolled', 'moonshot'])
    ) & (
        df['Phase'].str.lower() != 'unavailable'
    ) & (
        df['Phase'].str.lower() != 'debarred'
    )
    filtered_df = df[denominator_mask].copy()

    # GEM / Program per student (via user id -> card 6289); falls back to the Batch name
    uid_col = next((c for c in ['User ID', 'UserID', 'User Id', 'user_id', 'Userid', 'userid'] if c in filtered_df.columns), None)
    if uid_col:
        seg = get_user_segments()
        filtered_df['_uid'] = clean_to_int(filtered_df[uid_col])
        filtered_df = filtered_df.merge(seg.rename(columns={'user_id': '_uid'}), on='_uid', how='left')
        filtered_df['gem_label'] = filtered_df['gem_label'].fillna(NOT_TAGGED)
        filtered_df['Program'] = filtered_df['au_batch_name'].apply(program_from_batch)
        no_match = filtered_df['au_batch_name'].isna()
        filtered_df.loc[no_match, 'Program'] = filtered_df.loc[no_match, 'Batch'].apply(program_from_batch)
    else:
        print("   ℹ️ Placement_Phase: no user id column found — GEM split skipped")
        filtered_df['gem_label'] = NOT_TAGGED
        filtered_df['Program'] = filtered_df['Batch'].apply(program_from_batch)
    keys = ['Batch', 'gem_label', 'Program']

    recommended_mask = (filtered_df['Recommended date'].notna()) & \
                       (filtered_df['Recommended date'].astype(str).str.strip() != "")
    grooming_mask = (filtered_df['Grooming Pool (Picked)'].notna()) & \
                    (filtered_df['Grooming Pool (Picked)'].astype(str).str.strip() != "")

    placed_list = [
        'Placed - NS + Self (with Testimonial)', 'Placed - Self (w/o testimonial)',
        'Placed once; now returned', 'Placed - Self (w/o attribution)',
        'Placed - NS (w/o testimonial)', 'Placed - Offer Reject',
        'Placed - Unclean/Unaccountable'
    ]
    pr_list = ['Placement', 'Placement pause', 'PR']
    demoted_list = ['Demoted']

    placement_mask = filtered_df['Placed'].str.lower().isin([p.lower() for p in placed_list])
    pr_mask = filtered_df['Phase'].str.lower().isin([p.lower() for p in pr_list]) + \
              filtered_df['PR'].str.lower().isin([p.lower() for p in demoted_list])

    batch_stats   = filtered_df.groupby(keys).size().reset_index(name='Total_Eligible_#')
    rec_counts    = filtered_df[recommended_mask].groupby(keys).size().reset_index(name='Recommended_#')
    gro_counts    = filtered_df[grooming_mask].groupby(keys).size().reset_index(name='Grooming_#')
    place_counts  = filtered_df[placement_mask].groupby(keys).size().reset_index(name='Placement_#')
    pr_counts     = filtered_df[pr_mask].groupby(keys).size().reset_index(name='PR_#')

    summary = pd.merge(batch_stats, rec_counts, on=keys, how='left').fillna(0)
    summary = pd.merge(summary, gro_counts, on=keys, how='left').fillna(0)
    summary = pd.merge(summary, place_counts, on=keys, how='left').fillna(0)
    summary = pd.merge(summary, pr_counts, on=keys, how='left').fillna(0)

    count_cols = ['Total_Eligible_#', 'Recommended_#', 'Grooming_#', 'Placement_#', 'PR_#']
    summary[count_cols] = summary[count_cols].astype(int)

    summary['Recommended_%'] = (summary['Recommended_#'] / summary['Total_Eligible_#'] * 100).round(2)
    summary['Grooming_%']    = (summary['Grooming_#']    / summary['Total_Eligible_#'] * 100).round(2)
    summary['Placement_%']   = (summary['Placement_#']   / summary['Total_Eligible_#'] * 100).round(2)
    summary['PR_%']          = (summary['PR_#']          / summary['Total_Eligible_#'] * 100).round(2)

    final_report = summary[[
        'Batch', 'gem_label', 'Total_Eligible_#',
        'Recommended_#', 'Recommended_%',
        'Grooming_#', 'Grooming_%',
        'PR_#', 'PR_%',
        'Placement_#', 'Placement_%',
        'Program'
    ]]
    write_sheet(SHEET_KEY, "Placement_Phase", final_report)

# -------------------- SECTION 7: MODULE CONTEST ATTEMPT WISE --------------------
def run_mc_attempt_wise():
    print("\n📌 Running: Module Contest Attempt Wise")

    workbook = gc.open('Placements')
    ws = workbook.worksheet('MC_Raw_2')
    data = ws.get_all_values()
    df = pd.DataFrame(data)
    df.columns = df.iloc[0]
    df = df.iloc[1:]

    df_au = fetch_enrolled_df()
    df['user_id'] = clean_to_int(df['user_id'])
    df = pd.merge(df, df_au, on=['user_id', 'admin_unit_name'], how='inner')

    # Fill null gem_label so it's never dropped from grouping downstream
    df['gem_label'] = df['gem_label'].fillna(NOT_TAGGED)

    df['Total Score'] = pd.to_numeric(df['Total Score'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df['contest_date'] = pd.to_datetime(df['contest_date'])

    threshold = 64
    df['is_cleared'] = df['Total Score'] >= threshold
    df['scored_gt_1'] = df['Total Score'] > 1

    # Batch strength split by gem_label too
    batch_strength = df.groupby(['admin_unit_name', 'gem_label'])['user_id'].nunique().reset_index()
    batch_strength.columns = ['admin_unit_name', 'gem_label', 'Batch_strength']

    attempts_group = df.groupby(['admin_unit_name', 'gem_label', 'module_name', 'contest_date']).agg(
        total_entries=('user_id', 'count'),
        scored_gt_1_count=('scored_gt_1', 'sum'),
        cleared_count=('is_cleared', 'sum')
    ).reset_index()

    # Which contests count is decided per AU batch (not per GEM segment), so a segment isn't dropped
    attempts_filtered = attempts_group.merge(valid_contest_keys(df, 10), on=['admin_unit_name', 'module_name', 'contest_date'])
    attempts_filtered = attempts_filtered.sort_values(['admin_unit_name', 'gem_label', 'module_name', 'contest_date'])
    attempts_filtered['Attempt_number'] = attempts_filtered.groupby(['admin_unit_name', 'gem_label', 'module_name']).cumcount() + 1

    final_df = pd.merge(attempts_filtered, batch_strength, on=['admin_unit_name', 'gem_label'], how='left')
    final_df['Attempt%']   = (final_df['scored_gt_1_count'] / final_df['Batch_strength'].replace(0, 1)) * 100
    final_df['Clearance%'] = (final_df['cleared_count']     / final_df['Batch_strength'].replace(0, 1)) * 100

    final_df = final_df.rename(columns={
        'admin_unit_name': 'Admin_unit_name',
        'module_name': 'Module_name',
        'contest_date': 'Contest_date'
    })

    result_cols = ['Admin_unit_name', 'gem_label', 'Batch_strength', 'Module_name', 'Attempt_number', 'Contest_date', 'Attempt%', 'Clearance%']
    final_report = final_df[result_cols].copy()
    final_report['Contest_date'] = final_report['Contest_date'].dt.strftime('%Y-%m-%d')
    final_report['Attempt%']   = final_report['Attempt%'].round(2)
    final_report['Clearance%'] = final_report['Clearance%'].round(2)

    write_sheet(SHEET_KEY, "MC_Attempt_wise", final_report)

# -------------------- SECTION 8: MID MODULE CONTEST ATTEMPT WISE --------------------
def run_mid_mc_attempt_wise():
    print("\n📌 Running: Mid Module Contest Attempt Wise")

    workbook = gc.open('Placements')
    ws = workbook.worksheet('Mid_MC_Raw')
    data = ws.get_all_values()
    df = pd.DataFrame(data)
    df.columns = df.iloc[0]
    df = df.iloc[1:]

    df_au = fetch_enrolled_df()
    df['user_id'] = clean_to_int(df['user_id'])
    df = pd.merge(df, df_au, on=['user_id', 'admin_unit_name'], how='inner')

    df['Total Score'] = pd.to_numeric(df['Total Score'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df['contest_date'] = pd.to_datetime(df['contest_date'])

    threshold = 64
    df['is_cleared'] = df['Total Score'] >= threshold
    df['scored_gt_1'] = df['Total Score'] > 1

    df['gem_label'] = df['gem_label'].fillna(NOT_TAGGED)

    # Batch strength split by gem_label too
    batch_strength = df.groupby(['admin_unit_name', 'gem_label'])['user_id'].nunique().reset_index()
    batch_strength.columns = ['admin_unit_name', 'gem_label', 'Batch_strength']

    attempts_group = df.groupby(['admin_unit_name', 'gem_label', 'module_name', 'contest_date']).agg(
        total_entries=('user_id', 'count'),
        scored_gt_1_count=('scored_gt_1', 'sum'),
        cleared_count=('is_cleared', 'sum')
    ).reset_index()

    # Which contests count (>20 entries) is decided per AU batch, not per GEM segment
    attempts_filtered = attempts_group.merge(valid_contest_keys(df, 20), on=['admin_unit_name', 'module_name', 'contest_date'])
    attempts_filtered = attempts_filtered.sort_values(['admin_unit_name', 'gem_label', 'module_name', 'contest_date'])
    attempts_filtered['Attempt_number'] = attempts_filtered.groupby(['admin_unit_name', 'gem_label', 'module_name']).cumcount() + 1

    final_df = pd.merge(attempts_filtered, batch_strength, on=['admin_unit_name', 'gem_label'], how='left')
    final_df['Attempt%']   = (final_df['scored_gt_1_count'] / final_df['Batch_strength'].replace(0, 1)) * 100
    final_df['Clearance%'] = (final_df['cleared_count']     / final_df['Batch_strength'].replace(0, 1)) * 100

    final_df = final_df.rename(columns={
        'admin_unit_name': 'Admin_unit_name',
        'module_name': 'Module_name',
        'contest_date': 'Contest_date'
    })

    result_cols = ['Admin_unit_name', 'gem_label', 'Batch_strength', 'Module_name', 'Attempt_number', 'Contest_date', 'Attempt%', 'Clearance%']
    final_report = final_df[result_cols].copy()
    final_report['Contest_date'] = final_report['Contest_date'].dt.strftime('%Y-%m-%d')
    final_report['Attempt%']   = final_report['Attempt%'].round(2)
    final_report['Clearance%'] = final_report['Clearance%'].round(2)

    write_sheet(SHEET_KEY, "Mid_MC_Attempt_wise", final_report)

# -------------------- SECTION 9: MODULE CONTEST OVERALL WISE --------------------
def run_mc_overall_wise():
    print("\n📌 Running: Module Contest Overall Wise")

    workbook = gc.open('Placements')
    ws = workbook.worksheet('MC_Raw_2')
    data = ws.get_all_values()
    df = pd.DataFrame(data)
    df.columns = df.iloc[0]
    df = df.iloc[1:]

    df_au = fetch_enrolled_df()
    df['user_id'] = clean_to_int(df['user_id'])
    df = pd.merge(df, df_au, on=['user_id', 'admin_unit_name'], how='inner')

    # Fill null gem_label so it's never dropped from grouping downstream
    df['gem_label'] = df['gem_label'].fillna(NOT_TAGGED)

    df['Total Score'] = pd.to_numeric(df['Total Score'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df['contest_date'] = pd.to_datetime(df['contest_date'])

    valid_contests = valid_contest_keys(df, 10)
    df_valid = pd.merge(df, valid_contests, on=['admin_unit_name', 'module_name', 'contest_date'])

    # Batch strength split by gem_label too
    batch_strength = df.groupby(['admin_unit_name', 'gem_label'])['user_id'].nunique().reset_index()
    batch_strength.columns = ['Admin_unit_name', 'gem_label', 'Batch_strength']

    threshold = 64
    user_best_scores = df_valid.groupby(['admin_unit_name', 'gem_label', 'module_name', 'user_id'])['Total Score'].max().reset_index()
    user_best_scores['attempted'] = user_best_scores['Total Score'] > 1
    user_best_scores['cleared']   = user_best_scores['Total Score'] >= threshold

    overall_stats = user_best_scores.groupby(['admin_unit_name', 'gem_label', 'module_name']).agg(
        overall_attempt_count=('attempted', 'sum'),
        overall_clearance_count=('cleared', 'sum')
    ).reset_index()

    final_df = pd.merge(
        overall_stats, batch_strength,
        left_on=['admin_unit_name', 'gem_label'],
        right_on=['Admin_unit_name', 'gem_label']
    )
    final_df['Overall - Attempt%']   = (final_df['overall_attempt_count']   / final_df['Batch_strength']) * 100
    final_df['Overall - Clearance%'] = (final_df['overall_clearance_count'] / final_df['Batch_strength']) * 100
    final_df = final_df.rename(columns={'module_name': 'Module_name'})

    result_cols = ['Admin_unit_name', 'gem_label', 'Batch_strength', 'Module_name', 'Overall - Attempt%', 'Overall - Clearance%']
    final_report = final_df[result_cols].copy()
    final_report['Overall - Attempt%']   = final_report['Overall - Attempt%'].round(2)
    final_report['Overall - Clearance%'] = final_report['Overall - Clearance%'].round(2)

    write_sheet(SHEET_KEY, "MC_Overall_wise", final_report)

# -------------------- SECTION 10: MID MODULE CONTEST OVERALL WISE --------------------
def run_mid_mc_overall_wise():
    print("\n📌 Running: Mid Module Contest Overall Wise")

    workbook = gc.open('Placements')
    ws = workbook.worksheet('Mid_MC_Raw')
    data = ws.get_all_values()
    df = pd.DataFrame(data)
    df.columns = df.iloc[0]
    df = df.iloc[1:]

    df_au = fetch_enrolled_df()
    df['user_id'] = clean_to_int(df['user_id'])
    df = pd.merge(df, df_au, on=['user_id', 'admin_unit_name'], how='inner')

    # Fill null gem_label so it's never dropped from grouping downstream
    df['gem_label'] = df['gem_label'].fillna(NOT_TAGGED)

    df['Total Score'] = pd.to_numeric(df['Total Score'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df['contest_date'] = pd.to_datetime(df['contest_date'])

    valid_contests = valid_contest_keys(df, 10)
    df_valid = pd.merge(df, valid_contests, on=['admin_unit_name', 'module_name', 'contest_date'])

    # Batch strength split by gem_label too
    batch_strength = df.groupby(['admin_unit_name', 'gem_label'])['user_id'].nunique().reset_index()
    batch_strength.columns = ['Admin_unit_name', 'gem_label', 'Batch_strength']

    threshold = 64
    user_best_scores = df_valid.groupby(['admin_unit_name', 'gem_label', 'module_name', 'user_id'])['Total Score'].max().reset_index()
    user_best_scores['attempted'] = user_best_scores['Total Score'] > 1
    user_best_scores['cleared']   = user_best_scores['Total Score'] >= threshold

    overall_stats = user_best_scores.groupby(['admin_unit_name', 'gem_label', 'module_name']).agg(
        overall_attempt_count=('attempted', 'sum'),
        overall_clearance_count=('cleared', 'sum')
    ).reset_index()

    final_df = pd.merge(
        overall_stats, batch_strength,
        left_on=['admin_unit_name', 'gem_label'],
        right_on=['Admin_unit_name', 'gem_label']
    )
    final_df['Overall - Attempt%']   = (final_df['overall_attempt_count']   / final_df['Batch_strength']) * 100
    final_df['Overall - Clearance%'] = (final_df['overall_clearance_count'] / final_df['Batch_strength']) * 100
    final_df = final_df.rename(columns={'module_name': 'Module_name'})

    result_cols = ['Admin_unit_name', 'gem_label', 'Batch_strength', 'Module_name', 'Overall - Attempt%', 'Overall - Clearance%']
    final_report = final_df[result_cols].copy()
    final_report['Overall - Attempt%']   = final_report['Overall - Attempt%'].round(2)
    final_report['Overall - Clearance%'] = final_report['Overall - Clearance%'].round(2)

    write_sheet(SHEET_KEY, "Mid_MC_Overall_wise", final_report)

# -------------------- SECTION 11: PROJECTS --------------------
def parse_batch_date(batch_name):
    if not isinstance(batch_name, str):
        return pd.NaT
    parts = batch_name.split()
    for i in range(len(parts) - 1, 0, -1):
        try:
            month_abbr = parts[i-1][:3]
            year = parts[i]
            if len(year) == 4 and year.isdigit():
                return datetime.strptime(f"{month_abbr} {year}", '%b %Y')
        except:
            continue
    return pd.NaT

def calculate_project_metrics_robust(df):
    OVERALL_CUTOFF = datetime.now()
    df.columns = df.columns.str.strip()
    df = df[df['label'].isin(ENROLLED_LABELS)]
    df['gem_label'] = df['gem_label'].fillna(NOT_TAGGED)
    df = df[df['Module_name'].isin(['DS 02 Spreadsheets', 'DS 04 SQL', 'DS 03 Power BI'])]

    df['marks_obtained']          = pd.to_numeric(df['marks_obtained'], errors='coerce')
    df['project_release_date']    = pd.to_datetime(df['project_release_date'], errors='coerce')
    df['Submission Time']         = pd.to_datetime(df['Submission Time'], errors='coerce')

    metrics_list = []
    for (au_batch_name, gem_label, batch, module), group in df.groupby(['au_batch_name', 'gem_label', 'Batch', 'Module_name']):
        denominator = group['user_id'].nunique()
        release_dt_raw = group['project_release_date'].min()
        if pd.isna(release_dt_raw):
            continue
        release_date = release_dt_raw.date()

        windows = {
            'M0':  pd.to_datetime(release_date + relativedelta(days=30))  + pd.Timedelta(hours=23, minutes=59),
            'M1':  pd.to_datetime(release_date + relativedelta(days=60))  + pd.Timedelta(hours=23, minutes=59),
            'M2':  pd.to_datetime(release_date + relativedelta(days=90))  + pd.Timedelta(hours=23, minutes=59),
            'MTD': OVERALL_CUTOFF
        }

        res = {
            'au_batch_name': au_batch_name,
            'gem_label': gem_label,
            'batch': batch,
            'Module': module,
            'Release Date': release_date.strftime('%Y-%m-%d'),
            'Enrolled Count': denominator
        }

        for label, cutoff in windows.items():
            mask = (group['Submission Time'].notna()) & (group['Submission Time'] <= cutoff)
            att_count = group[mask]['user_id'].nunique()
            clr_count = group[mask & (group['marks_obtained'] >= 8)]['user_id'].nunique()
            res[f'{label} Attempt %']   = f"{(att_count / denominator * 100):.1f}" if denominator > 0 else "0.0"
            res[f'{label} Clearance %'] = f"{(clr_count / denominator * 100):.1f}" if denominator > 0 else "0.0"

        metrics_list.append(res)

    result_df = pd.DataFrame(metrics_list)
    if result_df.empty:
        return result_df
    result_df['Sort_Key'] = result_df['batch'].apply(parse_batch_date)
    result_df = result_df.sort_values(by=['Sort_Key', 'Module']).drop(columns=['Sort_Key'])

    final_columns = [
        'au_batch_name', 'gem_label', 'batch', 'Module', 'Release Date', 'Enrolled Count',
        'M0 Attempt %', 'M0 Clearance %',
        'M1 Attempt %', 'M1 Clearance %',
        'M2 Attempt %', 'M2 Clearance %',
        'MTD Attempt %', 'MTD Clearance %'
    ]
    return result_df[final_columns]

def run_projects():
    print("\n📌 Running: Projects")

    r1 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6241/query/json')
    r2 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6242/query/json')
    r3 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6289/query/json')

    df1 = pd.DataFrame(r1.json())
    df2 = pd.DataFrame(r2.json())
    df3 = pd.DataFrame(r3.json())[['user_id', 'au_batch_name', 'label', 'gem_label']]

    concatenated_df = pd.concat([df1, df2], axis=0, ignore_index=True)
    concatenated_df = concatenated_df.rename(columns={'User ID': 'user_id'})

    screened_df = pd.merge(concatenated_df, df3, on='user_id', how='left')
    screened_df['Submission Time']          = pd.to_datetime(screened_df['Submission Time'])
    screened_df['latest_feedback_given_time'] = pd.to_datetime(screened_df['latest_feedback_given_time'])
    screened_df['project_deadline_date']    = pd.to_datetime(screened_df['project_deadline_date'])

    # Write raw projects data
    raw_sheet = gc.open_by_key('1e9BxI2N6ms1hh_OdfHgktntxi41_2Y2Jv5Oo-WnQDAo')
    raw_ws = raw_sheet.worksheet("Projects")
    raw_ws.clear()
    set_with_dataframe(raw_ws, screened_df, include_index=False, include_column_header=True)
    print("✅ Raw Projects sheet updated")

    # Write metrics
    projects_df = calculate_project_metrics_robust(screened_df)
    write_sheet(SHEET_KEY, "Projects", projects_df)

# -------------------- MAIN: RUN ALL --------------------
if __name__ == "__main__":
    print("🚀 Starting Batch Metrics Automation...")

    tasks = [
        ("Assignment",                  run_assignment),
        ("Attendance",                  run_attendance),
        ("TA",                          run_ta),
        ("Lecture Rating",              run_lecture_rating),
        ("Playlist",                    run_playlist),
        ("Placement Phase",             run_placement_phase),
        ("MC Attempt Wise",             run_mc_attempt_wise),
        ("Mid MC Attempt Wise",         run_mid_mc_attempt_wise),
        ("MC Overall Wise",             run_mc_overall_wise),
        ("Mid MC Overall Wise",         run_mid_mc_overall_wise),
        ("Projects",                    run_projects),
    ]

    for name, fn in tasks:
        try:
            fn()
        except Exception as e:
            print(f"❌ Error in {name}: {e}")

    # -------------------- TIMESTAMP --------------------
    current_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%b-%Y %H:%M:%S")
    print(f"\n✅ Timestamp: {current_time}")

    end_time = time.time()
    mins, secs = divmod(end_time - start_time, 60)
    print(f"⏱ Total time: {int(mins)}m {int(secs)}s")
    print("🎯 Batch Metrics Automation completed successfully!")
