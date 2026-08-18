import os
import time
import json
import calendar
import requests
import pandas as pd
import gspread
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import traceback

# -------------------- START TIMER --------------------
start_time = time.time()

# -------------------- ENV & AUTH --------------------
METABASE_API_KEY = os.getenv("METABASE_API_KEY")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")

if not METABASE_API_KEY or not service_account_json:
    raise ValueError("❌ Missing environment variables. Check GitHub secrets.")

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

# -------------------- SHEET KEYS --------------------
SHEET_BATCH_REPORT    = '1D-nPNTKoma5mS6B5_gAH5vRWysoGbE6inPghK969n8A'
SHEET_PROJECTS_MAIN   = '1e9BxI2N6ms1hh_OdfHgktntxi41_2Y2Jv5Oo-WnQDAo'
SHEET_EVALUATIONS     = '1gUshIBjLIVh4CqhVACVirqkPuHx6muCZIvXcXRY9qG4'
SHEET_LECTURE_SUBJ    = '1QKoJUTCM_rz7y0DVh_xEwqV1aGGd9dPHcZaLxBUtpiU'
SHEET_MENTOR          = '1XQRBzWqhn5DkEypVfCUJzdG5Y0P94oztPpXP4IscIVA'
SHEET_NPS             = '1fK-H9xyW9h0dnO97GHc0i07x7JHVhAgfclywKZ7bA-4'

# -------------------- UTILITIES --------------------
def mb_post(card_url, retry_count=0):
    try:
        r = requests.post(card_url, headers=METABASE_HEADERS, timeout=120)
        r.raise_for_status()
        return r
    except requests.exceptions.Timeout:
        print(f"⏱️ Timeout for {card_url}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed for {card_url}: {e}")
        raise

def write_sheet(sheet_key, worksheet_name, df):
    """
    Write DataFrame to Google Sheet with retry logic and rate limit handling
    """
    print(f"🔄 Updating sheet: {worksheet_name} ({len(df)} rows)")
    
    for attempt in range(1, 6):
        try:
            time.sleep(2)  # Rate limit protection
            sheet = gc.open_by_key(sheet_key)
            ws = sheet.worksheet(worksheet_name)
            ws.clear()
            set_with_dataframe(ws, df, include_index=False, include_column_header=True)
            print(f"✅ Successfully updated: {worksheet_name}")
            return
        except gspread.exceptions.APIError as e:
            if "RESOURCE_EXHAUSTED" in str(e) or "Quota exceeded" in str(e):
                wait_time = 60 * attempt
                print(f"⏳ Rate limit hit, waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"[Sheets] Attempt {attempt} failed for {worksheet_name}: {e}")
                if attempt < 5:
                    time.sleep(20)
                else:
                    print(f"❌ All attempts failed for {worksheet_name}.")
                    raise
        except Exception as e:
            print(f"[Sheets] Attempt {attempt} failed for {worksheet_name}: {e}")
            if attempt < 5:
                time.sleep(20)
            else:
                print(f"❌ All attempts failed for {worksheet_name}.")
                raise

def validate_response(response, card_id):
    """Validate API response and return JSON data"""
    try:
        data = response.json()
        if not data:
            print(f"⚠️ Warning: Card {card_id} returned empty data")
            return None
        return data
    except ValueError:
        print(f"❌ Invalid JSON response from card {card_id}")
        return None

# -------------------- MONTH REPLACEMENTS --------------------
MONTH_REPLACEMENTS = {
    'Data Science Certification - December 2022': '2022 12 Dec',
    'Data Science Certification - January 2023': '2023 01 Jan',
    'Data Science Certification  - February 2023': '2023 02 Feb',
    'Data Science Certification  - March 2023': '2023 03 Mar',
    'Professional Certificate Course In Data Science - April 2023': '2023 04 April',
    'Professional Certificate Course In Data Science - September 2023': '2023 09 Sept',
    'Professional Certificate Course In Data Science - June 2023': '2023 06 June',
    'Professional Certificate Course In Data Science - July 2023': '2023 07 July',
    'Professional Certificate Course In Data Science - August 2023': '2023 08 Aug',
    'Professional Certificate Course In Data Science - May 2023': '2023 05 May',
    'Professional Certificate Course In Data Science - October 2023': '2023 10 Oct',
    'Professional Certificate Course In Data Science - November 2023': '2023 11 Nov',
    'Professional Certificate Course In Data Science - December 2023': '2023 12 Dec',
    'Professional Certificate Course In Data Science - January 2024': '2024 13 Jan',
    'Professional Certificate Course In Data Science - February 2024': '2024 14 Feb',
    'Professional Certificate Course In Data Science - March 2024': '2024 15 March',
    'Professional Certificate Course In Data Science - April 2024': '2024 16 April',
    'Professional Certificate Course In Data Science - May 2024': '2024 17 May',
    'Professional Certificate Course In Data Science - June 2024': '2024 18 June',
    'Professional Certificate Course In Data Science - July 2024': '2024 19 July',
    'Professional Certificate Course In Data Science - August 2024': '2024 20 Aug',
    'Professional Certificate Course In Data Science - September 2024': '2024 21 Sept',
    'Professional Certificate Course In Data Science - October 2024': '2024 22 Oct',
    'Certification in Advance Software Development October 2024': '2024 22 Oct ASD',
    'Professional Certificate Course In Data Science - November 2024': '2024 23 Nov',
    'Professional Certificate Course In Data Science - December 2024': '2024 24 Dec',
    'Professional Certificate Course In Data Science - January 2025': '2025 25 Jan',
    'Professional Certificate Course In Data Science - February 2025': '2025 26 Feb',
    'Professional Certificate Course In Data Science - March 2025': '2025 27 March',
    'Professional Certificate Course In Data Science - April 2025': '2025 28 April',
    'Professional Certificate Course In Data Science - May 2025': '2025 29 May',
    'Professional Certificate Course In Data Science & AI - June 2025': '2025 30 June',
    'Professional Certificate Course In Data Science - July 2025': '2025 31 July',
    'Professional Certificate Course In Data Science August 2025': '2025 32 August',
    'Professional Certificate Course In Data Science September 2025': '2025 33 September',
    'Professional Certificate Course In Data Science October 2025': '2025 34 October',
    'Professional Certificate Course In Data Science November 2025': '2025 35 November',
    'Professional Certificate Course In Data Science December 2025': '2025 36 December',
    'Professional Certificate Course In Data Science January 2026': '2026 37 January',
    'Professional Certificate Course In Data Science February 2026': '2026 38 Febraury',
    'Professional Certificate Course In Data Science March 2026': '2026 39 March',
    'Professional Certificate Course In Data Science April 2026': '2026 40 April',
    'Professional Certificate Course In Data Science May 2026' : '2026 41 May',
    'Professional Certificate Course In Data Science June 2026' : '2026 42 June',
    'Professional Certificate Course In Data Science July 2026' : '2026 43 July',
    'Professional Certificate Course In Data Science August 2026' : '2026 44 August',
    'DS Xcelerate AU':'DS Xcelerate',
    'ASD Xcelerate AU':'ASD Xcelerate',
    'Agentic AI AU':'Agentic AI',
    'Agentic AI AU - March 2026':'Agentic AI AU - March 2026',
    'Agentic AI AU - April 2026':'Agentic AI AU - April 2026' , 
    'Agentic AI AU - May 2026':'Agentic AI AU - May 2026',
    'Agentic Generalist Course':'Agentic Generalist Course',
    'Agentic AI AU - June 2026':'Agentic AI AU - June 2026',
    'Agentic AI Generalist AU - June 2026':'Agentic AI Generalist AU - June 2026', 
    'Agentic AI AU - July 2026':'Agentic AI AU - July 2026',
    'Agentic AI AU - August 2026':'Agentic AI AU - August 2026',
    'Agentic AI - Generalist AU - Aug 2026':'Agentic AI - Generalist AU - Aug 2026',
    'Agentic AI AU - September 2026':'Agentic AI AU - September 2026'
}

def apply_month_replacements(series):
    """Apply month name replacements to a pandas Series"""
    def replace_month(match):
        return MONTH_REPLACEMENTS.get(match.group(0), match.group(0))
    # Sort keys longest-first so specific batch names (e.g. 'Agentic AI AU - March 2026')
    # match before their shorter prefixes (e.g. 'Agentic AI AU'). Regex alternation is
    # leftmost-match, not longest-match, so without this the prefix wins and truncates
    # the value, causing it to be dropped by the batches_to_retain filter.
    pattern = '|'.join(sorted(map(re.escape, MONTH_REPLACEMENTS.keys()), key=len, reverse=True))
    return series.str.replace(pattern, replace_month, regex=True)

# -------------------- SECTION 1: NPS --------------------
def run_nps():
    print("\n📌 Running: NPS")
    
    try:
        r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/9452/query/json')
        data = validate_response(r, '9452')
        if not data:
            print("⚠️ Card 9452 returned NO data — skipping write. Check the card in Metabase.")
            return
        
        df = pd.DataFrame(data)
        print(f"📊 Fetched {len(df)} NPS records")
        
        df = df.rename(columns={'course_name': 'Batch'})
        df['admin_unit_name'] = apply_month_replacements(df['admin_unit_name'])

        batches_to_retain = [
            '2025 25 Jan', '2025 26 Feb', '2025 27 March', '2025 28 April',
            '2025 29 May', '2025 30 June', '2025 31 July', '2025 32 August',
            '2025 33 September', '2025 34 October', '2025 35 November',
            '2025 36 December', '2026 37 January', '2026 38 Febraury','Agentic AI','2026 39 March','2026 40 April','2026 41 May','2026 42 June',
            '2026 43 July','Agentic AI AU - March 2026','Agentic AI AU - April 2026', 'Agentic AI AU - May 2026','Agentic Generalist Course',
            'Agentic AI AU - June 2026','Agentic AI Generalist AU - June 2026', 'Agentic AI AU - July 2026','Agentic AI AU - August 2026',
            'Agentic AI - Generalist AU - Aug 2026','Agentic AI AU - September 2026'
        ]

        # --- OPTIONAL DIAGNOSTIC: batch names present in data but missing from retain list ---
        # unmatched = set(df['admin_unit_name'].dropna().unique()) - set(batches_to_retain)
        # print("Batches present but NOT in retain list:", sorted(unmatched))

        df = df[df['admin_unit_name'].isin(batches_to_retain)]
        df['form_fill_date'] = pd.to_datetime(df['form_fill_date'])

        def get_month_bucket(date):
            if pd.isnull(date):
                return None
            month_abbr = date.strftime('%b')
            year = date.year
            if date.day <= 15:
                return f"{month_abbr}-{year} (1-15)"
            else:
                last_day = calendar.monthrange(year, date.month)[1]
                return f"{month_abbr}-{year} (16-{last_day})"

        def categorize_nps(rating):
            if pd.isna(rating):
                return None
            try:
                rating = float(rating)
            except (ValueError, TypeError):
                return None
            if rating >= 9:
                return "Promoter"
            elif rating >= 7:
                return "Passive"
            else:
                return "Detractor"

        def determine_sentiment(current_category, previous_category):
            if pd.isna(current_category) or pd.isna(previous_category):
                return "No Change"
            if current_category == previous_category:
                return "No Change"
            transitions = {
                ("Promoter", "Passive"): "Promoter → Passive",
                ("Promoter", "Detractor"): "Promoter → Detractor",
                ("Passive", "Promoter"): "Passive → Promoter",
                ("Detractor", "Promoter"): "Detractor → Promoter",
                ("Passive", "Detractor"): "Passive → Detractor",
                ("Detractor", "Passive"): "Detractor → Passive",
            }
            return transitions.get((previous_category, current_category), "No Change")

        df['month_bucket'] = df['form_fill_date'].apply(get_month_bucket)
        df = df.sort_values(['user_id', 'form_fill_date'])
        df['current_nps_category'] = df['nps_rating'].apply(categorize_nps)
        df['previous_nps_rating'] = df.groupby('user_id')['nps_rating'].shift(1)
        df['previous_nps_category'] = df['previous_nps_rating'].apply(categorize_nps)
        df['sentiment'] = df.apply(
            lambda row: determine_sentiment(row['current_nps_category'], row['previous_nps_category']), axis=1
        )
        df.loc[df['previous_nps_category'].isna(), 'sentiment'] = 'No Previous'

        write_sheet(SHEET_NPS, "NPS_NEW", df)
        
    except Exception as e:
        print(f"❌ Error in run_nps: {e}")
        traceback.print_exc()
        raise

# -------------------- SECTION 2: PROJECTS VIEW --------------------
def run_projects_view():
    print("\n📌 Running: Projects View")
    
    try:
        r1 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6959/query/json')
        time.sleep(1)
        r2 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6960/query/json')
        time.sleep(1)
        r3 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6289/query/json')

        data1 = validate_response(r1, '6959')
        data2 = validate_response(r2, '6960')
        data3 = validate_response(r3, '6289')
        
        if not data1 or not data2 or not data3:
            print("⚠️ One or more API responses are empty")
            return

        df1 = pd.DataFrame(data1)
        df2 = pd.DataFrame(data2)
        df3 = pd.DataFrame(data3)[['user_id', 'au_batch_name', 'label','au_start_date']]
        
        print(f"📊 df1: {len(df1)}, df2: {len(df2)}, df3: {len(df3)}")

        concatenated_df = pd.concat([df1, df2], axis=0, ignore_index=True)
        concatenated_df = concatenated_df.drop(concatenated_df.index[0]).reset_index(drop=True)

        screened_df = concatenated_df[concatenated_df['User ID'].isin(df3['user_id'])]
        screened_df = screened_df.rename(columns={'User ID': 'user_id'})
        screened_df_1 = pd.merge(screened_df, df3, on='user_id')
        screened_df_1['au_batch_name'] = apply_month_replacements(screened_df_1['au_batch_name'])

        write_sheet(SHEET_PROJECTS_MAIN, "Projects_view", screened_df_1)

        screened_df = concatenated_df[concatenated_df['User ID'].isin(df3['user_id'])]
        screened_df = screened_df.rename(columns={'User ID': 'user_id'})
        concatenated_df = pd.merge(screened_df, df3, on='user_id')

        # Also update Projects-1 (full concatenated with datetime cols)
        
        concatenated_df['Submission Time'] = pd.to_datetime(concatenated_df['Submission Time'])
        concatenated_df['latest_feedback_given_time'] = pd.to_datetime(concatenated_df['latest_feedback_given_time'])
        concatenated_df['project_deadline_date'] = pd.to_datetime(concatenated_df['project_deadline_date'])
        concatenated_df['latest_feedback_month_year'] = concatenated_df['latest_feedback_given_time'].dt.strftime('%B %Y')

        # # Also update Projects-1 (full concatenated with datetime cols)
        # concatenated_df['Submission Time'] = pd.to_datetime(concatenated_df['Submission Time'], utc=True).dt.tz_convert('Asia/Kolkata')
        # concatenated_df['latest_feedback_given_time'] = pd.to_datetime(concatenated_df['latest_feedback_given_time'], utc=True).dt.tz_convert('Asia/Kolkata')
        # concatenated_df['project_deadline_date'] = pd.to_datetime(concatenated_df['project_deadline_date'], utc=True).dt.tz_convert('Asia/Kolkata')

        def get_re_evaluation_flag(row):
            submission_time = row['Submission Time']
            latest_feedback = row['latest_feedback_given_time']
            submission_status = row['Submission Status']
            marks_obtained = row['marks_obtained']
        
            if pd.notna(submission_time) and pd.notna(latest_feedback) and submission_time > latest_feedback:
                return 1
            elif pd.notna(submission_time) and pd.isna(latest_feedback) and submission_status == 'Submitted' and marks_obtained == 0:
                return 2
            else:
                return 0
        
        concatenated_df['re_evaluation_flag'] = concatenated_df.apply(get_re_evaluation_flag, axis=1)

        write_sheet(SHEET_PROJECTS_MAIN, "Projects-1", concatenated_df)
        
    except Exception as e:
        print(f"❌ Error in run_projects_view: {e}")
        traceback.print_exc()
        raise

# -------------------- SECTION 3: LECTURES DATA --------------------
def run_lectures():
    print("\n📌 Running: Lectures Data")
    
    try:
        r1 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6031/query/json')
        time.sleep(1)
        r2 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8646/query/json')

        data1 = validate_response(r1, '6031')
        data2 = validate_response(r2, '8646')
        
        if not data1 or not data2:
            print("⚠️ One or more API responses are empty")
            return

        df  = pd.DataFrame(data1)
        df1 = pd.DataFrame(data2)
        
        print(f"📊 df: {len(df)}, df1: {len(df1)}")

        df2 = pd.merge(df, df1, on=['lecture_id', 'lecture_date', 'course_name'], how='inner')
        print(f"📊 After merge: {len(df2)}")
        
        df2 = df2.fillna(0)
        df2 = df2.dropna(subset=['module_name'])

        phase_2_modules = ['DS 05 Python', 'DS 06 EDA 1', 'DS 07 EDA 2']
        phase_3_modules = ['DS 08 ML 1 (old)', 'DS 09 ML 2', 'DS MLOPS']
        df2['module_group'] = df2['module_name'].apply(
            lambda x: 'Phase 2' if x in phase_2_modules
                      else 'Phase 3' if x in phase_3_modules
                      else x
        )
        df2 = df2.rename(columns={'course_name': 'Batch', 'module_name': 'Module'})
        write_sheet(SHEET_BATCH_REPORT, "Batch_Report", df2)
        
    except Exception as e:
        print(f"❌ Error in run_lectures: {e}")
        traceback.print_exc()
        raise

# -------------------- SECTION 4: ASSIGNMENT QUESTIONS BUCKET --------------------
def run_assignment_questions_bucket():
    print("\n📌 Running: Assignment Questions Bucket")
    
    try:
        r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/7939/query/json')
        data = validate_response(r, '7939')
        if not data:
            return
        
        df = pd.DataFrame(data)
        print(f"📊 Fetched {len(df)} records")
        
        df = df.fillna(0)
        df = df.rename(columns={'batch_name': 'Batch', 'module_name': 'Module'})
        write_sheet(SHEET_PROJECTS_MAIN, "Assignments_questions_bucket", df)
        
    except Exception as e:
        print(f"❌ Error in run_assignment_questions_bucket: {e}")
        traceback.print_exc()
        raise

# -------------------- SECTION 5: PROJECTS RAW --------------------
def run_projects_raw():
    print("\n📌 Running: Projects Raw")
    
    try:
        r1 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6241/query/json')
        time.sleep(1)
        r2 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6242/query/json')
        time.sleep(1)
        r3 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6289/query/json')

        data1 = validate_response(r1, '6241')
        data2 = validate_response(r2, '6242')
        data3 = validate_response(r3, '6289')
        
        if not data1 or not data2 or not data3:
            print("⚠️ One or more API responses are empty")
            return

        df1 = pd.DataFrame(data1)
        df2 = pd.DataFrame(data2)
        df3 = pd.DataFrame(data3)[['user_id', 'au_batch_name', 'label']]
        
        print(f"📊 df1: {len(df1)}, df2: {len(df2)}, df3: {len(df3)}")

        concatenated_df = pd.concat([df1, df2], axis=0, ignore_index=True)
        concatenated_df = concatenated_df.rename(columns={'User ID': 'user_id'})

        screened_df_1 = pd.merge(concatenated_df, df3, on='user_id', how='left')
        screened_df_1['Submission Time'] = pd.to_datetime(screened_df_1['Submission Time'])
        screened_df_1['latest_feedback_given_time'] = pd.to_datetime(screened_df_1['latest_feedback_given_time'])
        screened_df_1['project_deadline_date'] = pd.to_datetime(screened_df_1['project_deadline_date'])

        write_sheet(SHEET_PROJECTS_MAIN, "Projects", screened_df_1)
        
    except Exception as e:
        print(f"❌ Error in run_projects_raw: {e}")
        traceback.print_exc()
        raise

# -------------------- SECTION 6: PROJECT EVALUATIONS --------------------
def run_project_evaluations():
    print("\n📌 Running: Project Evaluations")
    
    try:
        r1 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6578/query/json')
        time.sleep(1)
        r2 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6579/query/json')

        data1 = validate_response(r1, '6578')
        data2 = validate_response(r2, '6579')
        
        if not data1 or not data2:
            print("⚠️ One or more API responses are empty")
            return

        df1 = pd.DataFrame(data1)
        df2 = pd.DataFrame(data2)
        
        print(f"📊 df1: {len(df1)}, df2: {len(df2)}")
        
        df = pd.concat([df1, df2], axis=0, ignore_index=True)

        df['Submission Time'] = pd.to_datetime(df['Submission Time'])
        df['feedback_given_time'] = pd.to_datetime(df['feedback_given_time'])

        filtered_df = df[~(
            (df['feedback_given_time'].isnull() | (df['feedback_given_time'] == '')) &
            (df['Evaluation Status'] == 'Evaluated')
        )]
        filtered_df = filtered_df.drop_duplicates(subset='submission_id')
        filtered_df = filtered_df.rename(columns={'User ID': 'user_id'})

        write_sheet(SHEET_EVALUATIONS, "Project_evaluations", filtered_df)
        
    except Exception as e:
        print(f"❌ Error in run_project_evaluations: {e}")
        traceback.print_exc()
        raise

# -------------------- SECTION 7: LECTURE QUALITY --------------------
def run_lecture_quality():
    print("\n📌 Running: Lecture Quality")
    
    try:
        r1 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/8646/query/json')
        time.sleep(1)
        r2 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/9192/query/json')
        time.sleep(1)
        r3 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/9166/query/json')

        data1 = validate_response(r1, '8646')
        data2 = validate_response(r2, '9192')
        data3 = validate_response(r3, '9166')
        
        if not data1 or not data2 or not data3:
            print("⚠️ One or more API responses are empty")
            return

        df1 = pd.DataFrame(data1)
        df2 = pd.DataFrame(data2)
        
        print(f"📊 df1: {len(df1)}, df2: {len(df2)}")
        
        # Validate merge keys
        if 'lecture_id' not in df1.columns or 'lecture_id' not in df2.columns:
            print("❌ Missing 'lecture_id' column for merge")
            return
        if 'lecture_date' not in df1.columns or 'lecture_date' not in df2.columns:
            print("❌ Missing 'lecture_date' column for merge")
            return

        df3 = pd.merge(df1, df2, on=['lecture_id', 'lecture_date'], how='inner')
        print(f"📊 After first merge: {len(df3)}")
        
        if df3.empty:
            print("⚠️ No matching records after merge")
            return
        
        df4 = df3.rename(columns={'course_name': 'Batch', 'lecture_date': 'date'})

        df_in_class = pd.DataFrame(data3)
        df_in_class = df_in_class.rename(columns={'batch_name': 'Batch', 'release_date': 'date'})

        df5 = pd.merge(df4, df_in_class, on=['Batch', 'date'], how='left')
        print(f"📊 Final shape: {len(df5)}")
        
        write_sheet(SHEET_BATCH_REPORT, "Lecture_Quality", df5)
        
    except Exception as e:
        print(f"❌ Error in run_lecture_quality: {e}")
        traceback.print_exc()
        raise

# -------------------- SECTION 8: LECTURE SUBJECTIVE FEEDBACK --------------------
def run_lecture_subjective_feedback():
    print("\n📌 Running: Lecture Subjective Feedback")
    
    try:
        r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/5037/query/json')
        data = validate_response(r, '5037')
        if not data:
            return
        
        df = pd.DataFrame(data)
        print(f"📊 Fetched {len(df)} records")
        
        write_sheet(SHEET_LECTURE_SUBJ, "Lecture_Subjective_Feedback", df)
        
    except Exception as e:
        print(f"❌ Error in run_lecture_subjective_feedback: {e}")
        traceback.print_exc()
        raise

# -------------------- SECTION 9: MENTOR SESSIONS --------------------
def run_mentor_sessions():
    print("\n📌 Running: Mentor Sessions")
    
    try:
        r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6161/query/json')
        data = validate_response(r, '6161')
        if not data:
            return
        
        df = pd.DataFrame(data)
        print(f"📊 Fetched {len(df)} records")
        
        df = df.rename(columns={'batch': 'Batch'})
        df['week_view'] = pd.to_datetime(df['week_view'])
        write_sheet(SHEET_MENTOR, "Mentor_sessions", df)
        
    except Exception as e:
        print(f"❌ Error in run_mentor_sessions: {e}")
        traceback.print_exc()
        raise

# -------------------- SECTION 10: MENTOR GROUP SESSIONS --------------------
def run_mentor_group_sessions():
    print("\n📌 Running: Mentor Group Sessions")
    
    try:
        r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6184/query/json')
        data = validate_response(r, '6184')
        if not data:
            return
        
        df = pd.DataFrame(data)
        print(f"📊 Fetched {len(df)} records")
        
        df = df.rename(columns={'batch': 'Batch', 'Mentor Name': 'mentor_name'})
        write_sheet(SHEET_MENTOR, "Mentor_group_sessions", df)
        
    except Exception as e:
        print(f"❌ Error in run_mentor_group_sessions: {e}")
        traceback.print_exc()
        raise

# -------------------- SECTION 11: MENTOR SLOTS + MENTOR BATCH --------------------
def run_mentor_slots_and_batch():
    print("\n📌 Running: Mentor Slots & Mentor-Batch")
    
    try:
        r3 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/7019/query/json')
        time.sleep(1)
        r6 = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/7941/query/json')
        
        data3 = validate_response(r3, '7019')
        data6 = validate_response(r6, '7941')
        
        if not data3 or not data6:
            print("⚠️ One or more API responses are empty")
            return
        
        df3 = pd.DataFrame(data3)
        df6 = pd.DataFrame(data6)
        
        print(f"📊 df3: {len(df3)}, df6: {len(df6)}")
        
        df3['date'] = pd.to_datetime(df3['date'])
        write_sheet(SHEET_MENTOR, "Mentor_slots", df3)

        df5 = pd.merge(df6, df3, on=['mentor_id', 'mentor_name'], how='left')
        df5 = df5.fillna(0)
        write_sheet(SHEET_MENTOR, "Mentor-Batch", df5)
        
    except Exception as e:
        print(f"❌ Error in run_mentor_slots_and_batch: {e}")
        traceback.print_exc()
        raise

# -------------------- SECTION 12: MENTOR CSAT --------------------
def run_mentor_csat():
    print("\n📌 Running: Mentor CSAT")
    
    try:
        r = mb_post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/card/6167/query/json')
        data = validate_response(r, '6167')
        if not data:
            return
        
        df = pd.DataFrame(data)
        print(f"📊 Fetched {len(df)} records")
        
        df = df.rename(columns={'batch': 'Batch', 'Mentor Name': 'mentor_name'})
        write_sheet(SHEET_MENTOR, "Mentor_CSAT", df)
        
    except Exception as e:
        print(f"❌ Error in run_mentor_csat: {e}")
        traceback.print_exc()
        raise

# -------------------- MAIN: RUN ALL --------------------
if __name__ == "__main__":
    print("🚀 Starting Data Pipeline Automation...")
    print(f"📅 Start time: {datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%d-%b-%Y %H:%M:%S IST')}\n")

    tasks = [
        ("NPS",                         run_nps),
        ("Projects View",               run_projects_view),
        ("Lectures Data",               run_lectures),
        ("Assignment Questions Bucket", run_assignment_questions_bucket),
        ("Projects Raw",                run_projects_raw),
        ("Project Evaluations",         run_project_evaluations),
        ("Lecture Quality",             run_lecture_quality),
        ("Lecture Subjective Feedback", run_lecture_subjective_feedback),
        ("Mentor Sessions",             run_mentor_sessions),
        ("Mentor Group Sessions",       run_mentor_group_sessions),
        ("Mentor Slots & Batch",        run_mentor_slots_and_batch),
        ("Mentor CSAT",                 run_mentor_csat),
    ]

    success_count = 0
    failed_tasks = []

    for name, fn in tasks:
        try:
            fn()
            success_count += 1
        except Exception as e:
            print(f"❌ Error in {name}: {e}")
            failed_tasks.append(name)

    # Summary
    print("\n" + "="*60)
    print("📊 EXECUTION SUMMARY")
    print("="*60)
    print(f"✅ Successful: {success_count}/{len(tasks)}")
    if failed_tasks:
        print(f"❌ Failed: {len(failed_tasks)}")
        print(f"   Tasks: {', '.join(failed_tasks)}")
    
    current_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%b-%Y %H:%M:%S IST")
    print(f"\n📅 End time: {current_time}")

    end_time = time.time()
    mins, secs = divmod(end_time - start_time, 60)
    print(f"⏱️  Total execution time: {int(mins)}m {int(secs)}s")
    
    if failed_tasks:
        print("\n⚠️  Pipeline completed with errors")
    else:
        print("\n🎯 Data Pipeline Automation completed successfully!")
