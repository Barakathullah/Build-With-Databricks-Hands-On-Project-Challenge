import pandas as pd
import numpy as np
import hashlib
from datetime import datetime
import os
from datetime import datetime, timedelta

#1. Define the 90-day (3 months) range
end_date = datetime.now()
start_date = end_date - timedelta(days=90)

# 2. Helper function to generate random timestamps
def random_dates(start, end, n):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = np.random.randint(0, int_delta, n)
    return [start + timedelta(seconds=int(s)) for s in random_second]



# Helper for MD5
def generate_md5(val):
    return hashlib.md5(str(val).encode()).hexdigest()

# Configuration
NUM_USERS = 500000
NUM_REQ = 5000000
OUTPUT_DIR = "adtech_raw_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 1. user_profile
user_ids = [f"U_{i:05d}" for i in range(NUM_USERS)]
user_profile = pd.DataFrame({
    "user_id": user_ids,
    "age_range": np.random.choice(["below_18", "18-24", "25-34", "35-44", "45-54", "55+", None], NUM_USERS),
    "gender": np.random.choice(["m", "f", "MALE", "FEMALE", None], NUM_USERS),
    "state": np.random.choice(["KA", "TN", "MH", "DL", "UP", "ka", None], NUM_USERS),
    "city": np.random.choice(["Bangalore", "Chennai", "Mumbai", "Delhi", "Lucknow", ""], NUM_USERS),
    "phone_price_range": np.random.choice(["low", "mid", "high", None], NUM_USERS),
    "phone_model": np.random.choice(["iPhone15", "SamsungS23", "RedmiNote", "Pixel8"], NUM_USERS),
    "language": np.random.choice(["English", "Hindi", "Tamil", "Telugu"], NUM_USERS)
})

# 2. user_app_genre
user_app_genre = pd.DataFrame({
    "user_id": user_ids,
    "app_id": [f"APP_{np.random.randint(100, 999)}" for _ in range(NUM_USERS)],
    "app_cat": np.random.choice(["Social", "Utility", "Games"], NUM_USERS),
    "primary_genre": np.random.choice(["Videos", "News", "Shopping"], NUM_USERS),
    "secondary_genre": np.random.choice(["Education", "Finance", "Lifestyle"], NUM_USERS)
})

# 3. Campaigns (UPDATED: 199 Campaigns, 70 Advertisers, Premium CPI Rates)
num_camps = 199
campaign_ids = [f"C_{i}" for i in range(1, num_camps + 1)]
advertiser_ids = [f"ADV_{np.random.randint(1, 71)}" for _ in range(num_camps)]
campaign_types = np.random.choice(["CPC", "CPM", "CPI"], num_camps)

def get_billing_rate(c_type):
    if c_type == "CPI":
        return np.random.choice([100.0, 120.0, 300.0]) # Premium install rates
    elif c_type == "CPC":
        return np.round(np.random.uniform(1.5, 5.0), 2)
    else: # CPM
        return np.round(np.random.uniform(5.0, 25.0), 2)

billing_rates = [get_billing_rate(t) for t in campaign_types]

# Targeting params for variety
targeting = [np.random.choice(['{"state":["KA","TN"], "phone_price_range":["low"]}', 
                               '{"state":["MH"], "gender":["f"]}', '{}'], p=[0.2, 0.2, 0.6]) for _ in range(num_camps)]

campaigns = pd.DataFrame({
    "campaign_id": campaign_ids,
    "advertiser_id": advertiser_ids,
    "campaign_type": campaign_types,
    "billing_rate": billing_rates,
    "targeting_params": targeting
})

# 4. REQUEST
req_user_ids = np.random.choice(user_ids, NUM_REQ)
requests = pd.merge(pd.DataFrame({"user_id": req_user_ids}), user_profile, on="user_id", how="left")
requests["request_id"] = [f"REQ_{i:06d}" for i in range(len(requests))]
requests["id_md5"] = requests["request_id"].apply(generate_md5)
#requests["timestamp"] = datetime.now()
requests["timestamp"] = random_dates(start_date, end_date, len(requests))
requests["device_type"] = np.random.choice(["Mobile", "Tablet", None], NUM_REQ)
requests["network_type"] = np.random.choice(["4G", "5G", "WiFi"], NUM_REQ)
requests["has_request"] = 1

# Inject duplicates for cleaning exercise
duplicates = requests.sample(n=100)
requests = pd.concat([requests, duplicates], ignore_index=True)

# 5. IMPRESSIONS
impressions_df = requests.sample(frac=0.5).copy()
impressions_df["impression_id"] = [f"IMP_{i:06d}" for i in range(len(impressions_df))]
impressions_df["campaign_id"] = np.random.choice(list(campaigns["campaign_id"]) + ["C_999", None], len(impressions_df))
impressions_df["ad_position"] = np.random.choice(["top", "interstitial", "bottom"], len(impressions_df))
impressions_df = pd.merge(impressions_df, campaigns[["campaign_id", "campaign_type"]], on="campaign_id", how="left")
impressions_df["has_impression"] = 1
impressions = impressions_df[["impression_id", "id_md5", "campaign_id", "ad_position", "campaign_type", "has_impression"]]

# 6. CLICKS
clicks_df = impressions_df.dropna(subset=['campaign_type']).sample(frac=0.2).copy()
clicks_df["click_id"] = [f"CLK_{i:06d}" for i in range(len(clicks_df))]
clicks_df["has_click"] = 1
# Inject 20 duplicate clicks
click_duplicates = clicks_df.sample(n=20)
clicks_df = pd.concat([clicks_df, click_duplicates], ignore_index=True)
clicks = pd.merge(clicks_df[["id_md5", "click_id", "has_click"]], requests[["id_md5", "user_id"]], on="id_md5", how="left").drop_duplicates(subset=['click_id'])

# Save to CSVs
for name, df in {"user_profile": user_profile, "user_app_genre": user_app_genre, "campaigns": campaigns, 
                 "requests": requests, "impressions": impressions, "clicks": clicks}.items():
    df.to_csv(f"{OUTPUT_DIR}/{name}.csv", index=False)
