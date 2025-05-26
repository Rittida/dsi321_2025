import os
import pandas as pd
import numpy as np
import plotly.express as px
import matplotlib.pyplot as plt
import streamlit as st
import pyarrow.parquet as pq
import s3fs
import time
from zoneinfo import ZoneInfo
from datetime import timedelta, datetime
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Set up environments of LakeFS
lakefs_endpoint = os.getenv("LAKEFS_ENDPOINT", "http://lakefs-dev:8000")
ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY")
SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY")

# Setting S3FileSystem for access LakeFS
fs = s3fs.S3FileSystem(
    key=ACCESS_KEY,
    secret=SECRET_KEY,
    client_kwargs={'endpoint_url': lakefs_endpoint}
)

@st.cache_data()
def load_data():
    lakefs_path = "s3://air-quality/main/airquality.parquet/year=2025"
    data_list = fs.glob(f"{lakefs_path}/*/*/*/*")
    df_all = pd.concat([pd.read_parquet(f"s3://{path}", filesystem=fs) for path in data_list], ignore_index=True)
    
    df_all['lat'] = pd.to_numeric(df_all['lat'], errors='coerce')
    df_all['long'] = pd.to_numeric(df_all['long'], errors='coerce')
    df_all['timestamp'] = pd.to_datetime(df_all['timestamp'], errors='coerce')  # <-- เพิ่มตรงนี้
    df_all['year'] = df_all['year'].astype(int)
    df_all['month'] = df_all['month'].astype(int)
    df_all.drop_duplicates(inplace=True)
    df_all['PM25.aqi'] = df_all['PM25.aqi'].mask(df_all['PM25.aqi'] < 0, pd.NA)
    df_all['PM25.aqi'] = df_all.groupby('stationID')['PM25.aqi'].transform(lambda x: x.fillna(method='ffill'))
    
    df_all['province'] = df_all['areaEN'].str.extract(r",\s*([^,]+)$")[0].str.strip()  # <-- สร้าง province
    return df_all

def filter_data(df, start_date, end_date, provinces):
    df_filtered = df.copy()

    df_filtered = df_filtered[
        (df_filtered['timestamp'].dt.date >= start_date) &
        (df_filtered['timestamp'].dt.date <= end_date)
    ]

    # If the provinces list is empty, select all provinces
    if provinces and len(provinces) > 0:
        df_filtered = df_filtered[df_filtered['province'].isin(provinces)]

    df_filtered = df_filtered[df_filtered['PM25.aqi'] >= 0]

    return df_filtered

st.title("Air Quality Dashboard")
df_all = load_data()

# Sidebar settings
with st.sidebar:
    st.title("⚙️ Settings")

    max_date = df_all['timestamp'].max().date()
    min_date = df_all['timestamp'].min().date()
    default_start_date = min_date
    default_end_date = max_date

    start_date = st.date_input(
        "🗓️ Start date",
        default_start_date,
        min_value=min_date,
        max_value=max_date
    )

    end_date = st.date_input(
        "🗓️ End date",
        default_end_date,
        min_value=min_date,
        max_value=max_date
    )
    ALL_PROVINCES = "Select All"
    province_list = df_all['province'].dropna().unique().tolist()
    province_list.sort()
    province_options = [ALL_PROVINCES] + province_list

    selected_provinces = st.sidebar.multiselect(
        "🗺️ Select Province(s)",
        options=province_options,
        default=[ALL_PROVINCES]
    )

    if ALL_PROVINCES in selected_provinces:
        selected_provinces = province_list

df_filtered = filter_data(df_all, start_date, end_date, selected_provinces)

# Scorecard
today = pd.to_datetime(datetime.today().date())
df_today = df_all[pd.to_datetime(df_all['timestamp']).dt.date == today.date()]

## Define PM2.5 < 25 = "Good air quality"
good_air_df = df_today[df_today['PM25.aqi'] < 25]

## Calculate number of stations and province
num_good_stations = good_air_df['nameEN'].nunique()
num_good_provinces = good_air_df['province'].nunique()

## Show scorecard
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("🤩 Stations with Good Air Quality", f"{num_good_stations} Stations")

with col2:
    st.metric("🌈 Provinces with Good Air Quality", f"{num_good_provinces} Provinces")

with col3:
    today_avg = df_today['PM25.aqi'].mean()
    yesterday = today - pd.Timedelta(days=1)
    df_yesterday = df_all[df_all['timestamp'].dt.date == yesterday.date()]
    yesterday_avg = df_yesterday['PM25.aqi'].mean()

    st.metric("🍂 National Average PM2.5", f"{today_avg:.1f} µg/m³", delta=f"{(today_avg - yesterday_avg):+.1f} µg/m³")

# ML Part
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from sklearn.cluster import KMeans
import numpy as np

# สมมติ df_all คือ DataFrame ที่มีข้อมูล timestamp, province, PM25.aqi
# 1. แปลง datetime ให้ถูกต้อง
df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])

# 2. กรองข้อมูลช่วง 7 วันที่ผ่านมา
today = datetime.today()
seven_days_ago = today - timedelta(days=7)
last_7_days_df = df_all[df_all['timestamp'] >= seven_days_ago]

# 3. คำนวณค่าเฉลี่ย PM2.5 ต่อจังหวัดในช่วง 7 วันล่าสุด
province_avg_pm25 = last_7_days_df.groupby('province')['PM25.aqi'].mean().reset_index()

# 4. เตรียมข้อมูลสำหรับ K-Means
X = province_avg_pm25[['PM25.aqi']].values

# 5. สร้างโมเดล KMeans กับจำนวน cluster ที่เหมาะสม (เช่น 3)
kmeans = KMeans(n_clusters=3, random_state=42)
kmeans.fit(X)

# 6. เพิ่มคอลัมน์ cluster label ให้กับ DataFrame
province_avg_pm25['cluster'] = kmeans.labels_

# 7. หา cluster ที่มีค่าเฉลี่ย PM2.5 ต่ำสุด (อากาศดีที่สุด)
cluster_means = province_avg_pm25.groupby('cluster')['PM25.aqi'].mean()
best_cluster = cluster_means.idxmin()

# 8. เลือกจังหวัดในกลุ่มอากาศดีที่สุด
best_provinces_cluster = province_avg_pm25[province_avg_pm25['cluster'] == best_cluster]

# 9. เรียงลำดับจังหวัดใน cluster ตามค่า PM2.5 จากน้อยไปมาก และเลือก Top 5
top_5_best_provinces = best_provinces_cluster.nsmallest(5, 'PM25.aqi')

# 10. ใช้ session_state เก็บสถานะ toggle button
if 'show_recommend_kmeans' not in st.session_state:
    st.session_state.show_recommend_kmeans = False

# 11. สร้าง toggle button
if st.button("🌤️ Recommender: Best Provinces to Go Outside (K-Means)"):
    st.session_state.show_recommend_kmeans = not st.session_state.show_recommend_kmeans

# 12. แสดงผลเมื่อ toggle เป็น True
if st.session_state.show_recommend_kmeans:
    st.subheader("✨ Top 5 Provinces in Best Air Quality (Last 7 Days)")
    display_df = top_5_best_provinces.drop(columns=['cluster'])
    st.dataframe(
        display_df.rename(columns={'province': 'Province', 'PM25.aqi': 'Average PM2.5 (µg/m³)'}),
        use_container_width=True
    )

# Trend Line 
st.header("📈 PM2.5 Trends by Provinces")

## Use the df_filtered dataset filtered by selected date range and province
if df_filtered.empty:
    st.warning("🙅🏻‍♀️ Sorry, no PM2.5 data found for the selected dates or provinces.")
else:
    df_trend = df_filtered.copy()

    # Check whether a single day or multiple days are selected.
    if start_date == end_date:
        # Hourly trend
        df_trend['hour'] = df_trend['timestamp'].dt.hour
        df_trend_grouped = df_trend.groupby(['province', 'hour'])['PM25.aqi'].mean().reset_index()
        fig = px.line(
            df_trend_grouped,
            x='hour',
            y='PM25.aqi',
            color='province',
            markers=True,
            labels={'hour': 'Hours', 'PM25.aqi': 'PM2.5'},
            title='Hourly PM2.5 Trend Chart'
        )
    else:
        # Daily trend
        df_trend['date'] = df_trend['timestamp'].dt.date
        df_trend_grouped = df_trend.groupby(['province', 'date'])['PM25.aqi'].mean().reset_index()
        fig = px.line(
            df_trend_grouped,
            x='date',
            y='PM25.aqi',
            color='province',
            markers=True,
            labels={'date': 'Date', 'PM25.aqi': 'PM2.5'},
            title='Daily Average PM2.5 Trend'
        )

    fig.update_layout(
        xaxis_title=None,
        yaxis_title="PM2.5 (µg/m³)",
        legend_title="Provinces",
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)

# Card view setting (Top 10 PM2.5)
## Define a color function based on AQI values
def get_color(aqi):
    if pd.isna(aqi):
        return '#d3d3d3'  # สีเทาอ่อน สำหรับค่าไม่มี
    aqi = float(aqi)
    if aqi <= 25:
        return '#a8e05f'  # ดีมาก (เขียว)
    elif aqi <= 50:
        return '#fdd74b'  # ดี (เหลือง)
    elif aqi <= 100:
        return '#fe9b57'  # ปานกลาง (ส้ม)
    elif aqi <= 200:
        return '#fe6a69'  # เริ่มอันตราย (แดง)
    elif aqi <= 300:
        return '#a97abc'  # อันตรายมาก (ม่วง)
    else:
        return '#a87383'  # อันตรายสูง (เทาเข้ม)

df_all = load_data()

st.header("🚨 Top 10 Stations with Highest PM2.5")
## Filter data within the selected date range
df_all['timestamp'] = pd.to_datetime(df_all['timestamp'], errors='coerce')
df_all['date'] = df_all['timestamp'].dt.date
df_all['hour'] = df_all['timestamp'].dt.hour

mask = (df_all['date'] >= start_date) & (df_all['date'] <= end_date)
filtered_df = df_all[mask].dropna(subset=['PM25.aqi'])

## Filter by province if the selected option is not 'All'
if selected_provinces and len(selected_provinces) > 0:
    filtered_df = filtered_df[filtered_df['province'].isin(selected_provinces)]
else:
    filtered_df = filtered_df  # if select 'all'

## Identify the Top 10 stations with the highest PM2.5 levels during this period
top10 = (
    filtered_df.groupby(['stationID', 'nameEN'])['PM25.aqi']
    .max()
    .reset_index()
    .sort_values(by='PM25.aqi', ascending=False)
    .head(10)
)

## Fetch the most recent data for the Top 10 stations to display in cards
latest_rows = filtered_df[filtered_df['stationID'].isin(top10['stationID'])]
latest_rows = latest_rows.sort_values('timestamp').drop_duplicates('stationID', keep='last')

cols = st.columns(3)
for i, (_, row) in enumerate(latest_rows.iterrows()):
    col = cols[i % 3]
    with col:
        station = row['nameEN']
        aqi = row['PM25.aqi']
        updated_time = row['timestamp'].strftime("%H:%M")
        updated_date = row['timestamp'].date()
        color = get_color(aqi)
        st.markdown(f"""
        <div style="
            background-color:{color};
            padding:16px;
            border-radius:16px;
            margin:10px 0;
            color:#000;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
            <h4>{station}</h4>
            <p><strong>PM2.5:</strong> {aqi:.1f} µg/m³</p>
            <p style="font-size: 12px; opacity: 0.6;">อัปเดตเวลา {updated_time} | วันที่ {updated_date}</p>
        </div>
        """, unsafe_allow_html=True)
