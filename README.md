# 🐿️ DSI321: Big Data Infrastructure project
Real-Time Weather Data Pipeline with Visualization

✏️ **By**: Rittida Yingnaikiat

📕 **Student ID**: 6524651350

🔎 **Update Date**: 27 May 2025


# 🚀 Introduction

Air quality has become an increasingly critical issue worldwide because it significantly affects public health and the environment. Among the many air pollutants, PM2.5 particles, which are smaller than 2.5 micrometers, are especially harmful. These tiny particles can penetrate deep into the lungs and even enter the bloodstream, causing respiratory and heart diseases. Therefore, monitoring and analyzing PM2.5 levels is essential to protect people's health and support environmental policies.

In this project, we work with near real-time air quality data collected from multiple automatic monitoring stations spread across various provinces in Thailand. The dataset includes hourly PM2.5 measurements along with important details such as timestamps and station locations. This data is provided by the Air4Thai network through an API maintained by the Pollution Control Department, ensuring that the information is reliable and regularly updated. To make the entire process smooth and scalable, we use Prefect.io to automate the data workflows and Docker to containerize the system, allowing it to run consistently across different environments.

>🪄 Main goal is to build an interactive dashboard using Streamlit that helps users easily understand trends in air pollution, identify areas with good or poor air quality, and recommend provinces with the best conditions for outdoor activities.

# 🚀 Dashbord using Steamlit
In this project, we designed an interactive dashboard to help users explore air quality data more intuitively. The dashboard includes several components that allow users to filter and analyze PM2.5 levels across different provinces and time periods in Thailand.
<p align="center">
  <img width="700" alt="Screenshot 2568-05-27 at 00 27 47" src="https://github.com/user-attachments/assets/9f1be73c-2f00-457c-ba2c-e46e39416d11" />
  <img width="700" alt="Screenshot 2568-05-27 at 00 27 56" src="https://github.com/user-attachments/assets/3e36f78a-3c58-4aab-965a-cb2a868f9ab3" />
</p>

## 🚪 Sidebar Filters
The dashboard features a sidebar that acts as a control panel for filtering data. Users can adjust the following:

🗓️ **Start Date and End Date**: These allow users to define a custom time range for the data they want to explore.

🗺️ **Provinces**: Users can select one or more provinces to focus on. By default, the dashboard includes data from all provinces, but users can narrow down their view to specific locations as needed.

These filters dynamically affect two key components in the dashboard:
- PM2.5 Trends by Provinces (line chart)
- Top 10 Stations with Highest PM2.5 (card view)
Other dashboard elements remain unaffected, as they provide a broader, nationwide overview.

<p align="center">
  <img width="200" alt="Screenshot 2568-05-27 at 00 18 01" src="https://github.com/user-attachments/assets/c8c5d38f-8515-4444-b82a-67ab8e3abb60" />
</p>

## ⚡️ Scorecards
The main dashboard contains three scorecards that offer quick insights into the current air quality:
<p align="center">
  <img width="500" alt="Screenshot 2568-05-27 at 00 36 53" src="https://github.com/user-attachments/assets/d45c7405-2aec-4120-b6e4-46edf8ba3bb4" />
</p>

1. **Number of Stations with Good Air Quality**: This scorecard shows how many monitoring stations are currently reporting good air quality, based on the WHO standard: PM2.5 (AQI) < 25 µg/m³.

3. **Number of Provinces with Good Air Quality**: Similar to the previous metric, but aggregated at the provincial level. A province is considered to have good air quality if its PM2.5 average is below the same WHO threshold.

5. **National PM2.5 Average (Compared to Yesterday)**: This scorecard displays the current national average PM2.5 value and shows whether it is higher or lower compared to the previous day, along with the difference in µg/m³.

## 🌫️ PM2.5 Trends by Provinces
This line chart visualizes PM2.5 trends over time for the selected provinces. The chart adjusts based on the start and end dates chosen by the user:

💥 If the start and end dates are different, the graph displays daily trends.
<p align="center">
  <img width="600" alt="Screenshot 2568-05-27 at 01 11 27" src="https://github.com/user-attachments/assets/d074bf12-15cc-4d04-85cc-73e018cd3fdb" />
</p>
💥 If the start and end dates are the same, it switches to show hourly trends for that particular day.
<p align="center">
  <img width="600" alt="Screenshot 2568-05-27 at 01 13 27" src="https://github.com/user-attachments/assets/0841bcf8-9c57-46f9-8af8-54940ae4b9a2" />
</p>
This allows users to easily detect changes in air quality over time, whether on a daily or hourly basis.

## ☄️ Top 10 Stations with Highest PM2.5
This section is a card-based view that lists the 10 monitoring stations currently reporting the highest PM2.5 levels. The data updates based on the applied province filters and reflects the most recent data available.
<p align="center">
  <img width="600" alt="Screenshot 2568-05-27 at 01 19 15" src="https://github.com/user-attachments/assets/7f06630e-5f3e-4b2f-b32b-2d45adc19252" />
</p>

## Machine Learning

**🌤️ Recommender System: Best Provinces to Go Outside (K-Means Clustering)**
<p align="center">
  <img width="432" alt="Screenshot 2568-05-27 at 01 24 12" src="https://github.com/user-attachments/assets/e580eb99-3fb4-4ad1-bb18-5e59182178a4" />
</p>

To assist users in choosing the best locations for outdoor activities, we integrated an unsupervised machine learning model using K-Means Clustering. This module identifies the Top 5 provinces with the best air quality in the last 7 days. Here's how it works:

### 1. Data Preparation
We calculate the 7-day average PM2.5 for each province. This gives us a simplified dataset where each province is represented by a single numerical value (one feature: PM2.5 level).
### 2. Model Training
We apply K-Means clustering (using scikit-learn) to group provinces into 3 clusters based on their PM2.5 levels.
Each cluster can be characterized as follows:

✅ **Cluster 0** — Best Air Quality
- This group contains provinces with the lowest average PM2.5 levels.
- It typically includes areas with consistently clean air over the past several days.
- We use this cluster as the recommended group for outdoor activities.

⚠️ **Cluster 1** — Moderate Air Quality
- Provinces in this group have moderate PM2.5 levels.
- The air quality might vary depending on local weather conditions or human activities.
- While not directly recommended, it is not considered hazardous either.

❌ **Cluster 2** — Poor Air Quality
- This cluster consists of provinces with the highest average PM2.5 levels.
- It often includes large cities or areas with persistent pollution issues.
- Outdoor activities should be avoided in these provinces during this period.

👉 The Top 5 recommended provinces displayed on the dashboard are selected exclusively from Cluster 0.

### 3. Province Grouping
The algorithm clusters provinces with similar air quality levels together. Each cluster reflects a different range of air conditions.
- We filter out all provinces that belong to Cluster 0, since this cluster represents the best air quality.
- These provinces are sorted by their 7-day average PM2.5 values in ascending order.
- Finally, the Top 5 provinces with the lowest PM2.5 levels within Cluster 0 are selected as the recommended provinces for outdoor activities.
- These recommended provinces are then displayed in the dashboard as a card titled: "✨ Top 5 Provinces in Best Air Quality (Last 7 Days)"

### 4. Cluster Analysis
We calculate the mean PM2.5 for each cluster and identify the cluster with the lowest average, indicating the best air quality.
### 5. Top 5 Recommendations
From the best-performing cluster, we select the Top 5 provinces with the lowest PM2.5 values and present them as recommendations for outdoor activities.
<p align="center">
  <img width="721" alt="Screenshot 2568-05-27 at 01 24 22" src="https://github.com/user-attachments/assets/74950fec-ac11-4b1f-a905-c1fb06ffb86a" />
</p>
