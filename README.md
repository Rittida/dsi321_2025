# 🐿️ DSI321: Big Data Infrastructure project
Real-Time Weather Data Pipeline with Visualization

✏️ **Name**: Rittida Yingnaikiat

📕 **Student ID**: 6524651350

# Introduction

Air quality has become an increasingly critical issue worldwide because it significantly affects public health and the environment. Among the many air pollutants, PM2.5 particles, which are smaller than 2.5 micrometers, are especially harmful. These tiny particles can penetrate deep into the lungs and even enter the bloodstream, causing respiratory and heart diseases. Therefore, monitoring and analyzing PM2.5 levels is essential to protect people's health and support environmental policies.

In this project, we work with near real-time air quality data collected from multiple automatic monitoring stations spread across various provinces in Thailand. The dataset includes hourly PM2.5 measurements along with important details such as timestamps and station locations. This data is provided by the Air4Thai network through an API maintained by the Pollution Control Department, ensuring that the information is reliable and regularly updated. To make the entire process smooth and scalable, we use Prefect.io to automate the data workflows and Docker to containerize the system, allowing it to run consistently across different environments.

🪄 Main goal is to build an interactive dashboard using Streamlit that helps users easily understand trends in air pollution, identify areas with good or poor air quality, and recommend provinces with the best conditions for outdoor activities.