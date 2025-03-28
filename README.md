# Rental Marketplace

Overview
========

This project focuses on implementing an end-to-end data pipeline for a rental marketplace platform, similar to Airbnb. The pipeline extracts data from an AWS Aurora MySQL database, processes it, and loads it into an Amazon Redshift data warehouse. The platform's data includes rental listings and user interactions, enabling analytical reporting and business intelligence. The pipeline ensures data validation, transformation, and efficient loading into Redshift to support reporting and decision-making processes.

<p align="center">
    <img src="images/architecture_diagram.jpg" alt="The architecture diagram" width="100%" />
</p>

## 🔧 AWS Glue Python Shell Jobs

### **1️⃣ Extract Job**
- Moves data from **RDS to S3** and **loads into Redshift Raw Layer**
- Script location: `glue_scripts/ingestion.py`

### **2️⃣ Transform Job**
- Processes **S3 data** and loads into **Redshift Curated Schema**
- Script location: `glue_scripts/transform.py`

### **3️⃣ Load and KPI Computation Job**
- Computes **business metrics** from **S3 data** and stores in **Redshift Presentation**
- Script location: `glue_scripts/loading.py`
## 🏗️ AWS Step Functions Workflow

<p align="center">
    <img src="images/Glue.png" alt="The architecture diagram" width="100%" />
</p>

- **Script location**: `glue_scripts/step_functions.py`

1. **Start Execution**
2. **Run Extract & Load Job**
3. **Run Transform Job**
4. **Run KPI Calculation Job**
5. **Send Email**
6. **End Execution**




## 📂 Datasets & Schema

<p align="center">
    <img src="images/Init_db_erd.jpg" alt="The architecture diagram" width="100%" />
</p>
### **1️⃣ Apartments Data**
| Column                | Type     | Description                   |
|----------------------|---------|-----------------------------|
| id                   | INT     | Unique apartment ID         |
| title                | STRING  | Apartment title             |
| source               | STRING  | Listing source              |
| price                | FLOAT   | Rental price                |
| currency             | STRING  | Currency type               |
| listing_created_on   | TIMESTAMP | Listing creation date   |
| is_active            | BOOL    | Active listing status       |
| last_modified_timestamp | TIMESTAMP | Last update timestamp |

### **2️⃣ Apartment Attributes**
| Column         | Type   | Description                   |
|--------------|-------|-----------------------------|
| id           | INT   | Apartment ID                |
| category     | STRING | Listing category           |
| body         | TEXT  | Description                |
| amenities    | TEXT  | Available amenities        |
| bathrooms    | INT   | Number of bathrooms       |
| bedrooms     | INT   | Number of bedrooms        |
| fee          | FLOAT | Additional fees            |
| has_photo    | BOOL  | Whether photos exist       |
| pets_allowed | STRING | Pet policy (nullable)    |
| price_display | STRING | Price display format    |
| price_type   | STRING | Pricing model            |
| square_feet  | INT   | Apartment size (sq ft)    |
| address      | STRING | Apartment address        |
| cityname     | STRING | City                     |
| state        | STRING | State                     |
| latitude     | FLOAT  | Latitude coordinate      |
| longitude    | FLOAT  | Longitude coordinate     |

### **3️⃣ User Viewings**
| Column         | Type     | Description                         |
|--------------|---------|---------------------------------|
| user_id      | INT     | User ID                          |
| apartment_id | INT     | Apartment ID                    |
| viewed_at    | TIMESTAMP | Viewing timestamp           |
| is_wishlisted | BOOL    | Whether user wishlisted       |
| call_to_action | STRING | Action taken after viewing  |

### **4️⃣ Bookings**
| Column        | Type     | Description                     |
|-------------|---------|-----------------------------|
| booking_id   | INT     | Unique booking ID         |
| user_id      | INT     | User ID                   |
| apartment_id | INT     | Apartment ID              |
| booking_date | TIMESTAMP | Date of booking       |
| checkin_date | TIMESTAMP | Check-in date         |
| checkout_date | TIMESTAMP | Check-out date       |
| total_price  | FLOAT   | Total booking cost       |
| currency     | STRING  | Payment currency        |
| booking_status | STRING | Booking status         |


## 🧰 Technologies Used
- **AWS Aurora MySQL**: Source database for rental marketplace data.
- **AWS S3**: Storage for raw and processed data.
- **AWS Redshift**: Data warehouse for analytical reporting.
- **AWS Glue**: ETL jobs for data extraction, transformation, and loading.
- **AWS Step Functions**: Orchestrates the pipeline workflow.
- **Python**: Scripting language for Glue jobs and data processing.


## 📊 Monthly KPI Analysis

### SQL Query
The following query retrieves key performance indicators (KPIs) for monthly analysis:

```sql
SELECT 
    o.month,
    o.occupancy_rate,
    d.avg_booking_duration,
    r.repeat_customer_rate
FROM presentation.public.occupancy_rate_per_month o
LEFT JOIN presentation.public.avg_booking_duration_per_month d
    ON o.month = d.month
LEFT JOIN presentation.public.repeat_customer_rate_per_month r
    ON o.month = r.month
ORDER BY o.month;
```

### Explanation for the CEO (Monthly KPIs)

- **Occupancy Rate**:  
  This metric shows the percentage of available rental nights that were booked in a month, indicating how effectively our inventory is being utilized.

- **Average Booking Duration**:  
  By tracking the average length of stays, we gain insight into customer preferences and the potential revenue per booking.

- **Repeat Customer Rate**:  
  This tells us the percentage of customers returning to book again within a month, a key indicator of customer satisfaction and loyalty.

