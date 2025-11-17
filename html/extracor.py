from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import datetime
import requests
import pandas as pd
import json
import os

app = FastAPI(title="BOAMP Data Extraction API")

# Enable CORS for frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request model
class ExtractionRequest(BaseModel):
    target_date: str

# Response model
class ExtractionResponse(BaseModel):
    status: str
    record_count: int
    message: str
    logs: list = []

@app.get("/")
async def read_index():
    """Serve the HTML frontend"""
    return FileResponse('templates/index.html')

@app.post("/extract-data")
async def extract_data(request: ExtractionRequest):
    """Extract BOAMP data for a specific date"""
    logs = []
    
    try:
        # Validate date format
        datetime.strptime(request.target_date, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    logs.append(f"Starting extraction for date: {request.target_date}")
    
    # Call the data extraction function
    records = get_all_records_for_date(request.target_date, logs)
    
    logs.append(f"Extraction complete. Found {len(records)} records")
    
    # Create Excel file
    if records:
        df = create_excel_simple(records, request.target_date)
        # Save to file
        filename = f"boamp_data_{request.target_date}.xlsx"
        df.to_excel(filename, index=False)
        logs.append(f"Excel file saved as: {filename}")
        
        return ExtractionResponse(
            status="success",
            record_count=len(records),
            message=f"Successfully extracted {len(records)} records",
            logs=logs
        )
    else:
        return ExtractionResponse(
            status="success",
            record_count=0,
            message="No records found for the specified date",
            logs=logs
        )

def get_all_records_for_date(target_date, logs, max_records=5000):
    """Get all records for a specific date with all available fields"""
    url = "https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records"
    all_records = []
    offset = 0
    limit = 100

    while len(all_records) < max_records:
        params = {
            'order_by': 'dateparution DESC',
            'limit': limit,
            'offset': offset
        }

        log_msg = f"Requesting offset {offset}..."
        logs.append(log_msg)
        print(log_msg)
        
        try:
            response = requests.get(url, params=params, timeout=30)

            if response.status_code != 200:
                error_msg = f"Error {response.status_code}: {response.text}"
                logs.append(error_msg)
                print(error_msg)
                break

            data = response.json()
            records = data.get('results', [])

            if not records:
                break  # No more records

            # Filter records for our target date
            target_records = [record for record in records if record.get('dateparution') == target_date]

            # If we found target records, add them
            if target_records:
                all_records.extend(target_records)
                log_msg = f"Retrieved {len(target_records)} records for {target_date}... Total so far: {len(all_records)}"
                logs.append(log_msg)
                print(log_msg)

            # Check if we've moved past our target date (since we're sorting DESC)
            if records and records[-1].get('dateparution', '') < target_date:
                log_msg = f"Reached dates earlier than {target_date}. Stopping."
                logs.append(log_msg)
                print(log_msg)
                break

            offset += limit

            if offset > 10000:
                log_msg = "Safety limit reached. Stopping."
                logs.append(log_msg)
                print(log_msg)
                break
                
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            logs.append(error_msg)
            print(error_msg)
            break

    return all_records

def create_excel_simple(records, target_date):
    """Simple and robust Excel creation"""
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if isinstance(value, (list, dict)):
                cleaned_record[key] = json.dumps(value, ensure_ascii=False)
            elif value is None:
                cleaned_record[key] = ''
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)

    df = pd.DataFrame(cleaned_records)
    return df

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "message": "BOAMP Data Extraction API is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)