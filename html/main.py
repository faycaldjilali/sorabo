from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import io
import uvicorn
from datetime import datetime
from typing import List, Optional
import json

app = FastAPI(title="BOAMP Data Processor")

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Store processed data in memory (in production, use a database)
processed_data = {}

def get_predefined_keywords():
    """Return predefined keywords for filtering"""
    return [
        "miroiterie", "métallerie", "menuiserie extérieure",
        "Travaux de menuiserie et de charpenterie",
        "Pose de portes et de fenêtres et d'éléments accessoires",
        "Pose d'encadrements de portes et de fenêtres",
        "Pose d'encadrements de portes",
        "Pose d'encadrements de fenêtres",
        "Pose de seuils",
        "Poses de portes et de fenêtres",
        "Pose de portes",
        "Pose de fenêtres",
        "Pose de menuiseries métalliques, excepté portes et fenêtres",
        "Travaux de cloisonnement",
        "Installation de volets",
        "Travaux d'installation de stores",
        "Travaux d'installation de vélums",
        "Travaux d'installation de volets roulants",
        "Serrurerie",
        "Services de serrurerie",
        "Menuiserie pour la construction",
        "Travaux de menuiserie",
        "Clôtures",
        "Clôtures de protection",
        "Travaux d'installation de clôtures, de garde-corps et de dispositifs de sécurité",
        "Pose de clôtures",
        "Ascenseurs, skips, monte-charges, escaliers mécaniques et trottoirs roulants",
        "Escaliers mécaniques",
        "Pièces pour ascenseurs, skips ou escaliers mécaniques",
        "Pièces pour escaliers mécaniques",
        "Escaliers",
        "Escaliers pliants",
        "Travaux d'installation d'ascenseurs et d'escaliers mécaniques",
        "Travaux d'installation d'escaliers mécaniques",
        "Services de réparation et d'entretien d'escaliers mécaniques",
        "Services d'installation de matériel de levage et de manutention, excepté ascenseurs et escaliers mécaniques",
        "45420000", "45421100", "45421110", "45421111", "45421112", "45421120", 
        "45421130", "45421131", "45421132", "45421140", "45421141", "45421142", 
        "45421143", "45421144", "45421145", "44316500", "98395000", "44220000", 
        "45421000", "34928200", "34928310", "45340000", "45342000", "42416000", 
        "42416400", "42419500", "42419530", "44233000", "44423220", "45313000", 
        "45313200", "50740000", "51511000",
    ]

def filter_by_keywords(df: pd.DataFrame, keywords: List[str]) -> pd.DataFrame:
    """Filter DataFrame based on keywords"""
    if df.empty:
        return df
    
    # Convert all columns to string and search for keywords
    mask = pd.Series([False] * len(df))
    
    for column in df.columns:
        if df[column].dtype == object:  # String columns
            column_mask = df[column].astype(str).str.lower()
            for keyword in keywords:
                keyword_lower = keyword.lower()
                mask = mask | column_mask.str.contains(keyword_lower, na=False)
    
    return df[mask]

def remove_duplicates(df: pd.DataFrame, id_column: str, keyword_column: str) -> pd.DataFrame:
    """Remove duplicates and combine keywords"""
    if id_column not in df.columns or keyword_column not in df.columns:
        return df
    
    # Group by ID and combine keywords
    grouped = df.groupby(id_column).agg({
        keyword_column: lambda x: '; '.join(set([str(i) for i in x if pd.notna(i) and str(i).strip() != '']))
    }).reset_index()
    
    # Merge with original data to keep other columns (keep first occurrence)
    other_columns = [col for col in df.columns if col != keyword_column]
    df_first = df.drop_duplicates(subset=[id_column], keep='first')[other_columns]
    
    result_df = pd.merge(df_first, grouped, on=id_column, how='inner')
    return result_df

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serve the main page"""
    predefined_keywords = get_predefined_keywords()
    return templates.TemplateResponse("templates\index2.html", {
        "request": request,
        "predefined_keywords": predefined_keywords
    })

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Handle file upload"""
    try:
        # Read the file
        contents = await file.read()
        
        # Determine file type and read accordingly
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(contents))
        elif file.filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")
        
        # Store the dataframe
        file_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        processed_data[file_id] = {
            "df": df,
            "filename": file.filename,
            "upload_time": datetime.now()
        }
        
        return JSONResponse({
            "file_id": file_id,
            "filename": file.filename,
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": df.columns.tolist()
        })
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@app.post("/filter")
async def filter_data(
    file_id: str = Form(...),
    selected_keywords: str = Form(...),
    custom_keywords: str = Form(...)
):
    """Apply keyword filtering"""
    try:
        if file_id not in processed_data:
            raise HTTPException(status_code=404, detail="File not found")
        
        df = processed_data[file_id]["df"]
        
        # Parse keywords
        selected_keywords_list = json.loads(selected_keywords)
        custom_keywords_list = [k.strip() for k in custom_keywords.split('\n') if k.strip()]
        
        all_keywords = selected_keywords_list + custom_keywords_list
        
        if not all_keywords:
            raise HTTPException(status_code=400, detail="No keywords provided")
        
        # Apply filtering
        filtered_df = filter_by_keywords(df, all_keywords)
        
        if filtered_df.empty:
            return JSONResponse({
                "status": "warning",
                "message": "No matches found for the selected keywords.",
                "rows": 0
            })
        
        # Store filtered data
        processed_data[file_id]["filtered_df"] = filtered_df
        
        return JSONResponse({
            "status": "success",
            "message": f"Found {len(filtered_df)} matching records",
            "rows": len(filtered_df)
        })
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error filtering data: {str(e)}")

@app.post("/remove-duplicates")
async def remove_duplicates_endpoint(
    file_id: str = Form(...),
    id_column: str = Form(...),
    keyword_column: str = Form(...)
):
    """Remove duplicates from filtered data"""
    try:
        if (file_id not in processed_data or 
            "filtered_df" not in processed_data[file_id]):
            raise HTTPException(status_code=404, detail="Filtered data not found")
        
        filtered_df = processed_data[file_id]["filtered_df"]
        
        # Remove duplicates
        cleaned_df = remove_duplicates(filtered_df, id_column, keyword_column)
        
        # Store cleaned data
        processed_data[file_id]["cleaned_df"] = cleaned_df
        
        duplicate_ids = filtered_df[filtered_df.duplicated(subset=[id_column], keep=False)][id_column].unique()
        
        return JSONResponse({
            "status": "success",
            "message": f"Removed duplicates! Kept {len(cleaned_df)} unique rows out of {len(filtered_df)} total.",
            "original_rows": len(filtered_df),
            "cleaned_rows": len(cleaned_df),
            "duplicate_ids_count": len(duplicate_ids)
        })
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error removing duplicates: {str(e)}")

@app.get("/download/{file_id}")
async def download_file(file_id: str, data_type: str = "cleaned"):
    """Download processed data"""
    try:
        if file_id not in processed_data:
            raise HTTPException(status_code=404, detail="File not found")
        
        if data_type == "cleaned" and "cleaned_df" in processed_data[file_id]:
            df = processed_data[file_id]["cleaned_df"]
            filename_suffix = "cleaned"
        elif data_type == "filtered" and "filtered_df" in processed_data[file_id]:
            df = processed_data[file_id]["filtered_df"]
            filename_suffix = "filtered"
        else:
            df = processed_data[file_id]["df"]
            filename_suffix = "original"
        
        # Create Excel file in memory
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False, engine='openpyxl')
        excel_buffer.seek(0)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"BOAMP_{filename_suffix}_{timestamp}.xlsx"
        
        return StreamingResponse(
            excel_buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error downloading file: {str(e)}")

@app.get("/columns/{file_id}")
async def get_columns(file_id: str):
    """Get column names for a file"""
    if file_id not in processed_data:
        raise HTTPException(status_code=404, detail="File not found")
    
    df = processed_data[file_id]["df"]
    return JSONResponse({"columns": df.columns.tolist()})

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)