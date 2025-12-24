from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import re
import uuid
import json
import asyncio
from typing import List, Optional, Dict, Any
import os
from datetime import datetime
from pathlib import Path

app = FastAPI(title="Real Estate Lead Processor API", version="1.0.0")

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for jobs (use Redis/DB in production)
jobs = {}

class LeadProcessor:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.stats = {
            "total": 0,
            "sellers_removed": 0,
            "duplicates_removed": 0,
            "invalid_numbers": 0,
            "excluded": 0,
            "final": 0,
            "processing_time": 0
        }
        self.errors = []
        self.results = []
        
    def normalize_phone(self, phone: str) -> str:
        """Normalize UAE phone numbers to +971XXXXXXXXX format"""
        if pd.isna(phone):
            return "INVALID"
            
        # Convert to string and remove all non-numeric except +
        phone_str = str(phone)
        cleaned = re.sub(r'[^\d+]', '', phone_str)
        
        # Handle different formats
        if cleaned.startswith('0') and len(cleaned) == 10:  # 0501234567
            return f'+971{cleaned[1:]}'
        elif cleaned.startswith('971') and len(cleaned) == 12:  # 971501234567
            return f'+{cleaned}'
        elif cleaned.startswith('+971') and len(cleaned) == 13:  # Already correct
            return cleaned
        elif len(cleaned) == 9:  # 501234567
            return f'+971{cleaned}'
        else:
            return "INVALID"
    
    def detect_column(self, headers: List[str], patterns: List[str]) -> Optional[str]:
        """Auto-detect column based on header patterns"""
        headers_lower = [str(h).lower() for h in headers]
        
        for pattern in patterns:
            if isinstance(pattern, str):
                for header in headers_lower:
                    if pattern.lower() in header:
                        return headers[headers_lower.index(header)]
            else:  # regex pattern
                for i, header in enumerate(headers_lower):
                    if pattern.search(header):
                        return headers[i]
        return None
    
    def is_seller(self, type_value: Any) -> bool:
        """Check if record is a seller"""
        if pd.isna(type_value):
            return False
            
        seller_patterns = [
            'seller', 'owner', 'vendor', 'landlord',
            'mortgagor', 'transferor', 'lessor', 'landlady'
        ]
        
        type_str = str(type_value).lower().strip()
        return any(pattern in type_str for pattern in seller_patterns)
    
    async def process_file(self, file_path: Path, exclusion_list: Optional[List[str]] = None):
        """Main processing pipeline"""
        start_time = datetime.now()
        
        try:
            # Step 1: Read file
            if file_path.suffix in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                df = pd.read_csv(file_path)
            
            self.stats['total'] = len(df)
            
            # Step 2: Auto-detect columns
            headers = list(df.columns)
            
            name_col = self.detect_column(headers, ['name', 'fullname', 'contact', 'client'])
            phone_col = self.detect_column(headers, ['mobile', 'phone', 'cell', 'contactno'])
            type_col = self.detect_column(headers, ['type', 'role', 'party', 'category'])
            
            if not all([name_col, phone_col]):
                raise ValueError("Could not auto-detect required columns")
            
            # Step 3: Normalize phone numbers
            df['normalized_phone'] = df[phone_col].apply(self.normalize_phone)
            
            # Count invalid numbers
            invalid_mask = df['normalized_phone'] == "INVALID"
            self.stats['invalid_numbers'] = invalid_mask.sum()
            df = df[~invalid_mask]
            
            # Step 4: Filter sellers
            if type_col:
                seller_mask = df[type_col].apply(self.is_seller)
                self.stats['sellers_removed'] = seller_mask.sum()
                df = df[~seller_mask]
            
            # Step 5: Remove duplicates
            df = df.drop_duplicates(subset=['normalized_phone'])
            self.stats['duplicates_removed'] = self.stats['total'] - len(df) - self.stats['invalid_numbers'] - self.stats['sellers_removed']
            
            # Step 6: Apply exclusion list
            if exclusion_list:
                exclusion_set = {self.normalize_phone(num) for num in exclusion_list}
                before_exclude = len(df)
                df = df[~df['normalized_phone'].isin(exclusion_set)]
                self.stats['excluded'] = before_exclude - len(df)
            
            # Prepare results
            self.results = df[[name_col, 'normalized_phone']].rename(
                columns={name_col: 'Name', 'normalized_phone': 'Mobile'}
            ).to_dict('records')
            
            self.stats['final'] = len(self.results)
            
            # Calculate processing time
            self.stats['processing_time'] = (datetime.now() - start_time).total_seconds()
            
            # Save results
            output_dir = Path("processed_results")
            output_dir.mkdir(exist_ok=True)
            
            # Save as CSV
            csv_path = output_dir / f"{self.job_id}.csv"
            df[['Name', 'normalized_phone']].rename(
                columns={'normalized_phone': 'Mobile'}
            ).to_csv(csv_path, index=False)
            
            # Save as TXT (phones only)
            txt_path = output_dir / f"{self.job_id}.txt"
            df['normalized_phone'].to_csv(txt_path, index=False, header=False)
            
            jobs[self.job_id]['status'] = 'completed'
            jobs[self.job_id]['stats'] = self.stats
            jobs[self.job_id]['downloads'] = {
                'csv': str(csv_path),
                'txt': str(txt_path)
            }
            
        except Exception as e:
            jobs[self.job_id]['status'] = 'failed'
            jobs[self.job_id]['error'] = str(e)
            raise

# API Endpoints
@app.get("/")
async def root():
    return {"message": "Real Estate Lead Processor API", "status": "running"}

@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    exclusion_list: Optional[UploadFile] = File(None)
):
    """Upload and process lead file"""
    job_id = str(uuid.uuid4())
    
    # Save uploaded files
    upload_dir = Path("uploads")
    upload_dir.mkdir(exist_ok=True)
    
    file_path = upload_dir / f"{job_id}_{file.filename}"
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)
    
    # Process exclusion list if provided
    exclusion_numbers = []
    if exclusion_list:
        exclusion_content = await exclusion_list.read()
        exclusion_text = exclusion_content.decode()
        exclusion_numbers = [num.strip() for num in exclusion_text.split(',')]
    
    # Initialize job
    jobs[job_id] = {
        "status": "processing",
        "filename": file.filename,
        "created_at": datetime.now().isoformat()
    }
    
    # Start processing
    processor = LeadProcessor(job_id)
    asyncio.create_task(processor.process_file(file_path, exclusion_numbers))
    
    return {"job_id": job_id, "status": "processing"}

@app.get("/api/job/{job_id}")
async def get_job_status(job_id: str):
    """Check job processing status"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    return {
        "status": job["status"],
        "stats": job.get("stats"),
        "filename": job["filename"],
        "created_at": job["created_at"],
        "downloads": job.get("downloads")
    }

@app.get("/api/download/{job_id}/{format}")
async def download_results(job_id: str, format: str):
    """Download processed results"""
    if job_id not in jobs or jobs[job_id]["status"] != "completed":
        raise HTTPException(status_code=404, detail="Results not ready")
    
    if format not in ["csv", "txt"]:
        raise HTTPException(status_code=400, detail="Invalid format")
    
    file_path = jobs[job_id]["downloads"][format]
    if not Path(file_path).exists():
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(
        path=file_path,
        filename=f"processed_leads_{job_id}.{format}",
        media_type="text/csv" if format == "csv" else "text/plain"
    )

@app.get("/api/sample")
async def get_sample_file():
    """Get sample CSV for testing"""
    sample_data = [
        ["Name", "Mobile", "Type"],
        ["Ahmed Al Mansouri", "0501234567", "Buyer"],
        ["Fatima Al Khaleej", "971501234567", "Seller"],
        ["Mohammed Ali", "050 987 6543", "Buyer"],
        ["Sara Ahmed", "+971501112233", "Seller"],
        ["Omar Khan", "055-123-4567", "Buyer"]
    ]
    
    sample_path = Path("sample_leads.csv")
    pd.DataFrame(sample_data[1:], columns=sample_data[0]).to_csv(sample_path, index=False)
    
    return FileResponse(
        path=sample_path,
        filename="sample_leads.csv",
        media_type="text/csv"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
