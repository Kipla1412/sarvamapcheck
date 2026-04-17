from fastapi import APIRouter, Request, Depends, HTTPException
from api.auth import get_current_user, require_permission
from pydantic import BaseModel
from fastapi.responses import StreamingResponse, FileResponse
import json
import uuid
import os
from pathlib import Path

from agent.agent import Agent

router = APIRouter(prefix="/agent")


class ChatRequest(BaseModel):
    message: str
    session_id: str | None = None


@router.post(
    "/chat",
    summary="Agent Chat Interface",
    description="""
Stream responses from the AI agent.

This endpoint processes a user message and streams the agent's responses 
as incremental events. The agent may call tools, generate text tokens, 
and produce structured outputs.

Features:
- Real-time streaming response
- Session-based conversation context
- Tool execution support
- Authenticated user isolation

Authentication:
Requires a valid authenticated user session.

Permission Required:
`agent:chat`
""",
    dependencies=[Depends(require_permission("agent", "chat"))]
)
async def chat(req: ChatRequest, request: Request):
    
    # DEBUG: Function entry point
    print("DEBUG: Chat function called!")

    sessions = request.app.state.sessions
    config = request.app.state.config

    user = request.state.user
    # Generate session id if missing - this isolates conversations
    user_id = user.get("sub")
    session_id = req.session_id or str(uuid.uuid4())
    
    print(f"DEBUG: User ID = {user_id}")
    print(f"DEBUG: Session ID = {session_id}")
    print(f"DEBUG: Request session_id = {req.session_id}")
    
    # Use session_id for storage - each conversation is isolated
    if session_id not in sessions:
        print(f"DEBUG: Creating new agent for session {session_id}")
        agent = Agent(config)
        await agent.__aenter__()
        sessions[session_id] = agent
    else:
        print(f"DEBUG: Reusing existing agent for session {session_id}")

    agent = sessions[session_id]

    async def event_stream():

        async for event in agent.run(req.message):

            yield json.dumps({
                "type": event.type.value if hasattr(event.type, "value") else str(event.type),
                "data": event.data
            }) + "\n"

    return StreamingResponse(
        event_stream(),
        media_type="application/json; charset=utf-8",
        headers={"X-Session-ID": session_id}
    )


class ReportDownloadRequest(BaseModel):
    patient_id: str
    report_type: str  # "summary", "soap", "assessment", "all"


class SaveSummaryRequest(BaseModel):
    summary: str
    session_id: str | None = None
    metadata: dict | None = None


@router.post(
    "/save-summary",
    summary="Save Last Summary",
    description="""
    Save the last generated summary for a session.
    
    This endpoint stores the provided summary text along with session metadata
    for future reference or retrieval.
    """,
)
async def save_summary(req: SaveSummaryRequest, request: Request):
    """
    Save the last summary for a session
    """
    try:
        # Get sessions from app state
        sessions = request.app.state.sessions
        
        # Get user info like chat endpoint
        user = request.state.user
        user_id = user.get("sub")
        
        # Use session_id for storage - same as chat endpoint
        session_id = req.session_id or str(uuid.uuid4())
        
        # DEBUG: Print session ID to console
        print(f"DEBUG: Save-summary function called!")
        print(f"DEBUG: User ID = {user_id}")
        print(f"DEBUG: Session ID = {session_id}")
        print(f"DEBUG: Request session_id = {req.session_id}")
        
        # Store summary in session or a separate storage
        # For now, we'll store it in the session state
        if not hasattr(request.app.state, 'summaries'):
            request.app.state.summaries = {}
        
        # Save the summary with metadata - using session_id for isolation
        summary_data = {
            "summary": req.summary,
            "user_id": user_id,        # For user tracking
            "session_id": session_id,   # For conversation tracking
            "timestamp": str(uuid.uuid4()),
            "metadata": req.metadata or {}
        }
        
        # Use session_id for storage - same as chat endpoint
        request.app.state.summaries[session_id] = summary_data
        
        return {
            "status": "success",
            "message": "Summary saved successfully",
            "session_id": session_id,  # Return session_id for consistency
            "user_id": user_id,
            "timestamp": summary_data["timestamp"]
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Failed to save summary: {str(e)}"
        }


@router.post(
    "/download-reports",
    summary="Download Patient Reports",
    description="""
    Download generated patient reports as PDF files.

    This endpoint allows downloading of clinical reports that were generated
    during the patient intake process.

    Report Types:
    - summary: Patient summary report
    - soap: SOAP note report  
    - assessment: Assessment and plan report
    - all: All reports bundled together

    Authentication:
    Requires a valid authenticated user session.

    Permission Required:
    `previsitagent:downloadreports`
    """,
    dependencies=[Depends(require_permission("previsitagent", "downloadreports"))]
)
async def download_reports(req: ReportDownloadRequest, request: Request):
    
    try:
        # Get the base patients directory
        patients_dir = Path("patients")
        
        if not patients_dir.exists():
            raise HTTPException(status_code=404, detail="Patients directory not found")
        
        # Get patient directory
        patient_dir = patients_dir / req.patient_id
        
        if not patient_dir.exists():
            raise HTTPException(status_code=404, detail=f"Patient {req.patient_id} not found")
        
        # Handle different report types
        if req.report_type == "all":
            # Return a list of all available reports
            reports = []
            report_files = {
                "summary": patient_dir / "patient_summary.pdf",
                "soap": patient_dir / "soap_report.pdf", 
                "assessment": patient_dir / "assessment_plan_report.pdf"
            }
            
            for report_name, file_path in report_files.items():
                if file_path.exists():
                    reports.append({
                        "type": report_name,
                        "filename": file_path.name,
                        "download_url": f"/agent/download-report/{req.patient_id}/{report_name}"
                    })
            
            return {"reports": reports}
            
        else:
            # Download specific report
            report_files = {
                "summary": "patient_summary.pdf",
                "soap": "soap_report.pdf",
                "assessment": "assessment_plan_report.pdf"
            }
            
            if req.report_type not in report_files:
                raise HTTPException(status_code=400, detail=f"Invalid report type: {req.report_type}")
            
            report_file = patient_dir / report_files[req.report_type]
            
            if not report_file.exists():
                raise HTTPException(status_code=404, detail=f"Report {req.report_type} not found")
            
            return FileResponse(
                path=report_file,
                filename=report_file.name,
                media_type="application/pdf"
            )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error downloading reports: {str(e)}")


@router.get(
    "/download-report/{patient_id}/{report_type}",
    summary="Download Specific Report",
    description="""
    Direct download endpoint for specific patient reports.

    Path Parameters:
    - patient_id: Patient identifier
    - report_type: Type of report (summary, soap, assessment)

    Returns:
    - PDF file for download
    """,
    dependencies=[Depends(require_permission("previsitagent", "downloadreports"))]
)
async def download_specific_report(patient_id: str, report_type: str, request: Request):
    
    try:
        # Get the base patients directory
        patients_dir = Path("patients")
        
        if not patients_dir.exists():
            raise HTTPException(status_code=404, detail="Patients directory not found")
        
        # Get patient directory
        patient_dir = patients_dir / patient_id
        
        if not patient_dir.exists():
            raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")
        
        # Map report types to filenames
        report_files = {
            "summary": "patient_summary.pdf",
            "soap": "soap_report.pdf",
            "assessment": "assessment_plan_report.pdf"
        }
        
        if report_type not in report_files:
            raise HTTPException(status_code=400, detail=f"Invalid report type: {report_type}")
        
        report_file = patient_dir / report_files[report_type]
        
        if not report_file.exists():
            raise HTTPException(status_code=404, detail=f"Report {report_type} not found")
        
        return FileResponse(
            path=report_file,
            filename=report_file.name,
            media_type="application/pdf"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error downloading report: {str(e)}")


# from fastapi import APIRouter, Request
# from pydantic import BaseModel
# from fastapi.responses import StreamingResponse
# import json
# from agent.agent import Agent
# from config.config import Config

# router = APIRouter(prefix="/agent")


# class ChatRequest(BaseModel):
#     message: str
#     session_id: str | None = None


# @router.post("/chat")
# async def chat(req: ChatRequest, request: Request):

#     sessions = request.app.state.sessions
#     config = request.app.state.config

#     if req.session_id not in sessions:
#         # First time this user has messaged? Give them a private agent.
#         agent = Agent(Config())
#         await agent.__aenter__()
#         sessions[req.session_id] = agent
    
#     # Use their private agent
#     agent = sessions[req.session_id]

#     async def event_stream():

#         async for event in agent.run(req.message):

#             yield json.dumps({
#                 "type": event.type.value if hasattr(event.type, "value") else str(event.type),
#                 "data": event.data
#             }) + "\n"

#     return StreamingResponse(event_stream(), media_type="application/json; charset=utf-8")

# @router.post("/approve")
# async def approve(data: dict, request: Request):

#     approval_id = data["approval_id"]
#     approved = data["approved"]

#     agent = request.app.state.agent
#     pending = agent.session.pending_approvals

#     if approval_id not in pending:
#         return {"status": "not_found", "approval_id": approval_id}
    
#     future = pending[approval_id]

#     if not future.done():
#         future.set_result(approved)

#     return {
#         "status": "ok",
#         "approval_id": approval_id,
#         "approved": approved
#     }