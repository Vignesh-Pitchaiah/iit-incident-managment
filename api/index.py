from fastapi import FastAPI, Request, HTTPException
import snowflake.connector
import json
import os
import re
import logging
from datetime import datetime
from typing import Optional

app = FastAPI()

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_resolution_note(note: str):
    """Parse RCA and business justification from resolution notes"""
    rca1 = rca2 = business = None
    
    if not note:
        logger.info("No resolution note provided")
        return None, None, None
    
    logger.info(f"Parsing resolution note: {repr(note)}")
    
    # Try multiple regex patterns for flexibility
    patterns = [
        # Pattern 1: Standard format
        (r"rca_?1[:\s]+(.*?)(?:\n|rca_?2|business|$)", r"rca_?2[:\s]+(.*?)(?:\n|business|$)", r"business_?justification[:\s]+(.*?)(?:\n|$)"),
        # Pattern 2: With equals signs
        (r"rca_?1\s*=\s*(.*?)(?:\n|rca_?2|business|$)", r"rca_?2\s*=\s*(.*?)(?:\n|business|$)", r"business_?justification\s*=\s*(.*?)(?:\n|$)"),
        # Pattern 3: More flexible
        (r"(?:^|\n)\s*rca_?1[:\s]+(.*?)(?=\n\s*(?:rca_?2|business)|$)", r"(?:^|\n)\s*rca_?2[:\s]+(.*?)(?=\n\s*business|$)", r"(?:^|\n)\s*business_?justification[:\s]+(.*?)$")
    ]
    
    for rca1_pattern, rca2_pattern, business_pattern in patterns:
        m1 = re.search(rca1_pattern, note, re.IGNORECASE | re.DOTALL | re.MULTILINE)
        m2 = re.search(rca2_pattern, note, re.IGNORECASE | re.DOTALL | re.MULTILINE)
        m3 = re.search(business_pattern, note, re.IGNORECASE | re.DOTALL | re.MULTILINE)
        
        if m1:
            rca1 = m1.group(1).strip()
            logger.info(f"Found RCA1 with pattern: {rca1}")
        if m2:
            rca2 = m2.group(1).strip()
            logger.info(f"Found RCA2 with pattern: {rca2}")
        if m3:
            business = m3.group(1).strip()
            logger.info(f"Found Business Justification with pattern: {business}")
        
        # If we found something, break
        if rca1 or rca2 or business:
            break
    
    logger.info(f"Final parsed values - RCA1: {repr(rca1)}, RCA2: {repr(rca2)}, Business: {repr(business)}")
    return rca1, rca2, business

def get_snowflake_connection():
    """Create Snowflake connection"""
    return snowflake.connector.connect(
        user="SVCDQM",
        password="LOGINuSER13579",
        account="NXKZZIV-WN17856", 
        warehouse="COMPUTE_WH",
        database="DEV_DWDB",
        schema="DT_OPS"
    )

def extract_resolution_info_from_payload(payload: dict, incident: dict) -> str:
    """Extract resolution information from various parts of the payload"""
    resolution_sources = []
    
    logger.info("Extracting resolution info from payload...")
    
    # Check incident-level fields
    fields_to_check = [
        'resolve_reason', 'description', 'body', 'summary', 
        'details', 'incident_notes', 'resolution_notes'
    ]
    
    for field in fields_to_check:
        value = incident.get(field)
        if value:
            logger.info(f"Found content in incident.{field}: {repr(value)}")
            resolution_sources.append(str(value))
    
    # Check log entries (this is often where resolution info is stored)
    if 'log_entries' in payload:
        logger.info("Checking log_entries for resolution info...")
        for entry in payload.get('log_entries', []):
            if entry.get('type') == 'resolve_log_entry':
                channel = entry.get('channel', {})
                if channel.get('details'):
                    logger.info(f"Found resolve_log_entry details: {repr(channel.get('details'))}")
                    resolution_sources.append(str(channel.get('details')))
    
    # Check messages in the payload
    if 'messages' in payload:
        logger.info("Checking messages for resolution info...")
        for message in payload.get('messages', []):
            content = message.get('content') or message.get('body')
            if content:
                logger.info(f"Found message content: {repr(content)}")
                resolution_sources.append(str(content))
    
    # Check notes array
    if incident.get('notes'):
        logger.info("Checking incident notes...")
        for note in incident.get('notes', []):
            if isinstance(note, dict):
                content = note.get('content') or note.get('note')
                if content:
                    logger.info(f"Found note content: {repr(content)}")
                    resolution_sources.append(str(content))
            elif isinstance(note, str):
                logger.info(f"Found note string: {repr(note)}")
                resolution_sources.append(note)
    
    # Check recent status change details
    if incident.get('last_status_change_by'):
        change_info = incident.get('last_status_change_by', {})
        if change_info.get('summary'):
            logger.info(f"Found last_status_change_by summary: {repr(change_info.get('summary'))}")
            resolution_sources.append(str(change_info.get('summary')))
    
    combined_resolution = '\n'.join(filter(None, resolution_sources))
    logger.info(f"Combined resolution info: {repr(combined_resolution)}")
    
    return combined_resolution

@app.post("/pagerduty")
async def ingest_incident(request: Request):
    try:
        # Log raw request for debugging
        raw_body = await request.body()
        logger.info(f"Raw webhook body: {raw_body.decode('utf-8')}")
        
        payload = json.loads(raw_body.decode('utf-8'))
        logger.info(f"Parsed webhook payload: {json.dumps(payload, indent=2)}")
        
        # Handle different webhook formats
        if "event" in payload:
            # v1 format
            event = payload.get("event", {})
            event_type = event.get("event_type")
            incident = event.get("data", {})
        elif "event_type" in payload:
            # v2 format
            event_type = payload.get("event_type")
            incident = payload.get("incident", {})
        else:
            logger.error("Unable to determine webhook format")
            return {"error": "Unknown webhook format", "payload": payload}
        
        # Handle ping events
        if event_type == "pagey.ping":
            logger.info("Received PagerDuty ping event - webhook is working!")
            return {
                "status": "ok", 
                "event_type": event_type, 
                "message": "Ping received successfully"
            }
        
        # Handle service events (not incident-related)
        if event_type.startswith("service."):
            logger.info(f"Received service event: {event_type} - ignoring")
            return {
                "status": "ok", 
                "event_type": event_type, 
                "message": "Service event ignored"
            }
        
        incident_id = incident.get("id")
        if not incident_id:
            logger.error(f"Missing incident id in payload for event type: {event_type}")
            return {"error": "Missing incident id", "payload": payload}
        
        logger.info(f"Processing event_type: {event_type} for incident: {incident_id}")
        
        # Common data
        status = incident.get("status")
        raw_payload = json.dumps(payload)
        
        # Snowflake connection
        conn = get_snowflake_connection()
        cs = conn.cursor()
        
        try:
            if event_type == "incident.triggered":
                logger.info(f"Handling triggered incident: {incident_id}")
                
                # First, check if incident already exists (avoid duplicates)
                cs.execute("SELECT id FROM pagerduty_incidents WHERE id = ?", (incident_id,))
                existing = cs.fetchone()
                
                if existing:
                    logger.warning(f"Incident {incident_id} already exists, updating instead")
                else:
                    # Insert new incident
                    cs.execute("""
                        INSERT INTO pagerduty_incidents 
                        (id, title, status, service, urgency, created_at, assignments, raw_payload) 
                        VALUES (?, ?, ?, ?, ?, ?, PARSE_JSON(?), PARSE_JSON(?))
                    """, (
                        incident_id,
                        incident.get("title"),
                        status,
                        incident.get("service", {}).get("summary") if incident.get("service") else None,
                        incident.get("urgency"),
                        incident.get("created_at"),
                        json.dumps(incident.get("assignees", [])),
                        raw_payload
                    ))
                    logger.info(f"Successfully inserted new incident: {incident_id}")
            
            else:
                # Handle all update events
                logger.info(f"Handling update event: {event_type} for incident: {incident_id}")
                
                # Extract resolution information from the entire payload
                resolution_note = extract_resolution_info_from_payload(payload, incident)
                
                # Parse RCA information
                rca_1, rca_2, business = parse_resolution_note(resolution_note)
                
                # Check if incident exists first
                cs.execute("SELECT id FROM pagerduty_incidents WHERE id = ?", (incident_id,))
                existing = cs.fetchone()
                
                if existing:
                    # Update existing incident
                    update_sql = """
                        UPDATE pagerduty_incidents 
                        SET status = ?,
                            title = ?,
                            service = ?,
                            urgency = ?,
                            assignments = PARSE_JSON(?),
                            raw_payload = PARSE_JSON(?),
                            updated_at = CURRENT_TIMESTAMP()
                    """
                    update_params = [
                        status,
                        incident.get("title"),
                        incident.get("service", {}).get("summary") if incident.get("service") else None,
                        incident.get("urgency"),
                        json.dumps(incident.get("assignees", [])),
                        raw_payload
                    ]
                    
                    # Only update RCA fields if we found values
                    if rca_1 is not None:
                        update_sql += ", rca_1 = ?"
                        update_params.append(rca_1)
                    if rca_2 is not None:
                        update_sql += ", rca_2 = ?"
                        update_params.append(rca_2)
                    if business is not None:
                        update_sql += ", business_justification = ?"
                        update_params.append(business)
                    
                    update_sql += " WHERE id = ?"
                    update_params.append(incident_id)
                    
                    logger.info(f"Executing update SQL: {update_sql}")
                    logger.info(f"Update parameters: {update_params}")
                    
                    cs.execute(update_sql, update_params)
                    rows_affected = cs.rowcount
                    logger.info(f"Updated incident {incident_id}, rows affected: {rows_affected}")
                    
                else:
                    # Insert as new incident if it doesn't exist
                    logger.warning(f"Incident {incident_id} not found, inserting as new")
                    cs.execute("""
                        INSERT INTO pagerduty_incidents 
                        (id, title, status, service, urgency, created_at, assignments, rca_1, rca_2, business_justification, raw_payload) 
                        VALUES (?, ?, ?, ?, ?, ?, PARSE_JSON(?), ?, ?, ?, PARSE_JSON(?))
                    """, (
                        incident_id,
                        incident.get("title"),
                        status,
                        incident.get("service", {}).get("summary") if incident.get("service") else None,
                        incident.get("urgency"),
                        incident.get("created_at") or datetime.utcnow().isoformat(),
                        json.dumps(incident.get("assignees", [])),
                        rca_1,
                        rca_2,
                        business,
                        raw_payload
                    ))
            
            conn.commit()
            logger.info(f"Successfully processed {event_type} for incident {incident_id}")
            
        except Exception as db_error:
            logger.error(f"Database error: {str(db_error)}")
            logger.error(f"Error details: {repr(db_error)}")
            conn.rollback()
            raise
        
        finally:
            cs.close()
            conn.close()
        
        return {
            "status": "ok", 
            "event_type": event_type, 
            "incident_id": incident_id,
            "rca_parsed": {
                "rca_1": rca_1,
                "rca_2": rca_2,
                "business_justification": business
            }
        }
    
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {str(e)}")
        return {"error": f"Invalid JSON: {str(e)}", "status": "error"}
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        logger.error(f"Error details: {repr(e)}")
        return {"error": str(e), "status": "error"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/test-db")
async def test_database():
    """Test database connection"""
    try:
        conn = get_snowflake_connection()
        cs = conn.cursor()
        cs.execute("SELECT CURRENT_TIMESTAMP()")
        result = cs.fetchone()
        cs.close()
        conn.close()
        return {"status": "db_ok", "timestamp": str(result[0])}
    except Exception as e:
        return {"status": "db_error", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
