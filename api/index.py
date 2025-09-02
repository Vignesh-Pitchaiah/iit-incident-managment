from fastapi import FastAPI, Request
import snowflake.connector
import json
import os
import re
import logging

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_resolution_note(note: str):
    """Parse RCA and business justification from resolution notes"""
    rca1 = rca2 = business = None
    if note:
        # More flexible regex patterns
        m1 = re.search(r"rca_?1[:\s]+(.*?)(?:\n|rca_?2|business|$)", note, re.IGNORECASE | re.DOTALL)
        m2 = re.search(r"rca_?2[:\s]+(.*?)(?:\n|business|$)", note, re.IGNORECASE | re.DOTALL)
        m3 = re.search(r"business_?justification[:\s]+(.*?)(?:\n|$)", note, re.IGNORECASE | re.DOTALL)
        
        rca1 = m1.group(1).strip() if m1 else None
        rca2 = m2.group(1).strip() if m2 else None
        business = m3.group(1).strip() if m3 else None
    
    logger.info(f"Parsed resolution note - RCA1: {rca1}, RCA2: {rca2}, Business: {business}")
    return rca1, rca2, business

def get_snowflake_connection():
    """Create Snowflake connection"""
    return snowflake.connector.connect(
        user="SVCDQM",
        password="LOGINuSER13579",  # Consider using environment variables
        account="NXKZZIV-WN17856",
        warehouse="COMPUTE_WH",
        database="DEV_DWDB",
        schema="DT_OPS"
    )

@app.post("/pagerduty")
async def ingest_incident(request: Request):
    try:
        payload = await request.json()
        logger.info(f"Received webhook: {json.dumps(payload, indent=2)}")
        
        # Handle both v1 and v2 webhook formats
        if "event" in payload:
            # v1 format
            event = payload.get("event", {})
            event_type = event.get("event_type")
            incident = event.get("data", {})
        else:
            # v2 format
            event_type = payload.get("event_type")
            incident = payload.get("incident", {})
        
        incident_id = incident.get("id")
        if not incident_id:
            logger.error(f"Missing incident id in payload: {payload}")
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
                # Insert new incident
                cs.execute("""
                    INSERT INTO pagerduty_incidents 
                    (id, title, status, service, urgency, created_at, assignments, raw_payload) 
                    VALUES (%s, %s, %s, %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s))
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
                logger.info(f"Inserted new incident: {incident_id}")
            
            elif event_type in ["incident.resolved", "incident.acknowledged", "incident.escalated", "incident.assigned", "incident.unacknowledged", "incident.delegated", "incident.priority_updated", "incident.responder_added", "incident.responder_replied", "incident.status_update_published", "incident.reopened"]:
                # Check multiple possible fields for resolution notes
                resolution_note = ""
                
                # Check various fields where RCA info might be stored
                possible_fields = [
                    incident.get("resolve_reason", ""),
                    incident.get("description", ""),
                    incident.get("body", ""),
                ]
                
                # Check last_status_change_by notes
                if incident.get("last_status_change_by"):
                    last_change = incident.get("last_status_change_by", {})
                    if isinstance(last_change, dict):
                        possible_fields.append(last_change.get("summary", ""))
                
                # Check incident notes/timeline
                if incident.get("incidents_responders"):
                    for responder in incident.get("incidents_responders", []):
                        if responder.get("message"):
                            possible_fields.append(responder.get("message", ""))
                
                # Combine all possible sources
                resolution_note = " ".join(filter(None, possible_fields))
                
                logger.info(f"Resolution note sources: {resolution_note}")
                
                # Parse RCA information
                rca_1, rca_2, business = parse_resolution_note(resolution_note)
                
                # Update existing incident
                cs.execute("""
                    UPDATE pagerduty_incidents 
                    SET status=%s,
                        title=%s,
                        service=%s,
                        urgency=%s,
                        assignments=PARSE_JSON(%s),
                        rca_1=%s,
                        rca_2=%s,
                        business_justification=%s,
                        raw_payload=PARSE_JSON(%s),
                        updated_at=CURRENT_TIMESTAMP()
                    WHERE id=%s
                """, (
                    status,
                    incident.get("title"),
                    incident.get("service", {}).get("summary") if incident.get("service") else None,
                    incident.get("urgency"),
                    json.dumps(incident.get("assignees", [])),
                    rca_1,
                    rca_2,
                    business,
                    raw_payload,
                    incident_id
                ))
                
                rows_affected = cs.rowcount
                logger.info(f"Updated incident {incident_id}, rows affected: {rows_affected}")
                
                if rows_affected == 0:
                    # If update didn't affect any rows, the incident might not exist
                    # Insert it as a new incident
                    logger.warning(f"Incident {incident_id} not found for update, inserting as new")
                    cs.execute("""
                        INSERT INTO pagerduty_incidents 
                        (id, title, status, service, urgency, created_at, assignments, rca_1, rca_2, business_justification, raw_payload) 
                        VALUES (%s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, PARSE_JSON(%s))
                    """, (
                        incident_id,
                        incident.get("title"),
                        status,
                        incident.get("service", {}).get("summary") if incident.get("service") else None,
                        incident.get("urgency"),
                        incident.get("created_at"),
                        json.dumps(incident.get("assignees", [])),
                        rca_1,
                        rca_2,
                        business,
                        raw_payload
                    ))
            
            else:
                logger.warning(f"Unhandled event type: {event_type}")
            
            conn.commit()
            logger.info(f"Successfully processed {event_type} for incident {incident_id}")
            
        except Exception as db_error:
            logger.error(f"Database error: {str(db_error)}")
            conn.rollback()
            raise
        
        finally:
            cs.close()
            conn.close()
        
        return {"status": "ok", "event_type": event_type, "incident_id": incident_id}
    
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return {"error": str(e), "status": "error"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
