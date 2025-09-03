from fastapi import FastAPI, Request
import snowflake.connector, json, re
from datetime import datetime

app = FastAPI()

def parse_rca(note):
    if not note: return None, None, None
    rca1 = re.search(r"rca1:\s*(.+?)(?:\s*rca2:|$)", note, re.I | re.DOTALL)
    rca2 = re.search(r"rca2:\s*(.+?)(?:\s*business_justification:|$)", note, re.I | re.DOTALL)
    business = re.search(r"business_justification:\s*(.+?)$", note, re.I | re.DOTALL)
    return (rca1.group(1).strip() if rca1 else None,
            rca2.group(1).strip() if rca2 else None,
            business.group(1).strip() if business else None)

def get_conn():
    return snowflake.connector.connect(
        user="SVCDQM", password="LOGINuSER13579", account="NXKZZIV-WN17856",
        warehouse="COMPUTE_WH", database="DEV_DWDB", schema="DT_OPS"
    )

def upsert_incident(incident, event_type, annotation_content=None):
    print(f"🔧 Starting upsert for incident: {incident.get('id')}, event: {event_type}")
    
    rca1, rca2, business = None, None, None
    
    if event_type == "incident.resolved":
        resolve_reason = incident.get("resolve_reason")
        print(f"🔍 Resolve reason: {resolve_reason}")
        rca1, rca2, business = parse_rca(resolve_reason)
    elif event_type == "incident.annotated" and annotation_content:
        print(f"📝 Parsing annotation: {annotation_content}")
        rca1, rca2, business = parse_rca(annotation_content)
    
    print(f"📋 Parsed RCA - RCA1: {rca1}, RCA2: {rca2}, Business: {business}")
    
    now = datetime.utcnow()
    incident_id = incident.get("id")
    
    # Extract priority string from dict
    priority_obj = incident.get("priority", {})
    priority = priority_obj.get("summary") if isinstance(priority_obj, dict) else priority_obj
    
    # Calculate closed timestamp for resolved incidents
    closed_timestamp = now if incident.get("status") == "resolved" else None
    
    print(f"🔌 Connecting to Snowflake...")
    conn = get_conn()
    cs = conn.cursor()
    print(f"✅ Connected to Snowflake")
    
    try:
        # Check if exists
        print(f"🔍 Checking if incident {incident_id} exists...")
        cs.execute("SELECT INCIDENT_ID FROM pagerduty_incidents WHERE INCIDENT_ID = %s", (incident_id,))
        exists = cs.fetchone()
        print(f"📊 Exists check result: {exists}")
        
        if exists:
            # Update
            print(f"🔄 Updating existing incident {incident_id}")
            cs.execute("""
                UPDATE pagerduty_incidents SET
                    INCIDENT_NUMBER = %s,
                    INCIDENT_TITLE = %s,
                    INCIDENT_DESCRIPTION = %s,
                    INCIDENT_STATUS = %s,
                    INCIDENT_SERVICE_SUMMARY = %s,
                    INCIDENT_LAST_STATUS_CHANGE_AT = %s,
                    INCIDENT_IS_MERGEABLE = %s,
                    PRIORITY = %s,
                    INCIDENT_CLOSED_TIMESTAMP = COALESCE(%s, INCIDENT_CLOSED_TIMESTAMP),
                    rca_1 = COALESCE(%s, rca_1),
                    rca_2 = COALESCE(%s, rca_2),
                    business_justification = COALESCE(%s, business_justification),
                    ETL_UPDATE_REC_DTTM = %s,
                    ETL_UPDATE_USER_ID = %s
                WHERE INCIDENT_ID = %s
            """, (
                incident.get("incident_number"), incident.get("title"), incident.get("description"),
                incident.get("status"), incident.get("service", {}).get("summary"),
                incident.get("last_status_change_at"), incident.get("is_mergeable"),
                priority, closed_timestamp,
                rca1, rca2, business, now, 'PAGERDUTY_WEBHOOK', incident_id
            ))
            print(f"📝 Update query executed")
        else:
            # Insert
            print(f"➕ Inserting new incident {incident_id}")
            print(f"📋 Insert values - Number: {incident.get('incident_number')}, Title: {incident.get('title')}, Priority: {priority}")
            cs.execute("""
                INSERT INTO pagerduty_incidents (
                    INCIDENT_ID, INCIDENT_NUMBER, INCIDENT_TITLE, INCIDENT_DESCRIPTION, 
                    INCIDENT_CREATED_AT, INCIDENT_STATUS, INCIDENT_SERVICE_SUMMARY, 
                    INCIDENT_LAST_STATUS_CHANGE_AT, INCIDENT_IS_MERGEABLE, PRIORITY,
                    INCIDENT_CLOSED_TIMESTAMP, AS_ON_DATE, ETL_INSERT_REC_DTTM, 
                    ETL_INSERT_USER_ID, rca_1, rca_2, business_justification
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                incident_id, incident.get("incident_number"), incident.get("title"), 
                incident.get("description"), incident.get("created_at"), incident.get("status"),
                incident.get("service", {}).get("summary"), incident.get("last_status_change_at"),
                incident.get("is_mergeable"), priority, closed_timestamp,
                now.date(), now, 'PAGERDUTY_WEBHOOK', rca1, rca2, business
            ))
            print(f"📝 Insert query executed")
        
        print(f"💾 Committing transaction...")
        conn.commit()
        print(f"✅ {'Updated' if exists else 'Inserted'} incident {incident_id}")
        
    except Exception as e:
        print(f"❌ Database error: {str(e)}")
        print(f"🔄 Rolling back transaction...")
        conn.rollback()
        raise
    finally:
        print(f"🔒 Closing Snowflake connection...")
        cs.close()
        conn.close()
        print(f"🔒 Connection closed")

@app.post("/pagerduty")
async def webhook(request: Request):
    payload = await request.json()
    event = payload.get("event", {})
    event_type = event.get("event_type")
    
    if event_type == "pagey.ping":
        return {"status": "ok"}
    
    data = event.get("data", {})
    incident = data.get("incident", {}) if event_type == "incident.annotated" else data
    
    if not incident.get("id"):
        return {"error": "Missing incident id"}
    
    annotation_content = data.get("content") if event_type == "incident.annotated" else None
    upsert_incident(incident, event_type, annotation_content)
    
    return {"status": "ok", "incident_id": incident.get("id")}

@app.get("/health")
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


