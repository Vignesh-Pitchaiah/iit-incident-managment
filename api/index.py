from fastapi import FastAPI, Request
import snowflake.connector, json, os, re

app = FastAPI()

def parse_resolution_note(note: str):
    rca1 = rca2 = business = None
    if note:
        m1 = re.search(r"rca1:\s*(.*)", note, re.IGNORECASE)
        m2 = re.search(r"rca2:\s*(.*)", note, re.IGNORECASE)
        m3 = re.search(r"business_justification:\s*(.*)", note, re.IGNORECASE)
        rca1 = m1.group(1).strip() if m1 else None
        rca2 = m2.group(1).strip() if m2 else None
        business = m3.group(1).strip() if m3 else None
    return rca1, rca2, business

@app.post("/pagerduty")
async def ingest_incident(request: Request):
    payload = await request.json()
    print("🔔 Incoming PagerDuty payload:", json.dumps(payload, indent=2))
    
    event = payload.get("event", {})
    event_type = event.get("event_type")
    incident = event.get("data", {})
    
    # Handle ping events
    if event_type == "pagey.ping":
        print("🏓 Ping event received - webhook working!")
        return {"status": "ok", "event_type": event_type, "message": "ping received"}
    
    print(f"📌 Event type: {event_type}")
    print(f"📌 Incident data: {incident}")
    
    incident_id = incident.get("id")
    if not incident_id:
        print("❌ Missing incident id")
        return {"error": "Missing incident id", "payload": payload}
    
    status = incident.get("status")
    raw_payload = json.dumps(payload)
    
    print(f"➡️ Processing incident {incident_id} with status: {status}")
    
    # Snowflake connection
    conn = snowflake.connector.connect(
        user="SVCDQM",
        password="LOGINuSER13579",
        account="NXKZZIV-WN17856",
        warehouse="COMPUTE_WH",
        database="DEV_DWDB",
        schema="DT_OPS"
    )
    cs = conn.cursor()
    print("✅ Connected to Snowflake")
    
    try:
        if event_type == "incident.triggered":
            print(f"🟢 Inserting new incident {incident_id}")
            cs.execute("""
                INSERT INTO pagerduty_incidents
                (id, title, status, service, urgency, created_at, assignments, raw_payload)
                VALUES (?, ?, ?, ?, ?, ?, PARSE_JSON(?), PARSE_JSON(?))
            """, (
                incident_id,
                incident.get("title"),
                status,
                incident.get("service", {}).get("summary"),
                incident.get("urgency"),
                incident.get("created_at"),
                json.dumps(incident.get("assignees", [])),
                raw_payload
            ))
            
        elif event_type == "incident.resolved":
            note_text = incident.get("resolve_reason", "")
            print(f"🟡 Parsing resolution note: {note_text}")
            rca_1, rca_2, business = parse_resolution_note(note_text)
            print(f"📝 Extracted RCA1: {rca_1}, RCA2: {rca_2}, Business: {business}")
            
            cs.execute("""
                UPDATE pagerduty_incidents
                SET status=?,
                    rca_1=?,
                    rca_2=?,
                    business_justification=?,
                    raw_payload=PARSE_JSON(?)
                WHERE id=?
            """, (
                status, rca_1, rca_2, business, raw_payload, incident_id
            ))
            print(f"🔄 Updated incident {incident_id}")
            
        else:
            # Handle other incident events (acknowledged, escalated, etc.)
            print(f"🔄 Updating incident {incident_id} for event {event_type}")
            
            # Check if incident exists first
            cs.execute("SELECT id FROM pagerduty_incidents WHERE id = ?", (incident_id,))
            existing = cs.fetchone()
            
            if existing:
                # Update existing incident
                cs.execute("""
                    UPDATE pagerduty_incidents
                    SET status=?,
                        title=?,
                        service=?,
                        urgency=?,
                        assignments=PARSE_JSON(?),
                        raw_payload=PARSE_JSON(?)
                    WHERE id=?
                """, (
                    status,
                    incident.get("title"),
                    incident.get("service", {}).get("summary"),
                    incident.get("urgency"),
                    json.dumps(incident.get("assignees", [])),
                    raw_payload,
                    incident_id
                ))
                print(f"✅ Updated existing incident {incident_id}")
            else:
                # Insert as new if doesn't exist
                print(f"⚠️ Incident {incident_id} not found, inserting as new")
                cs.execute("""
                    INSERT INTO pagerduty_incidents
                    (id, title, status, service, urgency, created_at, assignments, raw_payload)
                    VALUES (?, ?, ?, ?, ?, ?, PARSE_JSON(?), PARSE_JSON(?))
                """, (
                    incident_id,
                    incident.get("title"),
                    status,
                    incident.get("service", {}).get("summary"),
                    incident.get("urgency"),
                    incident.get("created_at"),
                    json.dumps(incident.get("assignees", [])),
                    raw_payload
                ))
                print(f"✅ Inserted new incident {incident_id}")
        
        conn.commit()
        print("💾 Transaction committed")
        
    except Exception as e:
        print(f"❌ Database error: {str(e)}")
        conn.rollback()
        raise
    finally:
        cs.close()
        conn.close()
        print("🔒 Snowflake connection closed")
    
    return {"status": "ok", "event_type": event_type, "incident_id": incident_id}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
