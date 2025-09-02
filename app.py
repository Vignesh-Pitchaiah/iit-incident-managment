from fastapi import FastAPI, Request
import snowflake.connector
import json
import re

app = FastAPI()

def parse_resolution_note(note: str):
    rca1 = rca2 = business = None
    if note:
        match1 = re.search(r"rca1:\s*(.*)", note, re.IGNORECASE)
        match2 = re.search(r"rca2:\s*(.*)", note, re.IGNORECASE)
        match3 = re.search(r"business_justification:\s*(.*)", note, re.IGNORECASE)
        if match1:
            rca1 = match1.group(1).strip()
        if match2:
            rca2 = match2.group(1).strip()
        if match3:
            business = match3.group(1).strip()
    return rca1, rca2, business


@app.post("/pagerduty")
async def ingest_incident(request: Request):
    payload = await request.json()
    event = payload.get("event", {})
    incident = event.get("data", {})

    # Extract fields safely
    id = incident.get("id")  # Incident ID
    if not id:
        return {"error": "No incident id in payload", "payload": payload}

    title = incident.get("title")
    status = incident.get("status")
    service = incident.get("service", {}).get("summary")
    urgency = incident.get("urgency")
    created_at = incident.get("created_at")
    assignments = json.dumps(incident.get("assignees", []))

    # Resolution note (only on resolved events)
    resolution_note = incident.get("resolution")
    rca_1, rca_2, business_justification = parse_resolution_note(resolution_note)

    raw_payload = json.dumps(payload)

    # Insert into Snowflake
    conn = snowflake.connector.connect(
        user="SVCDQM",
        password="LOGINuSER13579",
        account="NXKZZIV-WN17856",
        warehouse="COMPUTE_WH",
        database="DEV_DWDB",
        schema="DT_OPS"
    )
    cs = conn.cursor()
    cs.execute("""
        INSERT INTO pagerduty_incidents
        (id, title, status, service, urgency, created_at, assignments, rca_1, rca_2, business_justification, raw_payload)
        SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, PARSE_JSON(%s)
    """, (
        id, title, status, service, urgency, created_at,
        assignments, rca_1, rca_2, business_justification, raw_payload
    ))
    cs.close()
    conn.close()

    return {"status": "ok", "incident_id": id}


@app.get("/pagerduty")
async def test_endpoint():
    return {"message": "PagerDuty ingestion endpoint is alive"}


