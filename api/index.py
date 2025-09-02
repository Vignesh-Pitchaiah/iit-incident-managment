from fastapi import FastAPI, Request
import snowflake.connector
import json
import os
import re

app = FastAPI()

def parse_resolution_note(note: str):
    rca1 = rca2 = business = None
    if note:
        match1 = re.search(r"rca1:\s*(.*)", note, re.IGNORECASE)
        match2 = re.search(r"rca2:\s*(.*)", note, re.IGNORECASE)
        match3 = re.search(r"(business_justification|business):\s*(.*)", note, re.IGNORECASE)
        if match1:
            rca1 = match1.group(1).strip()
        if match2:
            rca2 = match2.group(1).strip()
        if match3:
            business = match3.group(2).strip()
    return rca1, rca2, business


@app.post("/pagerduty")
async def ingest_incident(request: Request):
    payload = await request.json()
    event_type = payload["event"]["event_type"]
    incident = payload["event"]["data"]

    # Extract base fields
    incident_id = incident["id"]
    title = incident.get("title")
    status = incident.get("status")
    service = incident.get("service", {}).get("summary")
    urgency = incident.get("urgency")
    created_at = incident.get("created_at")
    assignments = json.dumps(incident.get("assignments", []))
    raw_payload = json.dumps(payload)

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user="SVCDQM",
        password="LOGINuSER13579",
        account="NXKZZIV-WN17856",
        warehouse="COMPUTE_WH",
        database="DEV_DWDB",
        schema="DT_OPS"
    )
    cs = conn.cursor()

    if event_type == "incident.triggered":
        # Insert new record
        cs.execute("""
            INSERT INTO pagerduty_incidents
            (id, title, status, service, urgency, created_at, assignments, raw_payload)
            SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s)
        """, (
            incident_id, title, status, service, urgency,
            created_at, assignments, raw_payload
        ))

    elif event_type == "incident.resolved":
        # Extract RCA details from resolution note
        resolution = incident.get("resolve_reason") or ""
        rca_1, rca_2, business = parse_resolution_note(resolution)

        # Update existing record
        cs.execute("""
            UPDATE pagerduty_incidents
            SET status = %s,
                rca_1 = %s,
                rca_2 = %s,
                business = %s,
                raw_payload = PARSE_JSON(%s)
            WHERE id = %s
        """, (
            status, rca_1, rca_2, business, raw_payload, incident_id
        ))

    conn.commit()
    cs.close()
    conn.close()

    return {"status": "ok", "event_type": event_type}


@app.get("/pagerduty")
async def test_endpoint():
    return {"message": "PagerDuty ingestion endpoint is alive"}
