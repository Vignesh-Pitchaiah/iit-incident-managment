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
    event = payload.get("event", {})
    event_type = event.get("event_type")
    incident = event.get("data", {})

    incident_id = incident.get("id")
    if not incident_id:
        return {"error": "Missing incident id", "payload": payload}

    # Common data
    status = incident.get("status")
    raw_payload = json.dumps(payload)

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

    if event_type == "incident.triggered":
        cs.execute("""
            INSERT INTO pagerduty_incidents
            (id, title, status, service, urgency, created_at, assignments, raw_payload)
            SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s)
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
        rca_1, rca_2, business = parse_resolution_note(incident.get("resolve_reason", ""))
        cs.execute("""
            UPDATE pagerduty_incidents
            SET status=%s,
                rca_1=%s,
                rca_2=%s,
                business_justification=%s,
                raw_payload=PARSE_JSON(%s)
            WHERE id=%s
        """, (
            status, rca_1, rca_2, business, raw_payload, incident_id
        ))

    conn.commit()
    cs.close()
    conn.close()

    return {"status": "ok", "event_type": event_type}
