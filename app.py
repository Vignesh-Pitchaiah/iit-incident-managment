from fastapi import FastAPI, Request
import snowflake.connector
import json

app = FastAPI()

@app.post("/pagerduty")
async def ingest_incident(request: Request):
    payload = await request.json()
    incident = payload["event"]["data"]

    # Extract fields
    id = incident["id"]
    title = incident.get("title")
    status = incident.get("status")
    service = incident.get("service", {}).get("summary")
    urgency = incident.get("urgency")
    created_at = incident.get("created_at")
    assignments = json.dumps(incident.get("assignments", []))
    rca_1 = incident.get("body", {}).get("details", {}).get("rca_1")
    rca_2 = incident.get("body", {}).get("details", {}).get("rca_2")
    raw_payload = json.dumps(payload)

    # Insert into Snowflake
    conn = snowflake.connector.connect(
        user="SVCDQM",
        password="USERlOGIN13579",
        account="NXKZZIV-WN17856",
        warehouse="COMPUTE_WH",
        database="DEV_DWDB",
        schema="DT_OPS"
    )
    cs = conn.cursor()
    cs.execute("""
        INSERT INTO pagerduty_incidents
        (id, title, status, service, urgency, created_at, assignments, rca_1, rca_2, raw_payload)
        SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, PARSE_JSON(%s)
    """, (
        id, title, status, service, urgency, created_at,
        assignments, rca_1, rca_2, raw_payload
    ))
    cs.close()
    conn.close()

    return {"status": "ok"}

@app.get("/pagerduty")
async def test_endpoint():
    return {"message": "PagerDuty ingestion endpoint is alive"}
