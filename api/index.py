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
    rca1, rca2, business = None, None, None
    
    if event_type == "incident.resolved":
        rca1, rca2, business = parse_rca(incident.get("resolve_reason"))
    elif event_type == "incident.annotated" and annotation_content:
        rca1, rca2, business = parse_rca(annotation_content)
    
    now = datetime.utcnow()
    
    with get_conn() as conn:
        conn.cursor().execute("""
            MERGE INTO pagerduty_incidents t USING (
                SELECT %s as INCIDENT_ID
            ) s ON t.INCIDENT_ID = s.INCIDENT_ID
            WHEN MATCHED THEN UPDATE SET
                INCIDENT_TITLE = %s,
                INCIDENT_DESCRIPTION = %s,
                INCIDENT_STATUS = %s,
                INCIDENT_SERVICE_SUMMARY = %s,
                INCIDENT_LAST_STATUS_CHANGE_AT = %s,
                INCIDENT_IS_MERGEABLE = %s,
                INCIDENT_RESOLVE_REASON_TYPE = %s,
                INCIDENT_RESOLVE_REASON_ID = %s,
                rca_1 = COALESCE(%s, rca_1),
                rca_2 = COALESCE(%s, rca_2),
                business_justification = COALESCE(%s, business_justification),
                ETL_UPDATE_REC_DTTM = %s,
                ETL_UPDATE_USER_ID = 'PAGERDUTY_WEBHOOK'
            WHEN NOT MATCHED THEN INSERT (
                INCIDENT_NUMBER, INCIDENT_TITLE, INCIDENT_DESCRIPTION, INCIDENT_CREATED_AT,
                INCIDENT_STATUS, INCIDENT_SERVICE_SUMMARY, INCIDENT_LAST_STATUS_CHANGE_AT,
                INCIDENT_IS_MERGEABLE, INCIDENT_ID, INCIDENT_RESOLVE_REASON_TYPE,
                INCIDENT_RESOLVE_REASON_ID, FILENAME, AS_ON_DATE, ETL_BATCH_ID,
                ETL_INSERT_REC_DTTM, ETL_INSERT_USER_ID, rca_1, rca_2, business_justification
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, [
            # MERGE condition
            incident.get("id"),
            # UPDATE values
            incident.get("title"), incident.get("description"), incident.get("status"),
            incident.get("service", {}).get("summary"), incident.get("last_status_change_at"),
            incident.get("is_mergeable"), incident.get("resolve_reason", {}).get("type"),
            incident.get("resolve_reason", {}).get("id"), rca1, rca2, business, now,
            # INSERT values
            incident.get("incident_number"), incident.get("title"), incident.get("description"),
            incident.get("created_at"), incident.get("status"), incident.get("service", {}).get("summary"),
            incident.get("last_status_change_at"), incident.get("is_mergeable"), incident.get("id"),
            incident.get("resolve_reason", {}).get("type"), incident.get("resolve_reason", {}).get("id"),
            f"pagerduty_{now.strftime('%Y%m%d')}.json", now.date(), f"BATCH_{now.strftime('%Y%m%d_%H%M%S')}",
            now, 'PAGERDUTY_WEBHOOK', rca1, rca2, business
        ])
        conn.commit()

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
