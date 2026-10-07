RCA_SYSTEM = """You are a platform SRE assistant. Analyze one RHEL syslog line.
Respond with compact JSON only:
{"root_cause":"...","subsystem":"...","summary":"...","recommended_action":"..."}
Do not invent host facts that are not in the message."""

SEVERITY_SYSTEM = """Score operational severity of one event.
Respond with compact JSON only:
{"severity":"INFO|WARNING|CRITICAL","score":<0-100 integer>,"rationale":"..."}"""

METRIC_ALERT_SYSTEM = """Explain a predictive storage exhaustion alert.
Respond with compact JSON only:
{"severity":"WARNING|CRITICAL","score":<0-100 integer>,"root_cause":"...","summary":"...","recommended_action":"..."}"""


def rca_messages(*, host: str, syslogtag: str, facility: str, severity: str, message: str):
    user = (
        f"host={host}\nsyslogtag={syslogtag}\nfacility={facility}\n"
        f"severity={severity}\nmessage={message}\n"
    )
    return [
        {"role": "system", "content": RCA_SYSTEM},
        {"role": "user", "content": user},
    ]


def severity_messages(*, host: str, event_context: str, message: str):
    user = f"host={host}\ncontext={event_context}\nmessage={message}\n"
    return [
        {"role": "system", "content": SEVERITY_SYSTEM},
        {"role": "user", "content": user},
    ]


def metric_alert_messages(
    *,
    host: str,
    instance: str,
    used: float,
    capacity: float,
    rate_per_second: float,
    tte_seconds: float,
):
    user = (
        f"host={host} instance={instance} used={used} capacity={capacity} "
        f"rate_per_second={rate_per_second} tte_seconds={tte_seconds}\n"
    )
    return [
        {"role": "system", "content": METRIC_ALERT_SYSTEM},
        {"role": "user", "content": user},
    ]
