from __future__ import annotations

from datetime import datetime, timedelta, time
import re
import uuid
from zoneinfo import ZoneInfo

import pandas as pd
from ics import Calendar, Event
from prefect import flow


# =========================
# CONFIG
# =========================
INPUT_XLSX = "earnings_calendar.xlsx"
SHEET_NAME = "NextPerTicker"
OUTPUT_ICS = r"Q:\Bloomberg Data\Calendar\docs\earnings_outlook.ics"

# DuraÃ§Ã£o do evento no calendÃ¡rio
EVENT_DURATION_MIN = 30

# Timezone para NÃƒO deslocar 3 horas no Outlook
TZ = ZoneInfo("America/Sao_Paulo")

# HorÃ¡rios default quando Bloomberg traz janela (Aft-mkt/Bef-mkt/Dur-mkt)
DEFAULT_TIME_MAP = {
    "AFT-MKT": time(18, 0),
    "BEF-MKT": time(8, 0),
    "DUR-MKT": time(12, 0),
}

# Texto bonito no tÃ­tulo
MARKET_PT = {
    "AFT-MKT": "Pós-mercado",
    "BEF-MKT": "Pré-mercado",
    "DUR-MKT": "Durante mercado",
}

Cobertura_Arthur = ['Bancos', 'Financials Ex-Bancos', 'Consumo - Miguel', 'Properties', 'Homebuilders', 'Staples', 'Retail', 'E-commerce', 'Healthcare']
Cobertura_Paulo = ['Elétricas', 'Rodovias + Logistics Infrastructure', 'Industrials + Gestão de Resí­duos', 'Metals & Mining', 'Pulp & Paper', 'TMT', 'Oil & Gas', 'Rentals']

def remove_numbers_from_ticker(ticker: str) -> str:
    return re.sub(r"\d+", "", ticker)


def normalize_market_tag(raw: str | None) -> str | None:
    if raw is None or pd.isna(raw):
        return None
    s = str(raw).strip()
    if not s:
        return None

    u = s.upper().replace(" ", "").replace("_", "-")
    u = u.replace("AFTMKT", "AFT-MKT").replace("BEFMKT", "BEF-MKT").replace("DURMKT", "DUR-MKT")
    return u if u in DEFAULT_TIME_MAP else None


def parse_hhmm(raw: str | None) -> time | None:
    if raw is None or pd.isna(raw):
        return None
    s = str(raw).strip()
    if not s:
        return None
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if 0 <= h <= 23 and 0 <= mi <= 59:
        return time(h, mi)
    return None


def stable_uid(*parts: str) -> str:
    base = "|".join(parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))


def pick_hour_col(df: pd.DataFrame) -> str | None:
    if "Hora" in df.columns:
        return "Hora"
    if "Hora (raw)" in df.columns:
        return "Hora (raw)"
    if "announcement_time" in df.columns:
        return "announcement_time"
    return None

@flow(name="Rotina de Atualização Calendario Final (ICS)", log_prints=True)
def automation_calendar_flow():
    df = pd.read_excel(INPUT_XLSX, sheet_name=SHEET_NAME)

    if "Ticker" not in df.columns or "Data" not in df.columns:
        raise ValueError(f"Preciso de colunas 'Ticker' e 'Data'. Colunas: {list(df.columns)}")

    hour_col = pick_hour_col(df)

    cal = Calendar()

    for _, r in df.iterrows():
        ticker = str(r.get("Ticker", "")).strip()
        Setor = str(r.get("Setor", "")).strip()
        if not ticker or ticker.lower() == "nan":
            continue

        ticker = remove_numbers_from_ticker(ticker)
        start_dt = None 

        # Data
        dts = pd.to_datetime(r.get("Data"), errors="coerce")
        if pd.isna(dts):
            continue
        d = dts.date()

        # Hora / janela
        raw_time = None if hour_col is None else r.get(hour_col)
        raw_time_str = None if raw_time is None or pd.isna(raw_time) else str(raw_time).strip()

        hhmm = parse_hhmm(raw_time_str)
        mkt = normalize_market_tag(raw_time_str)

        print(Setor, r.get("Hora (raw)"), r.get("Hora (parsed)"), start_dt)


        if Setor in Cobertura_Arthur:
            start_dt = datetime.combine(d, time(7, 0)).replace(tzinfo=TZ)

        if Setor in Cobertura_Paulo:
            if mkt in ['BEF-MKT']:
               start_dt = datetime.combine(
                d - timedelta(days=1),
                time(8, 0),
                tzinfo=TZ
            )
            else:
                start_dt = datetime.combine(d, time(8, 0)).replace(tzinfo=TZ)

        if Setor in Cobertura_Arthur:
            if mkt in ['BEF-MKT']:
                start_dt = datetime.combine(
                            d - timedelta(days=1),
                            time(7, 0),
                            tzinfo=TZ
                        )
            else:
                start_dt = datetime.combine(d, time(7, 0)).replace(tzinfo=TZ) 

        if start_dt is None:
            # fallback padrão (ex: 08:00 no dia d)
            start_dt = datetime.combine(d, time(8, 0), tzinfo=TZ)

        if mkt is not None:
            title = f"[{ticker}] - Divulgação de Resultados 4Q25 ({MARKET_PT.get(mkt, mkt)})"
            time_label = raw_time_str  # "Aft-mkt"
        else:
            # Sem hora: posiciona meio-dia
            title = f"[{ticker}] - Divulgação de Resultados 4Q25 (Pós-mercado)"
            time_label = ""

        # Metadados opcionais
        period = str(r.get("Período", "")).strip() if "Período" in df.columns else ""
        sector = str(r.get("Setor", "")).strip() if "Setor" in df.columns else ""

        # DescriÃ§Ã£o (aqui pode ter tudo)
        desc = [
            f"Ticker: {ticker}",
            f"Data: {d.isoformat()}",
        ]
        if time_label:
            desc.append(f"Horário/Janela: {time_label}")
        if period and period.lower() != "nan":
            desc.append(f"Período: {period}")
        if sector and sector.lower() != "nan":
            desc.append(f"Setor: {sector}")
        description = "\n".join(desc)

        e = Event()
        e.name = title
        e.begin = start_dt
        e.end = start_dt + timedelta(minutes=EVENT_DURATION_MIN)
        e.description = description

        # UID estÃ¡vel (evita duplicar se reimportar/subscribir)
        e.uid = stable_uid(ticker, period, d.isoformat(), str(time_label))

        # Categoria (opcional)
        if sector and sector.lower() != "nan":
            e.categories = {sector}

        cal.events.add(e)

    with open(OUTPUT_ICS, "w", encoding="utf-8") as f:
        f.writelines(cal.serialize_iter())

    print(f"[OK] Gerado: {OUTPUT_ICS} com {len(cal.events)} eventos (TZ={TZ}).")

if __name__ == "__main__":
    automation_calendar_flow()