# automation_calendar_improved.py
# Gera arquivo .ics para Outlook com eventos de earnings
# Melhorias: tratamento de erros, lógica de horários completa, paths flexíveis
#
# Requisitos:
#   pip install ics pandas openpyxl prefect
#
# Uso:
#   python automation_calendar_improved.py

from __future__ import annotations

from datetime import datetime, timedelta, time
from pathlib import Path
import os
import re
import uuid
from zoneinfo import ZoneInfo

import pandas as pd
from ics import Calendar, Event
from prefect import flow


# =========================
# CONFIGURAÇÕES
# =========================
INPUT_XLSX = "earnings_calendar.xlsx"
SHEET_NAME = "NextPerTicker"
OUTPUT_DIR = Path(os.path.expanduser(r"Q:\Bloomberg Data\calendar\docs"))
OUTPUT_FILENAME = "earnings_outlook.ics"

# Duração do evento no calendário (em minutos)
EVENT_DURATION_MIN = 30

# Timezone - São Paulo (não desloca 3 horas no Outlook)
TZ = ZoneInfo("America/Sao_Paulo")

# Horários default para janelas de mercado
DEFAULT_TIME_MAP = {
    "AFT-MKT": time(18, 0),  # Pós-mercado
    "BEF-MKT": time(8, 0),   # Pré-mercado
    "DUR-MKT": time(12, 0),  # Durante mercado
}

# Textos em português para as janelas
MARKET_PT = {
    "AFT-MKT": "Pós-mercado",
    "BEF-MKT": "Pré-mercado",
    "DUR-MKT": "Durante mercado",
}

# Setores por analista
COBERTURA_ARTHUR = [
    'Bancos', 'Financials Ex-Bancos', 'Consumo - Miguel', 'Properties',
    'Homebuilders', 'Staples', 'Retail', 'E-commerce', 'Healthcare'
]

COBERTURA_PAULO = [
    'Elétricas', 'Rodovias + Logistics Infrastructure',
    'Industrials + Gestão de Resíduos', 'Metals & Mining',
    'Pulp & Paper', 'TMT', 'Oil & Gas', 'Rentals'
]

# Horários de notificação por analista
HORARIO_NOTIFICACAO = {
    'Arthur': time(7, 0),
    'Paulo': time(8, 0),
}


# =========================
# FUNÇÕES AUXILIARES
# =========================

def remove_numbers_from_ticker(ticker: str) -> str:
    """Remove números do ticker (ex: PETR4 -> PETR)"""
    return re.sub(r"\d+", "", ticker)


def normalize_market_tag(raw: str | None) -> str | None:
    """Normaliza tags de mercado (Aft-mkt, Bef-mkt, etc)"""
    if raw is None or pd.isna(raw):
        return None
    
    s = str(raw).strip()
    if not s:
        return None

    # Normaliza formato
    u = s.upper().replace(" ", "").replace("_", "-")
    u = u.replace("AFTMKT", "AFT-MKT").replace("BEFMKT", "BEF-MKT").replace("DURMKT", "DUR-MKT")
    
    return u if u in DEFAULT_TIME_MAP else None


def parse_hhmm(raw: str | None) -> time | None:
    """Parse de hora no formato HH:MM"""
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
    """Gera UID estável para evitar duplicatas ao reimportar"""
    base = "|".join(parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))


def pick_hour_col(df: pd.DataFrame) -> str | None:
    """Identifica coluna de hora na planilha"""
    possible_cols = ["Hora", "Hora (raw)", "Hora (parsed)", "announcement_time"]
    for col in possible_cols:
        if col in df.columns:
            return col
    return None


def get_analyst_from_sector(setor: str) -> str | None:
    """Identifica analista responsável pelo setor"""
    if setor in COBERTURA_ARTHUR:
        return 'Arthur'
    elif setor in COBERTURA_PAULO:
        return 'Paulo'
    return None

def determine_start_time(
    d: datetime.date,
    setor: str,
    hhmm: time | None,
    mkt: str | None
) -> tuple[datetime, str]:
    """
    Determina horário de início do evento e descrição do timing

    Returns:
        tuple: (datetime com timezone, descrição do timing)
    """

    # Prioridade 1: Hora específica fornecida
    if hhmm is not None:
        start_dt = datetime.combine(d, hhmm).replace(tzinfo=TZ)
        return start_dt, f"{hhmm.strftime('%H:%M')}"

    # Prioridade 2: Janela de mercado
    if mkt is not None:
        default_time = DEFAULT_TIME_MAP[mkt]

        # 👇 REGRA: pré-mercado acontece no dia anterior
        event_date = d
        if mkt.lower() in {"aft-mkt"}:
            event_date = d - timedelta(days=1)

        start_dt = datetime.combine(event_date, default_time).replace(tzinfo=TZ)
        return start_dt, MARKET_PT.get(mkt, mkt)

    # Prioridade 3: Horário por analista/setor
    analyst = get_analyst_from_sector(setor)
    if analyst:
        notify_time = HORARIO_NOTIFICACAO[analyst]
        start_dt = datetime.combine(d, notify_time).replace(tzinfo=TZ)
        return start_dt, f"Notificação {analyst} ({notify_time.strftime('%H:%M')})"

    # Fallback: Pós-mercado às 18h
    start_dt = datetime.combine(d, time(18, 0)).replace(tzinfo=TZ)
    return start_dt, "Pós-mercado"



def extract_period_quarter(periodo: str | None) -> str:
    """Extrai quarter do período (ex: '4T24' -> '4Q24')"""
    if not periodo or pd.isna(periodo):
        return "Resultados"
    
    periodo_str = str(periodo).strip()
    
    # Já está no formato correto (4Q24)
    if re.match(r'\d+Q\d{2}', periodo_str):
        return periodo_str
    
    # Converte 4T24 -> 4Q24
    match = re.match(r'(\d+)T(\d{2})', periodo_str)
    if match:
        return f"{match.group(1)}Q{match.group(2)}"
    
    return periodo_str


# =========================
# FLUXO PRINCIPAL
# =========================

@flow(name="Automação Calendário Earnings (ICS)", log_prints=True)
def automation_calendar_flow():
    """
    Fluxo principal: lê Excel e gera arquivo .ics para Outlook
    """
    
    # Validar entrada
    input_path = Path(INPUT_XLSX)
    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {INPUT_XLSX}")
    
    print(f"📊 Lendo arquivo: {INPUT_XLSX}")
    df = pd.read_excel(input_path, sheet_name=SHEET_NAME)
    
    # Validar colunas obrigatórias
    required_cols = ["Ticker", "Data"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Colunas obrigatórias faltando: {missing_cols}. Colunas disponíveis: {list(df.columns)}")
    
    hour_col = pick_hour_col(df)
    if hour_col:
        print(f"✓ Coluna de hora identificada: {hour_col}")
    else:
        print("⚠ Nenhuma coluna de hora encontrada - usando horários padrão")
    
    # Criar calendário
    cal = Calendar()
    eventos_criados = 0
    eventos_ignorados = 0
    
    for idx, row in df.iterrows():
        ticker = str(row.get("Ticker", "")).strip()
        setor = str(row.get("Setor", "")).strip() if "Setor" in df.columns else ""
        periodo = str(row.get("Período", "")).strip() if "Período" in df.columns else None
        
        # Validar ticker
        if not ticker or ticker.lower() == "nan":
            eventos_ignorados += 1
            continue
        
        ticker_clean = remove_numbers_from_ticker(ticker)
        
        # Validar data
        data_ts = pd.to_datetime(row.get("Data"), errors="coerce")
        if pd.isna(data_ts):
            print(f"⚠ Data inválida para {ticker} - ignorando")
            eventos_ignorados += 1
            continue
        
        data = data_ts.date()
        
        # Processar hora/janela
        raw_time = None if hour_col is None else row.get(hour_col)
        raw_time_str = None if raw_time is None or pd.isna(raw_time) else str(raw_time).strip()
        
        hhmm = parse_hhmm(raw_time_str)
        mkt = normalize_market_tag(raw_time_str)
        
        # Determinar horário de início
        start_dt, time_desc = determine_start_time(data, setor, hhmm, mkt)
        
        # Montar título
        period_label = extract_period_quarter(periodo)
        title = f"[{ticker_clean}] - Divulgação de {period_label}"
        if time_desc and time_desc not in ["Resultados"]:
            title += f" ({time_desc})"
        
        # Montar descrição
        desc_lines = [
            f"Ticker: {ticker_clean}",
            f"Data: {data.isoformat()}",
            f"Horário: {time_desc}",
        ]
        
        if periodo and periodo.lower() != "nan":
            desc_lines.append(f"Período: {periodo}")
        
        if setor and setor.lower() != "nan":
            desc_lines.append(f"Setor: {setor}")
            analyst = get_analyst_from_sector(setor)
            if analyst:
                desc_lines.append(f"Analista: {analyst}")
        
        description = "\n".join(desc_lines)
        
        # Criar evento
        event = Event()
        event.name = title
        event.begin = start_dt
        event.end = start_dt + timedelta(minutes=EVENT_DURATION_MIN)
        event.description = description
        event.uid = stable_uid(ticker_clean, str(periodo), data.isoformat(), time_desc)
        
        # Adicionar categoria (setor)
        if setor and setor.lower() != "nan":
            event.categories = {setor}
        
        cal.events.add(event)
        eventos_criados += 1
    
    # Garantir que diretório de saída existe
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Salvar arquivo
    output_path = output_dir / OUTPUT_FILENAME
    with open(output_path, "w", encoding="utf-8") as f:
        f.writelines(cal.serialize_iter())
    
    print(f"\n✅ Calendário gerado com sucesso!")
    print(f"📁 Arquivo: {output_path}")
    print(f"📅 Eventos criados: {eventos_criados}")
    if eventos_ignorados > 0:
        print(f"⚠ Eventos ignorados: {eventos_ignorados}")
    print(f"🌍 Timezone: {TZ}")
    
    return output_path


if __name__ == "__main__":
    automation_calendar_flow()