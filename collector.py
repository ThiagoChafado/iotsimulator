#!/usr/bin/env python3
"""
solar_collector.py
==================
coleta telemetria IoT de três endpoints de uma usina solar a cada 5 minutos,
valida cada resposta e persiste somente leituras íntegras em SQLite
duração de execução: 45 minutos (9 ciclos).
banco gerado: solar_telemetry.db

Requisitos:
    pip install requests

Uso:
    inicie o simulator.py em outro terminal primeiro:
    python simulator.py
    depois execute o coletor:
    python collector.py
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

# config

BASE_URL        = "http://localhost:5050"
POLL_INTERVAL   = 300       # segundos entre ciclos (5 min)
RUN_DURATION    = 45 * 60   # duração total em segundos (45 min)
DB_PATH         = "solar_telemetry.db"
REQUEST_TIMEOUT = 10        # timeout HTTP por requisição

ENDPOINTS: Dict[str, str] = {
    "inversor": f"{BASE_URL}/inversor",
    "rele":     f"{BASE_URL}/rele-protecao",
    "estacao":  f"{BASE_URL}/estacao-solarimetrica",
}

# logging (stdout + arquivo)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("collector.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# schema sql

SCHEMA = """
PRAGMA journal_mode = WAL;

-- ── Inversor ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inversor (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    sn           TEXT    NOT NULL,          -- serial number do dispositivo
    tsleitura    TEXT    NOT NULL,          -- timestamp gerado pelo equipamento
    collected_at TEXT    NOT NULL,          -- timestamp de coleta pelo coletor
    -- Energia gerada (Wh)
    Eday         REAL,                      -- geração do dia
    Etotal       REAL,                      -- geração acumulada
    -- Corrente AC (A)
    Iac          REAL,
    Iac1         REAL,
    Iac2         REAL,
    Iac3         REAL,
    -- Corrente DC por string (A)
    Ipv1         REAL,
    Ipv2         REAL,
    Ipv3         REAL,
    -- Potência AC (W)
    Pac          REAL,
    Pac1         INTEGER,
    Pac2         INTEGER,
    Pac3         INTEGER,
    -- Temperatura interna (°C)
    Temp         REAL,
    -- Tensão AC (V)
    Uac          REAL,
    Uac1         REAL,
    Uac2         REAL,
    Uac3         REAL,
    -- Tensão DC por string (V)
    Upv1         REAL,
    Upv2         REAL,
    Upv3         REAL,
    -- Fator de potência e frequência
    cos          REAL,
    fac          REAL,
    UNIQUE (sn, tsleitura)
);

-- ── Relé de Proteção ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rele_protecao (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    sn           TEXT    NOT NULL,
    tsleitura    TEXT    NOT NULL,
    tpLei        TEXT,
    collected_at TEXT    NOT NULL,
    -- Medições elétricas
    rFREQ        REAL,                      -- frequência (Hz)
    rIfaseA      REAL,                      -- corrente fase A (A)
    rIfaseB      REAL,
    rIfaseC      REAL,
    rVfaseA      REAL,                      -- tensão fase A (V)
    rVfaseB      REAL,
    rVfaseC      REAL,
    rpac         REAL,                      -- potência ativa total (W)
    rpac1        INTEGER,
    rpac2        INTEGER,
    rpac3        INTEGER,
    rtempinterno REAL,                      -- temperatura interna (°C)
    -- Funções de proteção (0 = inativo, 1 = atuado)
    r25  INTEGER, r27A INTEGER, r27B INTEGER, r27C INTEGER,
    r32A INTEGER, r32B INTEGER, r32C INTEGER,
    r37A INTEGER, r37B INTEGER, r37C INTEGER,
    r47  INTEGER,
    r50A INTEGER, r50B INTEGER,  r50C INTEGER,  r50N INTEGER,
    r51A INTEGER, r51B INTEGER,  r51C INTEGER,  r51N INTEGER,
    r59A INTEGER, r59B INTEGER,  r59C INTEGER,  r59N INTEGER,
    r67A INTEGER, r67B INTEGER,  r67C INTEGER,  r67N INTEGER,
    r78  INTEGER, r81  INTEGER,  r86  INTEGER,
    UNIQUE (sn, tsleitura)
);

-- ── Estação Solarimétrica ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS estacao_solarimetrica (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    sn           TEXT    NOT NULL,
    tsleitura    TEXT    NOT NULL,
    tpLei        TEXT,
    collected_at TEXT    NOT NULL,
    -- Irradiância solar (W/m²)
    IrDay        REAL,                      -- irradiância do dia (kWh/m²)
    IrTotal      REAL,                      -- irradiância acumulada (kWh/m²)
    IrGHI        REAL,                      -- irradiância horizontal global
    IrPOA        REAL,                      -- irradiância no plano do painel
    -- Meteorologia
    Umid         REAL,                      -- umidade relativa (%)
    chuTotal     REAL,                      -- chuva acumulada (mm)
    dirVento     REAL,                      -- direção do vento (°)
    tempAmb      REAL,                      -- temperatura ambiente (°C)
    tempMedMod   REAL,                      -- temperatura do módulo (°C)
    velVento     REAL,                      -- velocidade do vento (m/s)
    UNIQUE (sn, tsleitura)
);

-- ── Log de leituras rejeitadas ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rejected_readings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint     TEXT    NOT NULL,          -- "inversor" | "rele" | "estacao"
    collected_at TEXT    NOT NULL,
    reason       TEXT    NOT NULL,          -- motivo da rejeição
    raw_snippet  TEXT                       -- primeiros 500 chars do payload
);

-- Índices para consultas por dispositivo
CREATE INDEX IF NOT EXISTS idx_inversor_sn  ON inversor (sn);
CREATE INDEX IF NOT EXISTS idx_rele_sn      ON rele_protecao (sn);
CREATE INDEX IF NOT EXISTS idx_estacao_sn   ON estacao_solarimetrica (sn);
"""


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.executescript(SCHEMA)
    conn.commit()
    log.info("Banco de dados inicializado: %s", path)
    return conn


# utilitários de tipo

def _is_numeric(val: Any) -> bool:
    """
    Retorna True apenas para int/float puros.
    Exclui bool propositalmente — em Python, bool é subclasse de int,
    mas True/False não são leituras numéricas válidas.
    """
    return isinstance(val, (int, float)) and not isinstance(val, bool)


def _parse_float(val: Any) -> Optional[float]:
    """
    Converte val para float, aceitando strings numéricas (inclusive com vírgula).
    Retorna None se a conversão falhar ou val for bool/dict/list.
    """
    if isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val.replace(",", "."))
        except ValueError:
            return None
    return None


# Validação — Inversor

_INV_REQUIRED: set = {"sn", "tsleitura", "Pac", "Uac", "Iac", "fac", "Eday", "Etotal", "Temp"}
_INV_NUMERIC: List[str] = [
    "Pac", "Pac1", "Pac2", "Pac3",
    "Uac", "Uac1", "Uac2", "Uac3",
    "Iac", "Iac1", "Iac2", "Iac3",
    "Ipv1", "Ipv2", "Ipv3",
    "Upv1", "Upv2", "Upv3",
    "Eday", "Etotal", "Temp", "cos", "fac",
]


def validate_inversor(data: Dict[str, Any]) -> Tuple[bool, str]:
    # chaves obrigatórias
    missing = _INV_REQUIRED - set(data.keys())
    if missing:
        return False, f"Chaves ausentes: {sorted(missing)}"

    # SN deve seguir o padrão "A23518012X"
    sn = data.get("sn", "")
    if not isinstance(sn, str) or not sn.startswith("A23518012"):
        return False, f"SN inválido: {sn!r}"

    # campos numéricos: se o campo ESTÁ PRESENTE, o valor não pode ser
    #    str, bool, list, dict ou None — captura _bad_types_inversor (Uac1=None, fac="...")
    for field in _INV_NUMERIC:
        if field not in data:
            continue          # campo ausente é OK; falta de required já foi verificada
        if not _is_numeric(data[field]):
            return False, f"Tipo inválido em '{field}': {data[field]!r} ({type(data[field]).__name__})"

    # leitura zerada — todos os principais indicadores em 0
    if data["Pac"] == 0 and data.get("Uac", 0) == 0 and data.get("Iac", 0) == 0:
        return False, "Leitura zerada (Pac, Uac e Iac = 0)"

    # sanidade de frequência (rede brasileira ~60 Hz)
    fac = data["fac"]
    if not _is_numeric(fac) or not (58.0 <= fac <= 62.0):
        return False, f"Frequência fora do intervalo [58, 62] Hz: {fac!r}"

    # sanidade de tensão AC
    uac = data["Uac"]
    if not _is_numeric(uac) or not (100.0 <= uac <= 300.0):
        return False, f"Tensão AC fora do intervalo [100, 300] V: {uac!r}"

    return True, "OK"


def insert_inversor(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO inversor
            (sn, tsleitura, collected_at,
             Eday, Etotal,
             Iac,  Iac1,  Iac2,  Iac3,
             Ipv1, Ipv2, Ipv3,
             Pac,  Pac1,  Pac2,  Pac3,
             Temp,
             Uac,  Uac1,  Uac2,  Uac3,
             Upv1, Upv2, Upv3,
             cos, fac)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            data["sn"], data["tsleitura"], _now(),
            data.get("Eday"), data.get("Etotal"),
            data.get("Iac"),  data.get("Iac1"),  data.get("Iac2"),  data.get("Iac3"),
            data.get("Ipv1"), data.get("Ipv2"), data.get("Ipv3"),
            data.get("Pac"),  data.get("Pac1"),  data.get("Pac2"),  data.get("Pac3"),
            data.get("Temp"),
            data.get("Uac"),  data.get("Uac1"),  data.get("Uac2"),  data.get("Uac3"),
            data.get("Upv1"), data.get("Upv2"), data.get("Upv3"),
            data.get("cos"), data.get("fac"),
        ),
    )
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# validação — rele
# ─────────────────────────────────────────────────────────────────────────────
_RELE_REQUIRED: set = {"sn", "tsleitura", "tpLei"}
_RELE_NUMERIC: List[str] = [
    "rFREQ",
    "rIfaseA", "rIfaseB", "rIfaseC",
    "rVfaseA", "rVfaseB", "rVfaseC",
    "rpac", "rpac1", "rpac2", "rpac3",
    "rtempinterno",
]
_RELE_FLAGS: List[str] = [
    "r25",
    "r27A", "r27B", "r27C",
    "r32A", "r32B", "r32C",
    "r37A", "r37B", "r37C",
    "r47",
    "r50A", "r50B",  "r50C",  "r50N",
    "r51A", "r51B",  "r51C",  "r51N",
    "r59A", "r59B",  "r59C",  "r59N",
    "r67A", "r67B",  "r67C",  "r67N",
    "r78",  "r81",   "r86",
]


def validate_rele(data: Dict[str, Any]) -> Tuple[bool, str]:
    # chaves obrigatórias
    missing = _RELE_REQUIRED - set(data.keys())
    if missing:
        return False, f"Chaves ausentes: {sorted(missing)}"

    # SN
    sn = data.get("sn", "")
    if not isinstance(sn, str) or not sn.startswith("releprote_"):
        return False, f"SN inválido: {sn!r}"

    # tpLei
    if data.get("tpLei") != "rele":
        return False, f"tpLei inesperado: {data.get('tpLei')!r}"

    # tipos numéricos (se o campo está presente, valor deve ser int/float puro)
    for field in _RELE_NUMERIC:
        if field not in data:
            continue
        if not _is_numeric(data[field]):
            return False, f"Tipo inválido em '{field}': {data[field]!r} ({type(data[field]).__name__})"

    # leitura zerada
    if (data.get("rpac", 0) == 0
            and data.get("rFREQ", 0) == 0
            and data.get("rIfaseA", 0) == 0):
        return False, "Leitura zerada (rpac, rFREQ e rIfaseA = 0)"

    # sanidade de frequência
    freq = data.get("rFREQ")
    if freq is not None and _is_numeric(freq) and not (58.0 <= freq <= 62.0):
        return False, f"Frequência fora do intervalo [58, 62] Hz: {freq!r}"

    return True, "OK"


def insert_rele(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    flag_vals = tuple(data.get(f, 0) for f in _RELE_FLAGS)
    placeholders = ",".join(["?"] * len(_RELE_FLAGS))
    flag_cols = ", ".join(_RELE_FLAGS)

    conn.execute(
        f"""
        INSERT OR IGNORE INTO rele_protecao
            (sn, tsleitura, tpLei, collected_at,
             rFREQ,
             rIfaseA, rIfaseB, rIfaseC,
             rVfaseA, rVfaseB, rVfaseC,
             rpac, rpac1, rpac2, rpac3,
             rtempinterno,
             {flag_cols})
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,{placeholders})
        """,
        (
            data["sn"], data["tsleitura"], data.get("tpLei"), _now(),
            data.get("rFREQ"),
            data.get("rIfaseA"), data.get("rIfaseB"), data.get("rIfaseC"),
            data.get("rVfaseA"), data.get("rVfaseB"), data.get("rVfaseC"),
            data.get("rpac"), data.get("rpac1"), data.get("rpac2"), data.get("rpac3"),
            data.get("rtempinterno"),
            *flag_vals,
        ),
    )
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# validação — estacao
# ─────────────────────────────────────────────────────────────────────────────
_EST_REQUIRED: set = {"sn", "tsleitura", "tpLei"}

# IrDay/IrTotal chegam como float nativo; os demais chegam como strings
_EST_NATIVE_FLOAT: List[str] = ["IrDay", "IrTotal"]
_EST_STR_FLOAT: List[str] = [
    "IrGHI", "IrPOA",
    "Umid", "chuTotal", "dirVento",
    "tempAmb", "tempMedMod", "velVento",
]


def validate_estacao(data: Dict[str, Any]) -> Tuple[bool, str]:
    # chaves obrigatorias
    missing = _EST_REQUIRED - set(data.keys())
    if missing:
        return False, f"Chaves ausentes: {sorted(missing)}"

    # SN
    sn = data.get("sn", "")
    if not isinstance(sn, str) or not sn.startswith("estacao_"):
        return False, f"SN inválido: {sn!r}"

    # tpLei
    if data.get("tpLei") != "meteo":
        return False, f"tpLei inesperado: {data.get('tpLei')!r}"

    # campos que chegam como float nativo (se presentes, não podem ser str/bool/list)
    for field in _EST_NATIVE_FLOAT:
        if field not in data:
            continue
        if not _is_numeric(data[field]):
            return False, f"Tipo inválido em '{field}': {data[field]!r} ({type(data[field]).__name__})"

    #  campos que chegam como string mas devem ser conversíveis para float
    #    captura dict {"v": 3.9}, bool True, strings não numéricas, etc
    for field in _EST_STR_FLOAT:
        val = data.get(field)
        if val is None:
            continue
        if _parse_float(val) is None:
            return False, (
                f"Valor não conversível para float em '{field}': "
                f"{val!r} ({type(val).__name__})"
            )

    # leitura zerada — IrGHI, IrPOA, Umid e velVento todos em zero
    ghi   = _parse_float(data.get("IrGHI",   "0"))
    poa   = _parse_float(data.get("IrPOA",   "0"))
    umid  = _parse_float(data.get("Umid",    "0"))
    vento = _parse_float(data.get("velVento","0"))
    if ghi == 0 and poa == 0 and umid == 0 and vento == 0:
        return False, "Leitura zerada (IrGHI, IrPOA, Umid e velVento = 0)"

    return True, "OK"


def insert_estacao(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO estacao_solarimetrica
            (sn, tsleitura, tpLei, collected_at,
             IrDay, IrTotal, IrGHI, IrPOA,
             Umid, chuTotal, dirVento, tempAmb, tempMedMod, velVento)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            data["sn"], data["tsleitura"], data.get("tpLei"), _now(),
            data.get("IrDay"), data.get("IrTotal"),
            _parse_float(data.get("IrGHI")),    _parse_float(data.get("IrPOA")),
            _parse_float(data.get("Umid")),      _parse_float(data.get("chuTotal")),
            _parse_float(data.get("dirVento")),  _parse_float(data.get("tempAmb")),
            _parse_float(data.get("tempMedMod")),_parse_float(data.get("velVento")),
        ),
    )
    conn.commit()



# persistência de rejeições


def log_rejection(
    conn: sqlite3.Connection,
    endpoint: str,
    reason: str,
    raw: Any,
) -> None:
    snippet = str(raw)[:500] if raw is not None else None
    conn.execute(
        """
        INSERT INTO rejected_readings (endpoint, collected_at, reason, raw_snippet)
        VALUES (?, ?, ?, ?)
        """,
        (endpoint, _now(), reason, snippet),
    )
    conn.commit()


# ciclo de coleta

# yabela de tarefas: (nome, url, validador, insersor)
Task = Tuple[str, str, Callable, Callable]
TASKS: List[Task] = [
    ("inversor", ENDPOINTS["inversor"], validate_inversor, insert_inversor),
    ("rele",     ENDPOINTS["rele"],     validate_rele,     insert_rele),
    ("estacao",  ENDPOINTS["estacao"],  validate_estacao,  insert_estacao),
]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def collect_once(conn: sqlite3.Connection, stats: Dict[str, int]) -> None:
    """Faz uma requisição para cada endpoint, valida e persiste dados válidos."""

    for name, url, validator, inserter in TASKS:
        stats["total"] += 1
        raw_body: Any = None

        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)

            # verificar codigo HTTP 
            if resp.status_code != 200:
                reason = f"HTTP {resp.status_code} — {resp.text[:120]}"
                log.warning("[%s] ✗ Rejeitado — %s", name, reason)
                log_rejection(conn, name, reason, resp.text[:500])
                stats["rejected"] += 1
                continue

            # verificar Content-Type (captura resposta texto "OK|campo|...") 
            ct = resp.headers.get("Content-Type", "")
            if "application/json" not in ct:
                reason = f"Content-Type não-JSON: {ct!r} | body: {resp.text[:80]}"
                log.warning("[%s] ✗ Rejeitado — %s", name, reason)
                log_rejection(conn, name, reason, resp.text[:500])
                stats["rejected"] += 1
                continue

            # parser JSON 
            data: Dict[str, Any] = resp.json()
            raw_body = data

            # validar estrutura e integridade 
            ok, msg = validator(data)
            if not ok:
                sn_hint = data.get("sn", "?")
                log.warning("[%s] ✗ Rejeitado — %s | sn=%s", name, msg, sn_hint)
                log_rejection(conn, name, msg, data)
                stats["rejected"] += 1
                continue

            inserter(conn, data)
            log.info(
                "[%s] ✔ Salvo   — sn=%-24s ts=%s",
                name, data["sn"], data["tsleitura"],
            )
            stats["saved"] += 1

        except requests.exceptions.Timeout:
            reason = "Timeout na requisição HTTP"
            log.error("[%s] ✗ %s", name, reason)
            log_rejection(conn, name, reason, None)
            stats["rejected"] += 1

        except requests.exceptions.ConnectionError:
            reason = "Sem conexão com a API — verifique se o simulator.py está rodando"
            log.error("[%s] ✗ %s", name, reason)
            log_rejection(conn, name, reason, None)
            stats["rejected"] += 1

        except requests.exceptions.RequestException as exc:
            reason = f"Erro de rede: {exc}"
            log.error("[%s] ✗ %s", name, reason)
            log_rejection(conn, name, reason, None)
            stats["rejected"] += 1

        except Exception as exc:  # noqa: BLE001
            reason = f"Erro inesperado: {type(exc).__name__}: {exc}"
            log.error("[%s] ✗ %s | raw=%s", name, reason, str(raw_body)[:200])
            log_rejection(conn, name, reason, raw_body)
            stats["rejected"] += 1


# loop principal

def main() -> None:
    log.info("=" * 65)
    log.info("  Coletor Solar IoT — início: %s", _now())
    log.info("  Duração: %d min | Intervalo: %d s | DB: %s",
             RUN_DURATION // 60, POLL_INTERVAL, DB_PATH)
    log.info("=" * 65)

    conn  = init_db(DB_PATH)
    stats = {"total": 0, "saved": 0, "rejected": 0}

    start   = time.monotonic()
    deadline = start + RUN_DURATION
    cycle   = 0
    next_run = start  # primeiro ciclo imediatamente

    while True:
        now = time.monotonic()

        # aguardar até o próximo ciclo (ou até o deadline)
        wait = next_run - now
        if wait > 0:
            # não ultrapassar o deadline durante o sleep
            actual_wait = min(wait, deadline - now)
            if actual_wait > 0:
                log.info("Próximo ciclo em %.0f s …", actual_wait)
                time.sleep(actual_wait)

        if time.monotonic() >= deadline:
            break

        cycle += 1
        elapsed_min = (time.monotonic() - start) / 60
        log.info("─" * 65)
        log.info("  Ciclo %d — %.1f min decorridos", cycle, elapsed_min)
        log.info("─" * 65)

        collect_once(conn, stats)

        next_run = time.monotonic() + POLL_INTERVAL

    conn.close()

    # sumario
    total   = stats["total"]
    saved   = stats["saved"]
    rejected = stats["rejected"]
    pct_ok  = 100 * saved   / total if total else 0
    pct_err = 100 * rejected / total if total else 0

    log.info("=" * 65)
    log.info("  Coleta encerrada — %.1f min executados", (time.monotonic() - start) / 60)
    log.info("  Ciclos realizados : %d", cycle)
    log.info("  Requisições totais: %d", total)
    log.info("  ✔  Salvas         : %d  (%.0f%%)", saved,    pct_ok)
    log.info("  ✗  Rejeitadas     : %d  (%.0f%%)", rejected, pct_err)
    log.info("  Banco de dados    : %s", DB_PATH)
    log.info("=" * 65)


if __name__ == "__main__":
    main()
