"""
API Flask que simula leituras IoT para inversor, relé de proteção e estação solarimétrica.
Cada requisição sorteia um dos 5 SNs do tipo (não é possível escolher o dispositivo via API).
~80% respostas coerentes; ~20% cenários de erro (formato inválido, zeros, SN inválido, chaves faltando).
"""
from __future__ import annotations

import random
import secrets
from datetime import datetime
from typing import Any, Callable, Dict, List, Tuple

from flask import Flask, Response, jsonify

app = Flask(__name__)

# Cinco serial numbers por tipo de dispositivo
SN_INVERSOR = [f"A235180121{7 + i}" for i in range(5)]
SN_RELE = [f"releprote_{18746926 + i}" for i in range(5)]
SN_ESTACAO = [f"estacao_1874641{i}" for i in range(0, 5)]

def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _random_sn(sns: List[str]) -> str:
    """Escolhe um SN entre os cinco cadastrados; não há como o cliente escolher qual."""
    return secrets.choice(sns)

def _should_error() -> bool:
    return random.random() < 0.20

def _gen_inversor_coerente(sn: str) -> Dict[str, Any]:
    fac = round(random.uniform(59.8, 60.2), 2)
    uac = round(random.uniform(218.0, 242.0), 2)
    uac1 = round(uac + random.uniform(-2, 2), 2)
    uac2 = round(uac + random.uniform(-2, 2), 2)
    uac3 = round(uac + random.uniform(-2, 2), 2)
    pac = round(random.uniform(5.0, 480.0), 2)
    iac_each = round(pac * 1000 / (3 * uac * 0.98) / 1000, 3) if uac > 0 else 0.0
    iac = round(iac_each * 3, 3)
    eday = round(random.uniform(100.0, 2500.0), 1)
    etotal = round(random.uniform(1_000_000.0, 2_000_000.0), 2)
    upv = [round(random.uniform(350.0, 720.0), 2) for _ in range(3)]
    ipv = [round(pac * random.uniform(0.08, 0.15) / max(u, 1.0), 3) for u in upv]

    return {
        "Eday": eday,
        "Etotal": etotal,
        "Iac": iac,
        "Iac1": round(iac_each + random.uniform(-0.01, 0.01), 3),
        "Iac2": round(iac_each + random.uniform(-0.01, 0.01), 3),
        "Iac3": round(iac_each + random.uniform(-0.01, 0.01), 3),
        "Ipv1": ipv[0],
        "Ipv2": ipv[1],
        "Ipv3": ipv[2],
        "Pac": pac,
        "Pac1": int(round(pac / 3)),
        "Pac2": int(round(pac / 3)),
        "Pac3": int(round(pac / 3)),
        "Temp": round(random.uniform(28.0, 48.0), 1),
        "Uac": round((uac1 + uac2 + uac3) / 3, 2),
        "Uac1": uac1,
        "Uac2": uac2,
        "Uac3": uac3,
        "Upv1": upv[0],
        "Upv2": upv[1],
        "Upv3": upv[2],
        "cos": round(random.uniform(0.97, 1.0), 3),
        "fac": fac,
        "tsleitura": _now_ts(),
        "sn": sn,
    }

def _gen_rele_coerente(sn: str) -> Dict[str, Any]:
    base_v = round(random.uniform(220.0, 240.0), 2)
    rkeys = [
        "r25",
        "r27A",
        "r27B",
        "r27C",
        "r32A",
        "r32B",
        "r32C",
        "r37A",
        "r37B",
        "r37C",
        "r47",
        "r50A",
        "r50B",
        "r50C",
        "r50N",
        "r51A",
        "r51B",
        "r51C",
        "r51N",
        "r59A",
        "r59B",
        "r59C",
        "r59N",
        "r67A",
        "r67B",
        "r67C",
        "r67N",
        "r78",
        "r81",
        "r86",
    ]
    out: Dict[str, Any] = {k: random.choice([0, 0, 0, 1]) if k == "r25" else 0 for k in rkeys}
    ia = round(random.uniform(10.0, 450.0), 2)
    out["rFREQ"] = round(random.uniform(59.5, 60.5), 2)
    out["rIfaseA"] = ia
    out["rIfaseB"] = round(ia + random.uniform(-2, 2), 2)
    out["rIfaseC"] = round(ia + random.uniform(-2, 2), 2)
    out["rVfaseA"] = round(base_v + random.uniform(-1.5, 1.5), 2)
    out["rVfaseB"] = round(base_v + random.uniform(-1.5, 1.5), 2)
    out["rVfaseC"] = round(base_v + random.uniform(-1.5, 1.5), 2)
    rpac = round(random.uniform(5.0, 300.0), 2)
    out["rpac1"] = int(round(rpac / 3))
    out["rpac2"] = int(round(rpac / 3))
    out["rpac3"] = int(round(rpac / 3))
    out["rpac"] = rpac
    out["rtempinterno"] = round(random.uniform(30.0, 42.0), 2)
    out["sn"] = sn
    out["tpLei"] = "rele"
    out["tsleitura"] = _now_ts()
    return out

def _gen_estacao_coerente(sn: str) -> Dict[str, Any]:
    ir_ghi = round(random.uniform(0.0, 1050.0), 6)
    ir_poa = round(ir_ghi * random.uniform(1.0, 1.25), 6)
    return {
        "IrDay": round(random.uniform(2.0, 8.5), 5),
        "IrTotal": round(random.uniform(1200.0, 2200.0), 5),
        "IrGHI": f"{ir_ghi:.6f}",
        "IrPOA": f"{ir_poa:.6f}",
        "Umid": str(round(random.uniform(45.0, 95.0), 1)),
        "chuTotal": str(round(random.uniform(0.0, 2500.0), 1)),
        "dirVento": str(round(random.uniform(0.0, 360.0), 1)),
        "tempAmb": str(round(random.uniform(15.0, 38.0), 1)),
        "sn": sn,
        "tempMedMod": str(round(random.uniform(16.0, 55.0), 1)),
        "tpLei": "meteo",
        "tsleitura": _now_ts(),
        "velVento": str(round(random.uniform(0.5, 15.0), 1)),
    }

def _error_invalid_format() -> Response:
    return Response("OK|campo1|campo2|sem_json", mimetype="text/plain", status=200)

def _error_invalid_sn() -> Tuple[Dict[str, Any], int]:
    return {"erro": "serial_number_desconhecido", "detalhe": "Equipamento não encontrado"}, 404

def _zeroed_inversor(sn: str) -> Dict[str, Any]:
    z = _gen_inversor_coerente(sn)
    for k, v in list(z.items()):
        if k in ("tsleitura", "sn"):
            continue
        if isinstance(v, (int, float)):
            z[k] = 0.0 if isinstance(v, float) else 0
        elif k.startswith("Pac") and k != "Pac":
            z[k] = 0
    z["Pac"] = 0.0
    return z

def _zeroed_rele(sn: str) -> Dict[str, Any]:
    z = _gen_rele_coerente(sn)
    for k in z:
        if k in ("tsleitura", "sn", "tpLei"):
            continue
        z[k] = 0 if isinstance(z[k], int) else 0.0
    return z

def _zeroed_estacao(sn: str) -> Dict[str, Any]:
    z = _gen_estacao_coerente(sn)
    for k in z:
        if k in ("tsleitura", "sn", "tpLei"):
            continue
        z[k] = "0" if isinstance(z[k], str) and k not in ("IrGHI", "IrPOA") else "0.0"
    z["IrGHI"] = "0.000000"
    z["IrPOA"] = "0.000000"
    return z

def _drop_essential_inversor(sn: str) -> Dict[str, Any]:
    d = _gen_inversor_coerente(sn)
    which = random.choice(["sn", "tsleitura", "Pac"])
    del d[which]
    return d

def _drop_essential_rele(sn: str) -> Dict[str, Any]:
    d = _gen_rele_coerente(sn)
    which = random.choice(["sn", "tsleitura", "tpLei"])
    del d[which]
    return d

def _drop_essential_estacao(sn: str) -> Dict[str, Any]:
    d = _gen_estacao_coerente(sn)
    which = random.choice(["sn", "tsleitura", "tpLei"])
    del d[which]
    return d

def _bad_types_inversor(sn: str) -> Dict[str, Any]:
    d = _gen_inversor_coerente(sn)
    d["Pac"] = "invalido_kw"
    d["Uac1"] = None
    d["fac"] = "sessenta_hz"
    return d

def _bad_types_rele(sn: str) -> Dict[str, Any]:
    d = _gen_rele_coerente(sn)
    d["rVfaseA"] = "8155,31"
    d["rFREQ"] = []
    return d

def _bad_types_estacao(sn: str) -> Dict[str, Any]:
    d = _gen_estacao_coerente(sn)
    d["velVento"] = {"v": 3.9}
    d["IrGHI"] = True
    return d

ERROR_VARIANTS_INVERSOR: List[Callable[..., Any]] = [
    lambda _: _error_invalid_format(),
    lambda _: _error_invalid_sn(),
    _zeroed_inversor,
    _drop_essential_inversor,
    _bad_types_inversor,
]

ERROR_VARIANTS_RELE: List[Callable[..., Any]] = [
    lambda _: _error_invalid_format(),
    lambda _: _error_invalid_sn(),
    _zeroed_rele,
    _drop_essential_rele,
    _bad_types_rele,
]

ERROR_VARIANTS_ESTACAO: List[Callable[..., Any]] = [
    lambda _: _error_invalid_format(),
    lambda _: _error_invalid_sn(),
    _zeroed_estacao,
    _drop_essential_estacao,
    _bad_types_estacao,
]

def _apply_error_branch(
    sn: str,
    error_variants: List[Callable[..., Any]],
) -> Any:
    if not _should_error():
        return None
    fn = random.choice(error_variants)
    result = fn(sn)
    if isinstance(result, Response):
        return result
    if isinstance(result, tuple) and len(result) == 2:
        return jsonify(result[0]), result[1]
    return jsonify(result)

@app.route("/inversor", methods=["GET"])
def rota_inversor():
    sn = _random_sn(SN_INVERSOR)
    err = _apply_error_branch(sn, ERROR_VARIANTS_INVERSOR)
    if err is not None:
        return err
    return jsonify(_gen_inversor_coerente(sn))

@app.route("/rele-protecao", methods=["GET"])
def rota_rele():
    sn = _random_sn(SN_RELE)
    err = _apply_error_branch(sn, ERROR_VARIANTS_RELE)
    if err is not None:
        return err
    return jsonify(_gen_rele_coerente(sn))

@app.route("/estacao-solarimetrica", methods=["GET"])
def rota_estacao():
    sn = _random_sn(SN_ESTACAO)
    err = _apply_error_branch(sn, ERROR_VARIANTS_ESTACAO)
    if err is not None:
        return err
    return jsonify(_gen_estacao_coerente(sn))

@app.route("/health", methods=["GET"])
def health():
    return jsonify(
        {
            "status": "ok",
            "rotas": ["/inversor", "/rele-protecao", "/estacao-solarimetrica"],
            "dispositivos_por_rota": 5,
        }
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=False)
