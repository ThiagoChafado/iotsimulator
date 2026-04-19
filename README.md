# Solar Plant IoT Telemetry Collector

Solução para o desafio técnico de coleta, validação e persistência de dados
de telemetria de uma usina solar fotovoltaica.

---

## Estrutura do projeto

```
collector.py          ← Script principal (único entregável de código)
solar_telemetry.db    ← Banco de dados SQLite gerado após execução completa
README.md             ← Este arquivo
```

---

## Como executar

### 1. Pré-requisitos

```bash
pip install requests flask
```

### 2. Inicie o simulador da API (em um terminal separado)

```bash
python simulator.py
# API disponível em http://localhost:5050
```

### 3. Execute o coletor

```bash
python collector.py
```

O script roda por **45 minutos** (9 ciclos de 5 minutos cada) e gera:
- `solar_telemetry.db` — banco populado com leituras válidas
- `collector.log` — log completo de execução

---

## Modelagem do banco de dados

Quatro tabelas, cada uma otimizada para o seu tipo de dado:

| Tabela | Conteúdo | Chave única |
|---|---|---|
| `inversor` | Telemetria dos inversores fotovoltaicos | `(sn, tsleitura)` |
| `rele_protecao` | Medições e status dos relés de proteção | `(sn, tsleitura)` |
| `estacao_solarimetrica` | Dados meteorológicos e de irradiância | `(sn, tsleitura)` |
| `rejected_readings` | Leituras rejeitadas com motivo e snippet do payload | `id` |

**Princípio:** cada leitura é identificada pelo SN do dispositivo (`sn`) e pelo
timestamp gerado pelo próprio equipamento (`tsleitura`). A constraint
`UNIQUE(sn, tsleitura)` garante idempotência — re-execuções não duplicam dados.

Índices criados em `sn` nas três tabelas de leitura para agilizar consultas
por dispositivo.

---

## Estratégia de validação

A API injeta **~20% de anomalias**. O coletor detecta e rejeita todas elas:

| Cenário de erro da API | Como é detectado |
|---|---|
| Resposta em texto puro (`OK\|campo\|...`) | `Content-Type` não é `application/json` |
| HTTP 404 (SN desconhecido) | Código de status HTTP ≠ 200 |
| Leitura zerada (todos os campos = 0) | Verificação dos campos-chave (`Pac`, `Uac`, `Iac`, etc.) |
| Chave essencial ausente (`sn`, `tsleitura`, `Pac`, `tpLei`) | Verificação do conjunto de chaves obrigatórias |
| Tipo inválido (`"invalido_kw"`, `None`, `[]`, `{"v": 3.9}`, `True`) | Verificação estrita de tipo numérico (exclui `bool`) |

Rejeições são salvas em `rejected_readings` com endpoint, motivo e snippet
do payload para auditoria.

---

## Consultas de exemplo

```sql
-- Todas as leituras de um inversor específico
SELECT tsleitura, Pac, Uac, fac, Temp
FROM inversor
WHERE sn = 'A2351801217'
ORDER BY tsleitura;

-- Média de potência por dispositivo
SELECT sn, ROUND(AVG(Pac), 2) AS pac_medio_w
FROM inversor
GROUP BY sn;

-- Rejeições por tipo de motivo
SELECT endpoint, reason, COUNT(*) AS qtd
FROM rejected_readings
GROUP BY endpoint, reason
ORDER BY qtd DESC;

-- Temperatura ambiente ao longo do tempo
SELECT sn, tsleitura, tempAmb, velVento, Umid
FROM estacao_solarimetrica
ORDER BY tsleitura;
```
# iotsimulator
