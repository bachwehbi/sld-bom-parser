# Databricks notebook source
# MAGIC %md
# MAGIC # SLD-to-BOM — Material Catalog & Stock Data Generation
# MAGIC
# MAGIC Generates three Delta tables with realistic Schneider Electric product data
# MAGIC for the residential and commercial building market (Spain / Europe):
# MAGIC
# MAGIC | Table | Contents |
# MAGIC |-------|----------|
# MAGIC | `material` | ~550 product references (active + discontinued) |
# MAGIC | `stock` | Per-reference availability across 5 Spanish distribution centers |
# MAGIC | `work_orders` | Incoming stock orders with expected delivery dates |
# MAGIC
# MAGIC ## Prerequisites (Tier 2+)
# MAGIC
# MAGIC Run `setup.py` first — the schema and volume must exist before this notebook can write tables.
# MAGIC
# MAGIC > **Bringing your own catalog data?** Skip this notebook and load your product references
# MAGIC > directly into the `material`, `stock`, and `work_orders` tables using the same schema.
# MAGIC > The matching pipeline (`sld_bom_matching_nb.py`) is schema-driven and works with any product data.
# MAGIC
# MAGIC **Run this notebook once** after `setup.py` to populate the catalog.
# MAGIC Re-running is safe — tables are replaced with fresh data (`overwrite` mode).

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# DBTITLE 1,Imports
import json
import random
import uuid
from datetime import date, timedelta
from itertools import product as iterproduct

random.seed(42)   # reproducible

DISTRIBUTION_CENTERS = ["MADRID", "BARCELONA", "BILBAO", "SEVILLA", "VALENCIA"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Material catalog generation
# MAGIC
# MAGIC References are generated systematically by iterating over the technical axes that
# MAGIC define each product type, mirroring Schneider Electric's actual reference structure.
# MAGIC
# MAGIC ### Component types covered
# MAGIC | Component | Ranges | Tier |
# MAGIC |-----------|--------|------|
# MAGIC | Interruptor Automático | Resi9, Acti9 iC60N, Acti9 iC60H | economy / standard / premium |
# MAGIC | Interruptor Diferencial | Acti9 iID (AC, A, A-SI), Vigi iC60 block | standard / premium |
# MAGIC | Contactor | TeSys D LC1D | standard |
# MAGIC | Reloj | Acti9 IH, IHP+, IC Astro | economy / standard / premium |
# MAGIC | Limitador de Sobretensión | Acti9 iPRD | standard / premium |
# MAGIC | Portafusibles | Acti9 iSF | standard |
# MAGIC | Contador de Energía | iEM3110, iEM3255, iEM3355 | economy / standard / premium |
# MAGIC | Inversor/Transferpact | TransferPacT Active | premium |
# MAGIC | Interruptor de Corte en Carga | Acti9 iSW | standard |

# COMMAND ----------

# DBTITLE 1,Interruptor Automático — circuit breakers
def gen_circuit_breakers():
    rows = []

    # ── Resi9 (economy, 4.5 kA, residential) ──────────────────────────────────
    for poles, calibre, curve in iterproduct(
        [1, 2, 3, 4],
        [6, 10, 16, 20, 25, 32, 40, 63],
        ["B", "C"],
    ):
        p_code = {1: "1", 2: "2", 3: "3", 4: "4"}[poles]
        c_code = {"B": "1", "C": "2"}[curve]
        ref = f"R9F{c_code}{p_code}2{calibre:02d}"
        props = {"poles": poles, "calibre_A": calibre, "curve": curve, "breaking_kA": 4.5}
        desc  = f"Resi9 {poles}P {calibre}A {curve}"
        rows.append({
            "reference": ref,
            "product_description": desc,
            "product_long_description": f"Interruptor automático Resi9 {poles}P {calibre}A curva {curve} 4,5kA para instalaciones residenciales",
            "strategic_product_family_description": "Resi9",
            "range": "Resi9",
            "component_type": "interruptor automatico",
            "tier": "economy",
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": round(8.0 + calibre * 0.4 + poles * 1.5, 2),
            "properties": json.dumps(props),
        })

    # ── Acti9 iC60N (standard, 6 kA) ──────────────────────────────────────────
    for poles, calibre, curve in iterproduct(
        [1, 2, 3, 4],
        [6, 10, 16, 20, 25, 32, 40, 50, 63],
        ["B", "C", "D"],
    ):
        p_code = {1: "1", 2: "2", 3: "3", 4: "4"}[poles]
        c_code = {"B": "3", "C": "4", "D": "5"}[curve]
        ref = f"A9F7{c_code}{p_code}{calibre:02d}"
        props = {"poles": poles, "calibre_A": calibre, "curve": curve, "breaking_kA": 6}
        desc  = f"Acti9 iC60N {poles}P {calibre}A {curve}"
        rows.append({
            "reference": ref,
            "product_description": desc,
            "product_long_description": f"Interruptor automático Acti9 iC60N {poles}P {calibre}A curva {curve} 6kA, uso profesional en cuadros terciarios y residenciales",
            "strategic_product_family_description": "Acti9",
            "range": "iC60N",
            "component_type": "interruptor automatico",
            "tier": "standard",
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": round(18.0 + calibre * 0.55 + poles * 2.5 + (2.0 if curve == "D" else 0), 2),
            "properties": json.dumps(props),
        })

    # ── Acti9 iC60H (premium, 10 kA) ──────────────────────────────────────────
    for poles, calibre, curve in iterproduct(
        [1, 2, 3, 4],
        [6, 10, 16, 20, 25, 32, 40, 50, 63],
        ["B", "C", "D"],
    ):
        p_code = {1: "1", 2: "2", 3: "3", 4: "4"}[poles]
        c_code = {"B": "6", "C": "7", "D": "8"}[curve]
        ref = f"A9F{c_code}{p_code}{calibre:02d}"
        props = {"poles": poles, "calibre_A": calibre, "curve": curve, "breaking_kA": 10}
        desc  = f"Acti9 iC60H {poles}P {calibre}A {curve}"
        rows.append({
            "reference": ref,
            "product_description": desc,
            "product_long_description": f"Interruptor automático Acti9 iC60H {poles}P {calibre}A curva {curve} 10kA, alto poder de corte para cabeceras de cuadro",
            "strategic_product_family_description": "Acti9",
            "range": "iC60H",
            "component_type": "interruptor automatico",
            "tier": "premium",
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": round(28.0 + calibre * 0.75 + poles * 3.5 + (3.0 if curve == "D" else 0), 2),
            "properties": json.dumps(props),
        })

    # ── Multi9 C60N (discontinued, superseded by iC60N) ───────────────────────
    for poles, calibre, curve in iterproduct([1, 2, 3], [6, 16, 20, 32, 63], ["C"]):
        p_code = {1: "1", 2: "2", 3: "3"}[poles]
        ref    = f"C60N{p_code}{calibre:02d}{curve}"
        new_ref = f"A9F74{p_code}{calibre:02d}" if curve == "C" else f"A9F73{p_code}{calibre:02d}"
        props  = {"poles": poles, "calibre_A": calibre, "curve": curve, "breaking_kA": 6}
        rows.append({
            "reference": ref,
            "product_description": f"Multi9 C60N {poles}P {calibre}A {curve}",
            "product_long_description": f"Interruptor automático Multi9 C60N {poles}P {calibre}A curva {curve} — DISCONTINUADO, sustituido por Acti9 iC60N",
            "strategic_product_family_description": "Multi9",
            "range": "C60N",
            "component_type": "interruptor automatico",
            "tier": "standard",
            "status": "DISCONTINUED",
            "superseded_by": new_ref,
            "list_price_eur": None,
            "properties": json.dumps(props),
        })

    return rows

ia_rows = gen_circuit_breakers()
print(f"Circuit breakers: {len(ia_rows)} references")

# COMMAND ----------

# DBTITLE 1,Interruptor Diferencial — RCDs
def gen_rcds():
    rows = []

    # ── Acti9 iID — 2P and 4P, all sensitivities and types ───────────────────
    type_map = {
        "AC":   ("standard", 0),
        "A":    ("standard", 8),
        "A-SI": ("premium",  15),
    }
    for poles, calibre, sensitivity, rcd_type in iterproduct(
        [2, 4],
        [25, 40, 63, 100],
        [30, 300],
        ["AC", "A", "A-SI"],
    ):
        tier, price_add = type_map[rcd_type]
        # Sensitivity 300mA not available in A-SI (super-immunized)
        if sensitivity == 300 and rcd_type == "A-SI":
            continue
        p_code = {2: "2", 4: "4"}[poles]
        s_code = {30: "3", 300: "0"}[sensitivity]
        t_code = {"AC": "1", "A": "2", "A-SI": "4"}[rcd_type]
        ref    = f"A9R{t_code}{s_code}{p_code}{calibre:02d}"
        props  = {"poles": poles, "calibre_A": calibre, "sensitivity_mA": sensitivity,
                  "type": rcd_type, "selectivity": "Instantáneo"}
        desc   = f"Acti9 iID {poles}P {calibre}A {sensitivity}mA {rcd_type}"
        rows.append({
            "reference": ref,
            "product_description": desc,
            "product_long_description": f"Interruptor diferencial Acti9 iID {poles}P {calibre}A {sensitivity}mA Tipo {rcd_type} para protección contra contactos indirectos",
            "strategic_product_family_description": "Acti9",
            "range": "iID",
            "component_type": "interruptor diferencial",
            "tier": tier,
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": round(35.0 + calibre * 0.4 + poles * 4.0 + (12 if sensitivity == 30 else 0) + price_add, 2),
            "properties": json.dumps(props),
        })

    # ── Acti9 iID Selectivo — 4P, 300mA, selectivo (S-type) ──────────────────
    for calibre in [63, 100]:
        ref   = f"A9R15{calibre:02d}S"
        props = {"poles": 4, "calibre_A": calibre, "sensitivity_mA": 300,
                 "type": "A", "selectivity": "Selectivo"}
        rows.append({
            "reference": ref,
            "product_description": f"Acti9 iID 4P {calibre}A 300mA A Selectivo",
            "product_long_description": f"Interruptor diferencial selectivo Acti9 iID 4P {calibre}A 300mA Tipo A — para cabeceras con selectividad diferencial",
            "strategic_product_family_description": "Acti9",
            "range": "iID Selectivo",
            "component_type": "interruptor diferencial",
            "tier": "premium",
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": round(95.0 + calibre * 0.5, 2),
            "properties": json.dumps(props),
        })

    return rows

rcd_rows = gen_rcds()
print(f"RCDs: {len(rcd_rows)} references")

# COMMAND ----------

# DBTITLE 1,Contactores — TeSys D LC1D
def gen_contactors():
    rows = []
    calibres = [9, 12, 18, 25, 32, 40, 50, 65, 80]
    coil_voltages = [("BD", "24VDC"), ("B7", "24VAC"), ("E7", "48VAC"),
                     ("F7", "110VAC"), ("P7", "230VAC"), ("Q7", "400VAC")]

    for calibre, (coil_code, coil_desc) in iterproduct(calibres, coil_voltages):
        ref   = f"LC1D{calibre:02d}{coil_code}"
        props = {"calibre_A": calibre, "coil_voltage": coil_desc,
                 "poles": 3, "type": "non-reversing"}
        desc  = f"TeSys D LC1D {calibre}A bobina {coil_desc}"
        rows.append({
            "reference": ref,
            "product_description": desc,
            "product_long_description": f"Contactor TeSys D {calibre}A AC-3 3P + 1NO+1NC bobina {coil_desc} para mando de motores y cargas en cuadros de distribución",
            "strategic_product_family_description": "TeSys",
            "range": "TeSys D",
            "component_type": "contactor",
            "tier": "standard",
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": round(22.0 + calibre * 0.8, 2),
            "properties": json.dumps(props),
        })
    return rows

contactor_rows = gen_contactors()
print(f"Contactors: {len(contactor_rows)} references")

# COMMAND ----------

# DBTITLE 1,Relojes — time switches
def gen_timers():
    rows = []

    # IH mechanical (economy)
    for channels in [1, 2]:
        ref  = f"IH{'P' if channels == 2 else 'S'}24001"
        rows.append({
            "reference": ref,
            "product_description": f"Acti9 IH {channels}C 24h mecánico",
            "product_long_description": f"Reloj horario mecánico 24h {channels} canal{'es' if channels > 1 else ''} 16A 230VAC DIN para encendido programado de cargas",
            "strategic_product_family_description": "Acti9",
            "range": "IH",
            "component_type": "reloj",
            "tier": "economy",
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": 18.50 + channels * 4.0,
            "properties": json.dumps({"channels": channels, "type": "Horario", "voltage": "230VAC"}),
        })

    # IHP+ digital programmable (standard)
    for channels in [1, 2]:
        ref = f"CCT15{450 + channels - 1}"
        rows.append({
            "reference": ref,
            "product_description": f"Acti9 IHP+ {channels}C 24h/7d",
            "product_long_description": f"Reloj programable digital 24h/7 días {channels} canal{'es' if channels > 1 else ''} 16A 230VAC — programación diaria y semanal",
            "strategic_product_family_description": "Acti9",
            "range": "IHP+",
            "component_type": "reloj",
            "tier": "standard",
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": 32.0 + channels * 6.0,
            "properties": json.dumps({"channels": channels, "type": "Horario", "voltage": "230VAC"}),
        })

    # IC Astro astronomic (premium)
    for channels in [1, 2]:
        ref = f"CCT15{220 + channels}"
        rows.append({
            "reference": ref,
            "product_description": f"Acti9 IC Astro {channels}C astronómico",
            "product_long_description": f"Interruptor astronómico Acti9 IC Astro {channels} canal{'es' if channels > 1 else ''} 16A 230VAC — conmutación por orto/ocaso sin sondas externas",
            "strategic_product_family_description": "Acti9",
            "range": "IC Astro",
            "component_type": "reloj",
            "tier": "premium",
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": 68.0 + channels * 12.0,
            "properties": json.dumps({"channels": channels, "type": "Astro", "voltage": "230VAC"}),
        })

    return rows

timer_rows = gen_timers()
print(f"Timers: {len(timer_rows)} references")

# COMMAND ----------

# DBTITLE 1,Limitadores de Sobretensión — SPD
def gen_spd():
    rows = []

    configs = [
        ("A9L16294", "iPRD1 12.5r 1P+N",  "Type 1+2", 1, 12.5, "standard", 85.0),
        ("A9L16482", "iPRD1 25r 3P+N",     "Type 1+2", 4, 25.0, "standard", 175.0),
        ("A9L40294", "iPRD 40r 1P+N",      "Type 2",   2, 40.0, "standard", 62.0),
        ("A9L40482", "iPRD 40r 3P+N",      "Type 2",   4, 40.0, "standard", 120.0),
        ("A9L16296", "iPRD1 12.5r 1P+N SC","Type 1+2", 2, 12.5, "premium",  98.0),
        ("A9L40296", "iPRD 40r 1P+N SC",   "Type 2",   2, 40.0, "premium",  75.0),
        ("A9L40694", "iPRD 40r 3P+N 400V", "Type 2",   4, 40.0, "standard", 128.0),
        ("A9L16694", "iPRD1 25r 3P+N 400V","Type 1+2", 4, 25.0, "premium",  190.0),
    ]
    for ref, desc, spd_type, poles, imax_kA, tier, price in configs:
        rows.append({
            "reference": ref,
            "product_description": desc,
            "product_long_description": f"Limitador de sobretensión Acti9 {desc} — protección {spd_type} contra sobretensiones transitorias en instalaciones {('monofásicas' if poles <= 2 else 'trifásicas')}",
            "strategic_product_family_description": "Acti9",
            "range": "iPRD",
            "component_type": "limitador de sobretension",
            "tier": tier,
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": price,
            "properties": json.dumps({"poles": poles, "imax_kA": imax_kA, "type": spd_type}),
        })
    return rows

spd_rows = gen_spd()
print(f"SPDs: {len(spd_rows)} references")

# COMMAND ----------

# DBTITLE 1,Portafusibles — fuse holders
def gen_fuse_holders():
    rows = []
    configs = [
        ("A9N15636", "iSF 1P 32A 10x38",   1,  32, "10x38", 12.5),
        ("A9N15640", "iSF 2P 32A 10x38",   2,  32, "10x38", 22.0),
        ("A9N15644", "iSF 3P 32A 10x38",   3,  32, "10x38", 31.5),
        ("A9N15650", "iSF 1P 63A 14x51",   1,  63, "14x51", 18.0),
        ("A9N15652", "iSF 2P 63A 14x51",   2,  63, "14x51", 33.0),
        ("A9N15654", "iSF 3P 63A 14x51",   3,  63, "14x51", 46.0),
        ("A9N15658", "iSF 3P+N 32A 10x38", 4,  32, "10x38", 41.0),
        ("A9N15662", "iSF 3P+N 63A 14x51", 4,  63, "14x51", 60.0),
    ]
    for ref, desc, poles, calibre, fuse_size, price in configs:
        rows.append({
            "reference": ref,
            "product_description": desc,
            "product_long_description": f"Portafusibles Acti9 iSF {poles}P {calibre}A fusible cilíndrico {fuse_size} para protección de circuitos",
            "strategic_product_family_description": "Acti9",
            "range": "iSF",
            "component_type": "portafusibles",
            "tier": "standard",
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": price,
            "properties": json.dumps({"poles": poles, "calibre_A": calibre, "fuse_size": fuse_size}),
        })
    return rows

fuse_rows = gen_fuse_holders()
print(f"Fuse holders: {len(fuse_rows)} references")

# COMMAND ----------

# DBTITLE 1,Contadores de Energía — energy meters
def gen_meters():
    rows = []
    configs = [
        # ref,          desc,               range,      tier,      price,  props
        ("A9MEM3110", "iEM3110 1F directo",  "iEM3110", "economy",  48.0,  {"phases": 1, "connection": "direct",  "max_A": 63,  "tariffs": 1, "communication": None}),
        ("A9MEM3155", "iEM3155 3F directo",  "iEM3155", "economy",  72.0,  {"phases": 3, "connection": "direct",  "max_A": 63,  "tariffs": 1, "communication": None}),
        ("A9MEM3210", "iEM3210 3F TC 1/5A",  "iEM3210", "standard", 115.0, {"phases": 3, "connection": "CT",      "max_A": 999, "tariffs": 2, "communication": "pulse"}),
        ("A9MEM3255", "iEM3255 3F TC Modbus","iEM3255", "standard", 165.0, {"phases": 3, "connection": "CT",      "max_A": 999, "tariffs": 4, "communication": "Modbus"}),
        ("A9MEM3350", "iEM3350 3F directo",  "iEM3350", "premium",  142.0, {"phases": 3, "connection": "direct",  "max_A": 125, "tariffs": 4, "communication": "pulse"}),
        ("A9MEM3355", "iEM3355 3F TC multi", "iEM3355", "premium",  210.0, {"phases": 3, "connection": "CT",      "max_A": 999, "tariffs": 4, "communication": "Modbus"}),
        ("A9MEM3410", "iEM3410 3F directo",  "iEM3410", "premium",  185.0, {"phases": 3, "connection": "direct",  "max_A": 125, "tariffs": 4, "communication": "BACnet"}),
    ]
    for ref, desc, rng, tier, price, props in configs:
        rows.append({
            "reference": ref,
            "product_description": desc,
            "product_long_description": f"Contador de energía {desc} para medida y monitorización del consumo eléctrico en instalaciones {'monofásicas' if props['phases'] == 1 else 'trifásicas'}",
            "strategic_product_family_description": "Acti9",
            "range": rng,
            "component_type": "contador de energia",
            "tier": tier,
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": price,
            "properties": json.dumps(props),
        })
    return rows

meter_rows = gen_meters()
print(f"Meters: {len(meter_rows)} references")

# COMMAND ----------

# DBTITLE 1,Transferpact — transfer switches
def gen_transferpact():
    rows = []
    configs = [
        ("TA1DA3L0324TPE", "TransferPacT Active 32A 3P",  32,  3, 380.0),
        ("TA1DA4L0324TPE", "TransferPacT Active 32A 4P",  32,  4, 420.0),
        ("TA1DA4L0634TPE", "TransferPacT Active 63A 4P",  63,  4, 540.0),
        ("TA1DA4L1004TPE", "TransferPacT Active 100A 4P", 100, 4, 720.0),
        ("TA1DA4L1604TPE", "TransferPacT Active 160A 4P", 160, 4, 950.0),
        ("TA1DA4L2504TPE", "TransferPacT Active 250A 4P", 250, 4, 1450.0),
    ]
    for ref, desc, calibre, poles, price in configs:
        rows.append({
            "reference": ref,
            "product_description": desc,
            "product_long_description": f"Conmutador automático de redes TransferPacT Active {calibre}A {poles}P — conmutación automática entre red normal y grupo electrógeno con controlador integrado",
            "strategic_product_family_description": "TransferPacT",
            "range": "TransferPacT Active",
            "component_type": "inversor y conmutador de redes",
            "tier": "premium",
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": price,
            "properties": json.dumps({"poles": poles, "calibre_A": calibre, "type": "automatic", "display": "LCD"}),
        })
    return rows

transferpact_rows = gen_transferpact()
print(f"TransferPacT: {len(transferpact_rows)} references")

# COMMAND ----------

# DBTITLE 1,Interruptor de Corte en Carga — load break switches
def gen_load_break():
    rows = []
    configs = [
        (2, 25),  (2, 40),  (2, 63),  (2, 100),
        (3, 25),  (3, 40),  (3, 63),  (3, 100),
        (4, 25),  (4, 40),  (4, 63),  (4, 100), (4, 160),
    ]
    for poles, calibre in configs:
        ref   = f"A9S6{poles}{calibre:03d}"
        price = round(28.0 + poles * 5.0 + calibre * 0.35, 2)
        rows.append({
            "reference": ref,
            "product_description": f"Acti9 iSW-NA {poles}P {calibre}A",
            "product_long_description": f"Interruptor en carga Acti9 iSW-NA {poles}P {calibre}A para seccionamiento y mando manual de circuitos sin protección de cortocircuito integrada",
            "strategic_product_family_description": "Acti9",
            "range": "iSW",
            "component_type": "interruptor de corte en carga",
            "tier": "standard",
            "status": "ACTIVE",
            "superseded_by": None,
            "list_price_eur": price,
            "properties": json.dumps({"poles": poles, "calibre_A": calibre}),
        })
    return rows

load_break_rows = gen_load_break()
print(f"Load break switches: {len(load_break_rows)} references")

# COMMAND ----------

# DBTITLE 1,Assemble and write material table
all_rows = (
    ia_rows + rcd_rows + contactor_rows + timer_rows +
    spd_rows + fuse_rows + meter_rows + transferpact_rows + load_break_rows
)
print(f"\nTotal references: {len(all_rows)}")

# Deduplicate on reference (safety net)
seen = set()
deduped = []
for r in all_rows:
    if r["reference"] not in seen:
        seen.add(r["reference"])
        deduped.append(r)
print(f"After dedup: {len(deduped)}")

df_material = spark.createDataFrame(deduped)
df_material.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.material")
print(f"✓ {CATALOG}.{SCHEMA}.material written")
display(df_material.groupBy("component_type", "tier", "status").count().orderBy("component_type", "tier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Stock table
# MAGIC
# MAGIC Each active reference gets a stock entry for each distribution center.
# MAGIC Stock levels follow a realistic distribution:
# MAGIC - ~55% well stocked (10-200 units)
# MAGIC - ~20% low stock (1-9 units)
# MAGIC - ~25% out of stock (0 units)
# MAGIC
# MAGIC Discontinued references have no stock.

# COMMAND ----------

# DBTITLE 1,Generate stock rows
from datetime import datetime

def gen_stock(material_rows):
    stock_rows = []
    for row in material_rows:
        if row["status"] == "DISCONTINUED":
            continue
        for dc in DISTRIBUTION_CENTERS:
            roll = random.random()
            if roll < 0.55:
                qty = random.randint(10, 200)
            elif roll < 0.75:
                qty = random.randint(1, 9)
            else:
                qty = 0
            stock_rows.append({
                "reference":            row["reference"],
                "distribution_center":  dc,
                "qty_available":        qty,
                "last_updated":         datetime.utcnow().isoformat(),
            })
    return stock_rows

stock_rows = gen_stock(deduped)
print(f"Stock rows: {len(stock_rows)}")

df_stock = spark.createDataFrame(stock_rows)
df_stock.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.stock")
print(f"✓ {CATALOG}.{SCHEMA}.stock written")

# Summary
display(spark.sql(f"""
    SELECT
        CASE WHEN qty_available = 0 THEN 'Out of stock'
             WHEN qty_available < 10 THEN 'Low stock'
             ELSE 'In stock'
        END AS stock_status,
        COUNT(*) AS count
    FROM {CATALOG}.{SCHEMA}.stock
    GROUP BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Work orders table
# MAGIC
# MAGIC For every out-of-stock entry with an active reference, there is a ~65% chance
# MAGIC of an incoming work order. Expected delivery dates range from 5 to 45 days out.

# COMMAND ----------

# DBTITLE 1,Generate work orders
def gen_work_orders(stock_rows):
    wo_rows = []
    today = date.today()
    for s in stock_rows:
        if s["qty_available"] == 0 and random.random() < 0.65:
            eta_days = random.randint(5, 45)
            wo_rows.append({
                "order_id":            str(uuid.uuid4()),
                "reference":           s["reference"],
                "distribution_center": s["distribution_center"],
                "qty_incoming":        random.randint(20, 150),
                "expected_date":       (today + timedelta(days=eta_days)).isoformat(),
            })
    return wo_rows

wo_rows = gen_work_orders(stock_rows)
print(f"Work orders: {len(wo_rows)}")

df_wo = spark.createDataFrame(wo_rows)
df_wo.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.work_orders")
print(f"✓ {CATALOG}.{SCHEMA}.work_orders written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verification

# COMMAND ----------

# DBTITLE 1,Verify all tables
for tbl, min_rows in [
    ("material",    400),
    ("stock",       1000),
    ("work_orders", 200),
]:
    cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.{tbl}").collect()[0]["n"]
    status = "✓" if cnt >= min_rows else "✗"
    print(f"  {status}  {CATALOG}.{SCHEMA}.{tbl:<15} {cnt:>6} rows")

print()
print("Sample — material:")
display(spark.sql(f"SELECT reference, product_description, tier, status, list_price_eur FROM {CATALOG}.{SCHEMA}.material LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Grant read access to app service principal
# MAGIC
# MAGIC The Databricks App and serving endpoint need read access to these tables.
# MAGIC Uncomment and run if your app uses a service principal.

# COMMAND ----------

# # DBTITLE 1,Grants (uncomment if needed)
# SP = "your-service-principal@your-tenant.com"
# for tbl in ["material", "stock", "work_orders"]:
#     spark.sql(f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.{tbl} TO `{SP}`")
#     print(f"Granted SELECT on {tbl} to {SP}")
