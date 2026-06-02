import os
import csv
import json
import gzip
import random
from datetime import datetime, timedelta

# Crear carpetas de salida
os.makedirs('data', exist_ok=True)

# 1. Cuentas Beneficiadas (Metadatos de Ingesta para cruce en Spark)
with gzip.open('data/free_accounts.csv.gz', 'wt', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['account_id', 'account_name', 'client_segment', 'benefit_type', 'activation_date'])
    segments = ['ENTERPRISE', 'SME', 'FINTECH_PARTNER']
    promos = ['FREE_PROMO_90D', 'PARTNER_ZERO_FEE', 'WELCOME_OFFER']
    for idx in range(100, 150):
        writer.writerow([
            f"ACC_{idx}",
            f"Global Partner {idx} S.A.C.",
            random.choice(segments),
            random.choice(promos),
            (datetime(2026, 1, 1) + timedelta(days=random.randint(0, 100))).strftime('%Y-%m-%d')
        ])

# 2. Historial de Transacciones de Garantías (7 dias con volumen incremental)
start_date = datetime(2026, 5, 1)
countries = ['CL', 'PE', 'MX', 'BR']
statuses = ['APPROVED', 'APPROVED', 'APPROVED', 'APPROVED', 'PENDING_RISK_AUDIT', 'DECLINED']

for day in range(7):
    current_day = start_date + timedelta(days=day)
    day_str = current_day.strftime('%Y-%m-%d')
    day_dir = f"data/transactions/day={day_str}"
    os.makedirs(day_dir, exist_ok=True)
    
    with open(f"{day_dir}/transactions.csv", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'transaction_id', 'source_account_id', 'target_account_id', 
            'guarantee_amount_usd', 'fee_amount_usd', 'status', 'timestamp', 'country_code'
        ])
        
        for i in range(2000):
            t_id = f"TX_{day:02d}_{100000 + i}"
            status = random.choice(statuses)
            
            # Ciclo de riesgo circular el dia 3
            if day >= 2 and i < 5:
                status = 'APPROVED'
                if i == 0:
                    src, tgt = "ACC_101", "ACC_102"
                elif i == 1:
                    src, tgt = "ACC_102", "ACC_103"
                elif i == 2:
                    src, tgt = "ACC_103", "ACC_101"
                else:
                    src = f"ACC_{random.randint(100, 999)}"
                    tgt = f"ACC_{random.randint(100, 999)}"
            else:
                src = f"ACC_{random.randint(100, 999)}"
                tgt = f"ACC_{random.randint(100, 999)}"
                
            amount = round(random.uniform(500.0, 75000.0), 2)
            if day >= 4 and src in ["ACC_101", "ACC_102", "ACC_103"]:
                amount = round(amount * 5.0, 2)
                
            fee = round(amount * random.uniform(0.015, 0.035), 2)
            ts = current_day + timedelta(seconds=random.randint(0, 86400 - 1))
            country = random.choice(countries)
            
            writer.writerow([
                t_id, src, tgt, amount, fee, status, ts.strftime('%Y-%m-%d %H:%M:%S'), country
            ])

# 3. Datos de Proveedores Incompletos desde SAP (Sin Dominio Web de Internet y con datos PII)
# FASE CENCOSUD TPRM: Nombre legal y pais, pero sin dominio. Posee tax_id que es PII (dato sensible).
company_list = [
    {"name": "NovaTech Solutions", "country": "CL", "tax_id": "96884102-K"},
    {"name": "Andina Seguros", "country": "PE", "tax_id": "20554109841"},
    {"name": "Patagonia Guarantees", "country": "BR", "tax_id": "45.882.109/0001-32"},
    {"name": "MexiCredit Corp", "country": "MX", "tax_id": "MCR961023-HJ1"},
    {"name": "Apex Finance Group", "country": "CL", "tax_id": "76442109-1"},
    {"name": "Pacifico Avales", "country": "PE", "tax_id": "10428810231"},
    {"name": "Boreal Insurtech", "country": "BR", "tax_id": "12.441.980/0001-09"},
    {"name": "Atlantica Garantias", "country": "MX", "tax_id": "ATG881109-KL2"},
    {"name": "Sierra Venture Partners", "country": "CL", "tax_id": "89441203-9"},
    {"name": "Austral Leasing", "country": "PE", "tax_id": "20443198021"}
]

sap_vendors = []
for idx in range(100, 300):
    base_comp = random.choice(company_list)
    sap_vendors.append({
        "vendor_id": f"VEN_{idx}",
        "legal_name": f"{base_comp['name']} Division {idx} S.A.",
        "country": base_comp['country'],
        "tax_id_pii": f"{base_comp['tax_id']}-{idx}",
        "search_api_endpoint": f"http://localhost:8000/mock_search_api/{idx}"
    })

with open('data/sap_vendors.json', 'w') as f:
    json.dump(sap_vendors, f, indent=2)

print("Datos de negocio (SAP, Transacciones y Promociones) generados exitosamente!")
