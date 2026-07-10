"""NimbusMart seed data generator.

Deterministic (fixed seed) so every lab's expected output is stable.
Emits data/nimbusmart/seed.js defining window.NIMBUS = { <table>: [rows...] }.

Engineered facts the course depends on (do not break):
  - FRAUD_REVIEW_THRESHOLD = 0.80
  - Exactly 43 orders have fraud_score >= 0.80 (the review queue).
  - Exactly 4 of those are exactly 0.80 (teaches inclusive boundary).
  - 15 orders have NO fraud_scores row (teaches left join nulls).
  - seller_id 'S-777' (MegaDeals) owns ~35% of orders (the skew hot key, E3).
  - customers master is messy: dup emails w/ casing, null cities, padded names.
  - customer_updates feeds the SCD2 lab (B2): customers who moved cities.
  - courier_pings has late data: ingested_at lags event_ts, sometimes > 1h.
"""
import json
import random
from pathlib import Path

random.seed(42)

OUT = Path(__file__).parent / "seed.js"

FRAUD_REVIEW_THRESHOLD = 0.80

COUNTRIES = ["DE", "US", "IN", "BR", "JP", "FR", "AU"]
CITIES = {
    "DE": ["Berlin", "Munich", "Hamburg"],
    "US": ["Austin", "Seattle", "Denver"],
    "IN": ["Pune", "Bengaluru", "Chennai"],
    "BR": ["Sao Paulo", "Recife"],
    "JP": ["Osaka", "Tokyo"],
    "FR": ["Lyon", "Paris"],
    "AU": ["Sydney", "Perth"],
}
FIRST = ["Ana", "Ben", "Chiara", "Dev", "Elif", "Farid", "Grace", "Hiro", "Ines",
         "Jonas", "Kavya", "Liam", "Mei", "Noor", "Otto", "Priya", "Quentin",
         "Rosa", "Sam", "Tunde", "Uma", "Viktor", "Wei", "Ximena", "Yusuf", "Zoe"]
LAST = ["Alvarez", "Brandt", "Chen", "Desai", "Eriksen", "Fischer", "Gupta",
        "Haas", "Ito", "Jansen", "Kimura", "Lopez", "Mehta", "Novak", "Okafor",
        "Petrov", "Quispe", "Rossi", "Sato", "Tanaka", "Ueda", "Vogel", "Weber",
        "Xu", "Yamada", "Zhou"]


def ts(day_offset, hour, minute, second=0):
    """Timestamp within May-June 2026 as ISO string. day_offset 0 = 2026-05-01."""
    import datetime
    base = datetime.datetime(2026, 5, 1)
    t = base + datetime.timedelta(days=day_offset, hours=hour, minutes=minute, seconds=second)
    return t.strftime("%Y-%m-%dT%H:%M:%S")


# ---------------------------------------------------------------- customers
customers = []
for i in range(1, 61):
    cid = f"C-{i:04d}"
    country = random.choice(COUNTRIES)
    city = random.choice(CITIES[country])
    name = f"{random.choice(FIRST)} {random.choice(LAST)}"
    email = name.lower().replace(" ", ".") + f"{i}@example.com"
    segment = "business" if random.random() < 0.2 else "consumer"
    created = ts(random.randint(-120, 0), random.randint(8, 20), random.randint(0, 59))
    customers.append({
        "customer_id": cid, "name": name, "email": email, "city": city,
        "country": country, "segment": segment, "created_at": created,
    })

# Messiness (intentional, taught in C4/B2):
customers[7]["city"] = None
customers[19]["city"] = None
customers[33]["city"] = None
customers[11]["name"] = "  " + customers[11]["name"] + " "   # padded whitespace
customers[24]["name"] = customers[24]["name"].upper()          # casing drift
# Duplicate emails with different casing (dedup lab):
customers[40]["email"] = customers[14]["email"].upper()
customers[51]["email"] = customers[22]["email"].title()
# C-0042 is the SCD2 poster child: moved cities mid-quarter (see customer_updates)

customer_updates = [
    {"customer_id": "C-0042", "city": "Munich",  "country": "DE", "updated_at": ts(35, 9, 12)},
    {"customer_id": "C-0042", "city": "Hamburg", "country": "DE", "updated_at": ts(52, 14, 3)},
    {"customer_id": "C-0007", "city": "Denver",  "country": "US", "updated_at": ts(40, 11, 45)},
    {"customer_id": "C-0013", "city": "Chennai", "country": "IN", "updated_at": ts(22, 16, 20)},
    {"customer_id": "C-0027", "city": "Paris",   "country": "FR", "updated_at": ts(47, 10, 5)},
    {"customer_id": "C-0031", "city": "Tokyo",   "country": "JP", "updated_at": ts(29, 13, 55)},
    {"customer_id": "C-0055", "city": "Perth",   "country": "AU", "updated_at": ts(58, 8, 30)},
    {"customer_id": "C-0019", "city": "Recife",  "country": "BR", "updated_at": ts(44, 15, 10)},
]

# ---------------------------------------------------------------- products
DEPTS = [("electronics", ["audio", "computing", "photo"]),
         ("home", ["kitchen", "lighting", "storage"]),
         ("outdoors", ["camping", "cycling"]),
         ("toys", ["building", "games"])]
BRANDS = ["Voltix", "Kestrel", "Brauer", "Nimbo", "Taiga", "Quanta"]
ADJ = ["Compact", "Pro", "Classic", "Ultra", "Mini", "Twin", "Smart"]
NOUN = ["Speaker", "Kettle", "Lamp", "Tent", "Drone", "Keyboard", "Blender",
        "Backpack", "Tripod", "Router", "Headphones", "Multitool"]
products = []
for i in range(40):
    pid = f"P-{100 + i}"
    dept, aisles = random.choice(DEPTS)
    products.append({
        "product_id": pid,
        "name": f"{random.choice(BRANDS)} {random.choice(ADJ)} {random.choice(NOUN)}",
        "category": {"dept": dept, "aisle": random.choice(aisles)},
        "price": round(random.uniform(9, 480), 2),
        "tags": random.sample(["bestseller", "eco", "new", "clearance", "premium", "bundle"],
                              k=random.randint(1, 3)),
        "attrs": {"brand": random.choice(BRANDS), "weight_kg": round(random.uniform(0.1, 9.5), 2)},
    })

# ---------------------------------------------------------------- orders
SELLERS = ["S-101", "S-204", "S-355", "S-410", "S-777", "S-812", "S-903"]
STATUSES = ["placed", "shipped", "delivered", "delivered", "delivered", "cancelled", "returned"]
orders = []
for i in range(240):
    oid = f"O-{10001 + i}"
    cust = random.choice(customers)
    # Skew: MegaDeals (S-777) owns ~35% of order volume
    seller = "S-777" if random.random() < 0.35 else random.choice(SELLERS[:4] + SELLERS[5:])
    status = random.choice(STATUSES)
    orders.append({
        "order_id": oid,
        "customer_id": cust["customer_id"],
        "seller_id": seller,
        "order_ts": ts(random.randint(0, 60), random.randint(0, 23), random.randint(0, 59)),
        "status": status,
        "total_amount": round(random.uniform(8, 950), 2),
        "item_count": random.randint(1, 6),
        "country": cust["country"],
        "channel": random.choice(["web", "app", "app"]),
    })

# ---------------------------------------------------------------- fraud_scores
# 15 orders deliberately unscored; exactly 43 scored >= 0.80, 4 of them == 0.80.
unscored = set(random.sample([o["order_id"] for o in orders], 15))
scored_orders = [o for o in orders if o["order_id"] not in unscored]
review_ids = set(random.sample([o["order_id"] for o in scored_orders], 43))
exact_ids = set(random.sample(sorted(review_ids), 4))
fraud_scores = []
for o in scored_orders:
    oid = o["order_id"]
    if oid in exact_ids:
        score = 0.80
    elif oid in review_ids:
        score = round(random.uniform(0.81, 0.99), 2)
    else:
        score = round(random.uniform(0.01, 0.79), 2)
    fraud_scores.append({
        "order_id": oid,
        "fraud_score": score,
        "model_version": random.choice(["v3.1", "v3.1", "v3.2"]),
        "scored_at": o["order_ts"][:11] + f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
    })

# ---------------------------------------------------------------- payments
METHODS = ["card", "card", "wallet", "bank", "cod"]
payments = []
pnum = 50001
for o in orders:
    if o["status"] == "cancelled" and random.random() < 0.5:
        continue  # some cancelled orders never paid
    status = "refunded" if o["status"] == "returned" else (
        "failed" if random.random() < 0.05 else "captured")
    payments.append({
        "payment_id": f"PAY-{pnum}",
        "order_id": o["order_id"],
        "method": random.choice(METHODS),
        "amount": o["total_amount"],
        "status": status,
    })
    pnum += 1

# ---------------------------------------------------------------- order_events (clickstream, drifty)
EVENT_TYPES = ["cart_add", "checkout_start", "payment_submitted", "fraud_check",
               "fulfillment_hold", "shipped_scan"]
order_events = []
enum = 90001
for o in random.sample(orders, 180):
    for et in random.sample(EVENT_TYPES, k=random.randint(1, 3)):
        ev = {
            "event_id": f"E-{enum}",
            "order_id": o["order_id"],
            "event_type": et,
            "event_ts": o["order_ts"],
            "device": random.choice(["ios", "android", "web"]),
        }
        # Schema drift, on purpose: newer app versions add a field; some drop device
        r = random.random()
        if r < 0.06:
            del ev["device"]
        elif r < 0.14:
            ev["app_version"] = random.choice(["4.1.0", "4.2.1"])
        order_events.append(ev)
        enum += 1
order_events = order_events[:400]

# ---------------------------------------------------------------- couriers + pings (event time, late data)
couriers = [{"courier_id": f"K-{i:02d}",
             "name": f"{random.choice(FIRST)} {random.choice(LAST)}",
             "home_zone": random.choice(["north", "south", "east", "west", "central"])}
            for i in range(1, 13)]

courier_pings = []
gnum = 70001
delivered = [o for o in orders if o["status"] in ("shipped", "delivered")][:110]
for o in delivered:
    k = random.choice(couriers)
    day = random.randint(0, 60)
    hour = random.randint(6, 20)
    for j, st in enumerate(["picked_up", "in_transit", "delivered"][:random.randint(2, 3)]):
        ev_min = random.randint(0, 59)
        lag_min = random.choice([1, 2, 3, 4, 5, 8, 12, 95, 130])  # some VERY late (>1h)
        courier_pings.append({
            "ping_id": f"G-{gnum}",
            "courier_id": k["courier_id"],
            "order_id": o["order_id"],
            "status": st,
            "zone": random.choice(["north", "south", "east", "west", "central"]),
            "event_ts": ts(day, min(hour + j, 23), ev_min),
            "ingested_at": ts(day, min(hour + j + (lag_min + ev_min) // 60, 23),
                              (ev_min + lag_min) % 60),
        })
        gnum += 1
courier_pings = courier_pings[:320]

# ---------------------------------------------------------------- emit
tables = {
    "customers": customers,
    "customer_updates": customer_updates,
    "products": products,
    "orders": orders,
    "fraud_scores": fraud_scores,
    "payments": payments,
    "order_events": order_events,
    "couriers": couriers,
    "courier_pings": courier_pings,
}

# Sanity gates for engineered facts
review = [f for f in fraud_scores if f["fraud_score"] >= FRAUD_REVIEW_THRESHOLD]
exact = [f for f in fraud_scores if f["fraud_score"] == FRAUD_REVIEW_THRESHOLD]
assert len(review) == 43, f"review queue must be 43, got {len(review)}"
assert len(exact) == 4, f"exactly-0.80 rows must be 4, got {len(exact)}"
assert len(orders) - len(fraud_scores) == 15
hot = sum(1 for o in orders if o["seller_id"] == "S-777")
assert hot / len(orders) > 0.25, f"S-777 skew too weak: {hot}/{len(orders)}"

js = "/* generated by data/nimbusmart/generate.py — do not edit by hand */\n"
js += "window.NIMBUS = " + json.dumps(tables, separators=(",", ":")) + ";\n"
js += f"window.FRAUD_REVIEW_THRESHOLD = {FRAUD_REVIEW_THRESHOLD};\n"
OUT.write_text(js, encoding="utf-8")

print(f"seed.js written: {OUT.stat().st_size:,} bytes")
for name, rows in tables.items():
    print(f"  {name:16s} {len(rows):4d} rows")
print(f"  review queue (score >= {FRAUD_REVIEW_THRESHOLD}): {len(review)} | exactly 0.80: {len(exact)} | unscored: 15 | S-777 share: {hot}/{len(orders)}")
