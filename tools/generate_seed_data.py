"""Deterministically generate the synthetic UDL/BDL seed data used by both the
legacy (Databricks) and target (BigQuery) runs. Re-running produces identical CSVs."""
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "legacy" / "data" / "seed"
R = random.Random(20241014)
START, DAYS = date(2024, 1, 1), 120

COUNTRIES = {"GB": "GBP", "DE": "EUR", "FR": "EUR", "NL": "EUR", "IN": "INR", "US": "USD", "PL": "PLN"}
REGION = {"GB": "UKI", "DE": "DACH", "FR": "WEU", "NL": "WEU", "IN": "SA", "US": "NA", "PL": "CEE"}
RETAILERS = ["Tesco", "Carrefour", "Ahold", "Lidl", "Reliance", "Walmart", "Rewe", "Biedronka"]
FORMATS = ["hypermarket", "supermarket", "convenience", "ecom_dark_store"]
BRANDS = {
    "Brightwave": ("Home Care", ["Laundry", "Dishwash"]),
    "Verdana": ("Beauty & Wellbeing", ["Skin Care", "Hair Care"]),
    "Nordhaven": ("Personal Care", ["Deodorant", "Body Wash"]),
    "Saffra": ("Foods", ["Condiments", "Soups"]),
    "Glacio": ("Ice Cream", ["Tubs", "Sticks"]),
}
CHANNELS = ["store", "store", "store", "ecom", "click_collect"]


def write(name, header, rows):
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / f"{name}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"{name}: {len(rows)} rows")


def d(x):
    return x.isoformat() if x else ""


def ts(x):
    return x.strftime("%Y-%m-%d %H:%M:%S")


def main():
    stores = []
    for i in range(1, 61):
        c = R.choice(list(COUNTRIES))
        stores.append([f"ST{i:04d}", R.choice(RETAILERS), c, REGION[c], R.choice(FORMATS),
                       d(date(2010, 1, 1) + timedelta(days=R.randint(0, 4800))), ts(datetime(2023, 12, 31, 23, 0, 0))])
    write("udl__stores", ["store_id", "retailer", "country", "region", "store_format", "open_date", "updated_at"], stores)

    updates = []
    for s in R.sample(stores, 14):
        u = list(s)
        u[4] = R.choice([f for f in FORMATS if f != s[4]])
        u[6] = ts(datetime(2024, 3, 1) + timedelta(hours=R.randint(0, 600)))
        updates.append(u)
    for i in range(61, 65):
        c = R.choice(list(COUNTRIES))
        updates.append([f"ST{i:04d}", R.choice(RETAILERS), c, REGION[c], "ecom_dark_store", "2024-03-15", ts(datetime(2024, 3, 15, 8))])
    dup = list(updates[0]); dup[6] = ts(datetime(2024, 4, 20, 9)); dup[4] = "supermarket"
    updates.append(dup)
    write("udl__store_updates", ["store_id", "retailer", "country", "region", "store_format", "open_date", "updated_at"], updates)

    products = []
    n = 1
    for brand, (cat, subs) in BRANDS.items():
        for sub in subs:
            for _ in range(12):
                products.append([f"SKU{n:05d}", f"{brand} {sub} {R.choice(['Original', 'Fresh', 'Sensitive', 'Max', 'Eco'])} {n}",
                                 brand, cat, sub, R.choice([150, 250, 400, 500, 750, 1000]),
                                 d(date(2018, 1, 1) + timedelta(days=R.randint(0, 2300))), "true" if R.random() > 0.08 else "false"])
                n += 1
    write("udl__products", ["sku", "product_name", "brand", "category", "subcategory", "pack_size_ml", "launch_date", "is_active"], products)
    skus = [p[0] for p in products]
    price = {s: round(R.uniform(1.5, 14.0), 2) for s in skus}

    fx = []
    base = {"EUR": 1.0, "GBP": 1.16, "USD": 0.92, "INR": 0.011, "PLN": 0.23}
    for cur, b in base.items():
        rate = b
        for k in range(DAYS):
            day = START + timedelta(days=k)
            rate = rate * (1 + R.uniform(-0.004, 0.004)) if cur != "EUR" else 1.0
            if day.weekday() >= 5 and cur != "EUR":
                continue  # no published rates at weekends -> as-of join required
            fx.append([cur, d(day), round(rate, 6)])
    write("udl__fx_rates", ["currency", "rate_date", "rate_to_eur"], fx)

    promos = []
    for i in range(1, 81):
        st = START + timedelta(days=R.randint(0, DAYS - 20))
        promos.append([f"PR{i:04d}", R.choice(skus), R.choice(RETAILERS), d(st), d(st + timedelta(days=R.randint(6, 20))),
                       R.choice(["TPR", "BOGOF", "multibuy", "display"]), round(R.choice([0.1, 0.15, 0.2, 0.25, 0.33]), 2)])
    write("udl__promotions", ["promo_id", "sku", "retailer", "start_date", "end_date", "promo_type", "discount_pct"], promos)

    sales = []
    for t in range(1, 14001):
        s = R.choice(stores)
        sku = R.choice(skus)
        when = datetime.combine(START, datetime.min.time()) + timedelta(days=R.randint(0, DAYS - 1), seconds=R.randint(0, 86399))
        qty = R.choice([1, 1, 1, 2, 2, 3, 4, 6]) * (-1 if R.random() < 0.03 else 1)
        cur = COUNTRIES[s[2]]
        unit = round(price[sku] * (1 / base[cur]) * R.uniform(0.95, 1.05), 2)
        disc = round(unit * abs(qty) * R.choice([0, 0, 0, 0.1, 0.2]), 2)
        sku_raw = sku if R.random() > 0.05 else f" {sku.lower()} "
        row = [f"T{t:07d}", ts(when), ts(when + timedelta(minutes=R.randint(5, 600))), s[0], sku_raw, qty, unit, disc,
               cur if R.random() > 0.01 else "", R.choice(CHANNELS)]
        sales.append(row)
        if R.random() < 0.04:  # late-arriving correction for the same txn_id
            corr = list(row)
            corr[2] = ts(when + timedelta(days=1, minutes=R.randint(0, 600)))
            corr[5] = max(1, abs(qty) - 1)
            sales.append(corr)
    write("udl__sales_transactions", ["txn_id", "txn_ts", "ingest_ts", "store_id", "sku", "qty", "unit_price", "discount_amt", "currency", "channel"], sales)

    inv = []
    pairs = [(R.choice(stores)[0], R.choice(skus)) for _ in range(250)]
    for wk in range(DAYS // 7):
        day = START + timedelta(days=7 * wk)
        for st, sku in pairs:
            inv.append([d(day), st, sku, max(0, int(R.gauss(40, 25)))])
    write("udl__inventory_snapshots", ["snapshot_date", "store_id", "sku", "on_hand_units"], inv)

    ship = []
    for i in range(1, 6001):
        promised = START + timedelta(days=R.randint(0, DAYS - 1))
        ordered = R.choice([12, 24, 48, 96])
        shipped = ordered if R.random() > 0.12 else ordered - R.randint(1, ordered // 2)
        delivered = None if R.random() < 0.02 else promised + timedelta(days=R.choice([-1, 0, 0, 0, 0, 1, 2]))
        ship.append([f"SH{i:06d}", f"DC{R.randint(1, 6):02d}", R.choice(stores)[0], R.choice(skus), ordered, shipped, d(promised), d(delivered)])
    write("udl__shipments", ["shipment_id", "dc_id", "store_id", "sku", "ordered_units", "shipped_units", "promised_date", "delivered_date"], ship)

    media = []
    for k in range(DAYS):
        day = START + timedelta(days=k)
        for brand in BRANDS:
            for c in COUNTRIES:
                for ch in ["tv", "search", "social", "retail_media"]:
                    if R.random() < 0.35:
                        spend = round(R.uniform(200, 9000), 2)
                        media.append([d(day), brand, c, ch, spend, int(spend * R.uniform(80, 400))])
    write("bdl__media_spend", ["spend_date", "brand", "country", "media_channel", "spend_eur", "impressions"], media)

    reviews = []
    words = ["love it", "too expensive", "smells great", "packaging leaked", "will buy again", "meh", "best in class"]
    for i in range(1, 3001):
        reviews.append([f"RV{i:06d}", R.choice(skus), R.choice(RETAILERS), d(START + timedelta(days=R.randint(0, DAYS - 1))),
                        R.choices([1, 2, 3, 4, 5], weights=[5, 7, 15, 35, 38])[0], R.choice(words)])
    write("bdl__customer_reviews", ["review_id", "sku", "retailer", "review_date", "rating", "review_text"], reviews)

    fc = []
    for wk in range(DAYS // 7):
        ws = START + timedelta(days=7 * wk)
        for sku in R.sample(skus, 40):
            for c in R.sample(list(COUNTRIES), 3):
                for v in ["v1", "v2"]:
                    fc.append([d(ws), sku, c, round(R.uniform(0, 12), 2), v])
    write("bdl__demand_forecast", ["week_start", "sku", "country", "forecast_units", "forecast_version"], fc)

    sus = [[s, round(R.uniform(8, 60), 1), round(R.uniform(0, 1), 3), round(R.uniform(50, 900), 1)] for s in skus]
    write("bdl__sustainability_metrics", ["sku", "plastic_g", "recycled_content_pct", "carbon_g_co2e"], sus)


if __name__ == "__main__":
    main()
