import pandas as pd
from faker import Faker
import random, uuid
from datetime import datetime, timedelta
fake=Faker()
Faker.seed(42)
random.seed(42)
CARRIERS=['Maersk', 'MSC', 'CMA-CGM', 'COSCO', 'Evergreen', 'Marsa Maroc', 'Comanav']
ROUTES=[('Tanger Med', 'Rotterdam'), ('Tanger Med', 'Barcelona'), ('Casablanca', 'Marseille'),
            ('Agadir', 'Hamburg'),('Shanghai', 'Rotterdam'), ('Shenzhen', 'Hamburg'),
            ('Singapore', 'Felixstowe'), ('Busan', 'Los Angeles'),
            ('Ningbo', 'New York')]
CARRIER_RELIABILITY={
    'Maersk':0.88,'MSC':0.82,'CMA-CGM':0.85,
    'COSCO': 0.79,'Evergreen':0.76,
    'Marsa Maroc':0.83,'Comanav':0.78
}
records=[]
for _ in range(5000):
    origin, dest=random.choice(ROUTES)
    carrier     =random.choice(CARRIERS)
    planned_eta = fake.date_time_between_dates(
    datetime_start=datetime(2026, 3, 1),
    datetime_end=datetime(2026, 9, 15)
)
    transaction_date=planned_eta-timedelta(days=random.randint(5,30))
    reliability =CARRIER_RELIABILITY[carrier]
    delayed     =random.random() > reliability
    delay_hours =round(random.uniform(12, 120),1) if delayed else 0
    actual_eta  =planned_eta + timedelta(hours=delay_hours)
    records.append({
        'transaction_date': transaction_date.isoformat(),
        'shipment_id':  str(uuid.uuid4()),
        'carrier':      carrier,
        'origin_city':  origin,
        'dest_city':    dest,
        'planned_eta':  planned_eta.isoformat(),
        'actual_eta':   actual_eta.isoformat(),
        'delay_hours':  delay_hours,
        'weight_kg':    round(random.uniform(500, 25000), 1),
        'commodity':    random.choice(['Electronics','Automotive',
                                        'Textiles','Chemicals','Food'])
    })
df = pd.DataFrame(records)
df.to_csv('data/shipments_raw.csv', index=False)
print(f'Generated {len(df)} shipment records')
print(df['planned_eta'].min())
print(df['planned_eta'].max())
print(df['planned_eta'].nunique())