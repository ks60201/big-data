from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from faker import Faker
import random
import time
from datetime import datetime, timedelta
import json
import os
import threading

# Initialize Faker
fake = Faker()

# Kafka Configuration
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka-1:9092,kafka-2:9092,kafka-3:9092').split(',')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'real-estate-listings')
TOPIC_PARTITIONS = int(os.getenv('TOPIC_PARTITIONS', 6))
TOPIC_REPLICATION = int(os.getenv('TOPIC_REPLICATION', 3))
BATCH_SIZE = 1000
THREADS = 3

# Constants for realistic data modeling (from your existing code)
CITIES = {
    'New York': {'state': 'NY', 'zip_prefix': '10', 'price_multiplier': 1.8},
    'Los Angeles': {'state': 'CA', 'zip_prefix': '90', 'price_multiplier': 1.6},
    'Chicago': {'state': 'IL', 'zip_prefix': '60', 'price_multiplier': 1.2},
    'Houston': {'state': 'TX', 'zip_prefix': '77', 'price_multiplier': 1.1},
    'Phoenix': {'state': 'AZ', 'zip_prefix': '85', 'price_multiplier': 1.0},
    'Philadelphia': {'state': 'PA', 'zip_prefix': '19', 'price_multiplier': 1.3},
    'San Antonio': {'state': 'TX', 'zip_prefix': '78', 'price_multiplier': 0.9},
    'San Diego': {'state': 'CA', 'zip_prefix': '92', 'price_multiplier': 1.5},
    'Dallas': {'state': 'TX', 'zip_prefix': '75', 'price_multiplier': 1.1},
    'San Jose': {'state': 'CA', 'zip_prefix': '95', 'price_multiplier': 1.7}
}

PROPERTY_TYPES = {
    'Apartment': {'sqft_range': (500, 2000), 'bedroom_range': (1, 3), 'bathroom_range': (1, 2.5)},
    'House': {'sqft_range': (1000, 5000), 'bedroom_range': (2, 5), 'bathroom_range': (1.5, 4)},
    'Villa': {'sqft_range': (2000, 8000), 'bedroom_range': (3, 6), 'bathroom_range': (2, 5)},
    'Condo': {'sqft_range': (600, 2500), 'bedroom_range': (1, 3), 'bathroom_range': (1, 3)},
    'Townhouse': {'sqft_range': (1200, 3000), 'bedroom_range': (2, 4), 'bathroom_range': (1.5, 3.5)},
    'Land': {'sqft_range': (5000, 50000), 'bedroom_range': (0, 0), 'bathroom_range': (0, 0)}
}

TRANSACTION_TYPES = ['Sale', 'Rent', 'Lease']
AMENITIES = ['Pool', 'Gym', 'Parking', 'Garden', 'Fireplace', 'Balcony', 'Elevator', 'Security']
CONSTRUCTION_TYPES = ['Brick', 'Wood', 'Concrete', 'Steel', 'Mixed']
NEIGHBORHOOD_CLASSES = ['A', 'B', 'C']

# Pre-generate agent data for consistency
AGENTS = [{'id': f"AGT-{i:05d}", 'name': fake.name(), 'company': fake.company()} for i in range(1, 1001)]

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=10,
    linger_ms=50,
    batch_size=16384,
    compression_type='snappy',
    max_in_flight_requests_per_connection=5
)


def create_topic_if_not_exists():
    """Create Kafka topic if it doesn't exist"""
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
        topic_list = [NewTopic(
            name=TOPIC_NAME,
            num_partitions=TOPIC_PARTITIONS,
            replication_factor=TOPIC_REPLICATION
        )]
        admin.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Created topic {TOPIC_NAME} with {TOPIC_PARTITIONS} partitions and replication {TOPIC_REPLICATION}")
    except Exception as e:
        print(f"Topic creation error (may already exist): {str(e)}")


def generate_realistic_price(property_type, city, sqft, bedrooms, year_built, neighborhood_class):
    """Generate realistic price based on multiple factors"""
    base_price_per_sqft = {
        'Apartment': 200, 'House': 180, 'Villa': 250, 'Condo': 220, 'Townhouse': 190, 'Land': 50
    }

    price = base_price_per_sqft[property_type] * sqft
    price *= CITIES[city]['price_multiplier']

    if bedrooms > 0:
        price *= (1 + (bedrooms - 1) * 0.15)

    age = datetime.now().year - year_built
    if age < 5:
        price *= 1.1
    elif age > 30:
        price *= 0.8
    elif age > 50:
        price *= 0.7

    neighborhood_multiplier = {'A': 1.5, 'B': 1.1, 'C': 0.8}
    price *= neighborhood_multiplier[neighborhood_class]
    price *= random.uniform(0.95, 1.05)

    return round(price, 2)


def generate_property_description(property_type, bedrooms, bathrooms, amenities):
    """Generate a realistic property description"""
    descriptors = {
        'Apartment': ['spacious', 'modern', 'cozy', 'luxury', 'renovated'],
        'House': ['charming', 'stunning', 'beautiful', 'family-sized', 'detached'],
        'Villa': ['luxurious', 'exclusive', 'prestigious', 'majestic', 'private'],
        'Condo': ['contemporary', 'sleek', 'urban', 'low-maintenance', 'secure'],
        'Townhouse': ['elegant', 'well-maintained', 'multi-level', 'rowhouse', 'townhome'],
        'Land': ['prime', 'developable', 'vacant', 'residential', 'commercial']
    }

    desc = random.choice(descriptors[property_type])
    if bedrooms > 0:
        desc += f" {bedrooms} bedroom{'s' if bedrooms > 1 else ''}"
    if bathrooms > 0:
        desc += f", {bathrooms} bathroom{'s' if bathrooms > 1 else ''}"
    desc += f" {property_type.lower()}"
    if amenities:
        desc += " featuring " + ", ".join(amenities[:3])
        if len(amenities) > 3:
            desc += " and more"
    desc += ". " + fake.sentence()
    return desc.capitalize()


def generate_real_estate_record():
    """Generate a single realistic real estate record"""
    city = random.choice(list(CITIES.keys()))
    city_data = CITIES[city]
    property_type = random.choice(list(PROPERTY_TYPES.keys()))
    property_data = PROPERTY_TYPES[property_type]
    transaction_type = random.choice(TRANSACTION_TYPES)
    neighborhood_class = random.choice(NEIGHBORHOOD_CLASSES)

    sqft = random.randint(*property_data['sqft_range'])
    bedrooms = random.randint(*property_data['bedroom_range']) if property_data['bedroom_range'][1] > 0 else 0
    bathrooms = round(random.uniform(*property_data['bathroom_range']), 1) if property_data['bathroom_range'][
                                                                                  1] > 0 else 0

    year_built = random.choices(
        range(1900, datetime.now().year + 1),
        weights=[1 / (2025 - y + 1) for y in range(1900, 2026)]
    )[0]

    price = generate_realistic_price(property_type, city, sqft, bedrooms, year_built, neighborhood_class)
    listing_date = fake.date_between(
        start_date=datetime.now() - timedelta(days=365 * 2),
        end_date=datetime.now()
    ).strftime('%Y-%m-%d')

    selected_amenities = random.sample(AMENITIES, k=random.randint(2, min(5, len(AMENITIES))))

    return {
        'property_id': f"PRP-{fake.unique.bothify(text='????-####').upper()}",
        'address': {
            'street': fake.street_address(),
            'city': city,
            'state': city_data['state'],
            'zip_code': f"{city_data['zip_prefix']}{fake.numerify(text='###')}",
            'country': 'USA'
        },
        'property_details': {
            'type': property_type,
            'transaction_type': transaction_type,
            'year_built': year_built,
            'construction_type': random.choice(CONSTRUCTION_TYPES),
            'floors': random.randint(1, 3) if property_type != 'Land' else 0,
            'lot_size': round(sqft * random.uniform(1.2, 5.0)) if property_type == 'Land' else None,
            'living_area': sqft if property_type != 'Land' else None,
            'bedrooms': bedrooms,
            'bathrooms': bathrooms,
            'amenities': selected_amenities,
            'has_garage': random.choice([True, False]),
            'parking_spaces': random.randint(0, 3),
            'neighborhood_class': neighborhood_class
        },
        'financials': {
            'price': price,
            'price_per_sqft': round(price / sqft, 2) if sqft > 0 else None,
            'tax_assessment': round(price * random.uniform(0.7, 0.9), 2),
            'hoa_fee': round(random.uniform(100, 500), 2) if random.random() > 0.3 else None,
            'last_sale_price': round(price * random.uniform(0.5, 1.2), 2),
            'last_sale_date': fake.date_between(start_date='-10y', end_date='-1y').strftime('%Y-%m-%d')
        },
        'listing_details': {
            'listing_date': listing_date,
            'days_on_market': (datetime.now() - datetime.strptime(listing_date, '%Y-%m-%d')).days,
            'listing_status': random.choice(['Active', 'Pending', 'Sold', 'Withdrawn']),
            'description': generate_property_description(property_type, bedrooms, bathrooms, selected_amenities)
        },
        'agent': random.choice(AGENTS),
        'location': {
            'latitude': float(fake.latitude()),
            'longitude': float(fake.longitude()),
            'school_district': f"District {random.randint(1, 20)}",
            'walk_score': random.randint(0, 100),
            'transit_score': random.randint(0, 100)
        },
        'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    }


def produce_records(thread_id):
    """Thread function to produce records to Kafka"""
    print(f"Thread {thread_id} started producing to {KAFKA_BROKERS}")

    try:
        while True:
            start_time = time.time()
            records_sent = 0

            for _ in range(BATCH_SIZE):
                record = generate_real_estate_record()
                producer.send(
                    TOPIC_NAME,
                    value=record,
                    key=str(thread_id).encode('utf-8')  # Key for consistent partitioning
                )
                records_sent += 1

            producer.flush()
            elapsed = time.time() - start_time
            print(
                f"Thread {thread_id} sent {records_sent} records in {elapsed:.2f}s ({records_sent / elapsed:.1f} records/sec)")

    except Exception as e:
        print(f"Thread {thread_id} error: {str(e)}")


def main():
    """Main function to start the producer"""
    create_topic_if_not_exists()

    print(f"Starting {THREADS} producer threads to {KAFKA_BROKERS}")
    print(f"Topic: {TOPIC_NAME} (Partitions: {TOPIC_PARTITIONS}, Replication: {TOPIC_REPLICATION})")

    threads = []
    for i in range(THREADS):
        t = threading.Thread(target=produce_records, args=(i,))
        t.daemon = True
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping producers...")
        producer.close()
        for t in threads:
            t.join()


if __name__ == "__main__":
    main()