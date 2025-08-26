from kafka import KafkaProducer
from faker import Faker
import json
import time

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092',
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

def generate_users():
    return{
        'name':fake.name(),
        'email':fake.email(),
        'address':fake.address(),
        'phone_no':fake.phone_number()
    }
while True:
    user = generate_users()
    producer.send('fakes',user)
    print(f"sent:{user}")
    
    time.sleep(5)