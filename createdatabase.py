import random
from datetime import datetime, timedelta
from pymongo import MongoClient

# Conexión a MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['tpch_database']

# Generador de datos aleatorios
def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

# Crear colecciones
orders_collection = db['orders']
part_supplier_collection = db['part_supplier']
supplier_collection = db['suppliers']

# Fechas base para generar datos
start_date = datetime(2020, 1, 1)
end_date = datetime(2024, 1, 1)

# Insertar documentos en la colección `orders`
orders = []
for i in range(1, 6):  # Generar 5 órdenes
    order = {
        "orderkey": f"{i}",
        "customer": {
            "custkey": f"{random.randint(1000, 9999)}",
            "name": f"Customer {i}",
            "address": f"Address {i}",
            "phone": f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            "acctbal": round(random.uniform(100, 1000), 2),
            "mktsegment": random.choice(["Automotive", "Electronics", "Retail"]),
            "comment": f"Comment {i}"
        },
        "status": random.choice(["F", "O"]),
        "totalprice": round(random.uniform(500, 5000), 2),
        "orderdate": random_date(start_date, end_date).strftime('%Y-%m-%d'),
        "priority": random.choice(["High", "Medium", "Low"]),
        "clerk": f"Clerk#{random.randint(100, 999)}",
        "shippriority": random.randint(1, 3),
        "comment": f"Order comment {i}",
        "lineitems": [
            {
                "linenumber": f"{j}",
                "partkey": f"{random.randint(1000, 9999)}",
                "supplierkey": f"{random.randint(100, 999)}",
                "quantity": random.randint(1, 100),
                "extendedprice": round(random.uniform(100, 500), 2),
                "discount": round(random.uniform(0.01, 0.1), 2),
                "tax": round(random.uniform(0.05, 0.2), 2),
                "returnflag": random.choice(["N", "R"]),
                "linestatus": random.choice(["O", "C"]),
                "shipdate": random_date(start_date, end_date).strftime('%Y-%m-%d'),
                "commitdate": random_date(start_date, end_date).strftime('%Y-%m-%d'),
                "receiptdate": random_date(start_date, end_date).strftime('%Y-%m-%d'),
                "shipinstruct": "DELIVER IN PERSON",
                "shipmode": random.choice(["AIR", "TRUCK"]),
                "comment": f"Lineitem comment {j}"
            }
            for j in range(1, random.randint(4, 10))
        ]
    }
    orders.append(order)

orders_collection.insert_many(orders)

# Insertar documentos en la colección `part_supplier`
part_suppliers = []
for i in range(1, 6):  # Generar 5 relaciones de partes y proveedores
    part_supplier = {
        "part": {
            "partkey": f"{random.randint(1000, 9999)}",
            "name": f"Part {i}",
            "mfgr": f"Manufacturer {i}",
            "brand": f"Brand {random.randint(1, 5)}",
            "type": random.choice(["Type A", "Type B", "Type C"]),
            "size": random.randint(1, 100),
            "container": random.choice(["Box", "Bag"]),
            "retailprice": round(random.uniform(10, 500), 2),
            "comment": f"Part comment {i}"
        },
        "supplier": {
            "suppkey": f"{random.randint(100, 999)}",
            "name": f"Supplier {i}",
            "address": f"Address {i}",
            "phone": f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            "acctbal": round(random.uniform(500, 5000), 2),
            "nation": {
                "name": f"Nation {i}",
                "region": {
                    "name": f"Region {random.randint(1, 5)}",
                    "comment": f"Region comment {i}"
                },
                "comment": f"Nation comment {i}"
            },
            "comment": f"Supplier comment {i}"
        },
        "availqty": random.randint(1, 500),
        "supplycost": round(random.uniform(5, 50), 2),
        "comment": f"Part supplier comment {i}"
    }
    part_suppliers.append(part_supplier)

part_supplier_collection.insert_many(part_suppliers)

# Insertar documentos en la colección `suppliers`
suppliers = []
for i in range(1, 6):  # Generar 5 proveedores
    supplier = {
        "suppkey": f"{i}",
        "name": f"Supplier {i}",
        "address": f"Address {i}",
        "phone": f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
        "acctbal": round(random.uniform(1000, 10000), 2),
        "nation": {
            "name": f"Nation {i}",
            "region": {
                "name": f"Region {random.randint(1, 5)}",
                "comment": f"Region comment {i}"
            },
            "comment": f"Nation comment {i}"
        },
        "comment": f"Supplier comment {i}"
    }
    suppliers.append(supplier)

supplier_collection.insert_many(suppliers)

print("Datos insertados correctamente.")