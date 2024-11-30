from pymongo import MongoClient
import json

# Conexión a MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['tpch_database']

"""
QUERY 1
"""
results1 = db.orders.aggregate([
    { "$unwind": "$lineitems" },
    { "$match": { "lineitems.shipdate": { "$lte": "2022-10-11" } } },
    { 
        "$group": {
            "_id": {
                "returnflag": "$lineitems.returnflag",
                "linestatus": "$lineitems.linestatus"
            },
            "sum_qty": { "$sum": "$lineitems.quantity" },
            "sum_base_price": { "$sum": "$lineitems.extendedprice" },
            "sum_disc_price": {
                "$sum": { 
                    "$multiply": [
                        "$lineitems.extendedprice",
                        { "$subtract": [1, "$lineitems.discount"] }
                    ]
                }
            },
            "sum_charge": {
                "$sum": {
                    "$multiply": [
                        "$lineitems.extendedprice",
                        { "$subtract": [1, "$lineitems.discount"] },
                        { "$add": [1, "$lineitems.tax"] }
                    ]
                }
            },
            "avg_qty": { "$avg": "$lineitems.quantity" },
            "avg_price": { "$avg": "$lineitems.extendedprice" },
            "avg_disc": { "$avg": "$lineitems.discount" },
            "count_order": { "$sum": 1 }
        }
    },
    { "$sort": { "_id.returnflag": 1, "_id.linestatus": 1 } }
])

SIZE = 64
TYPE = "Type C"
REGION = "Region 3"
"""
QUERY 2
"""
results2 = db.part_supplier.aggregate([
    { "$match": {
        "part.size": [SIZE],
        "part.type": [TYPE],
        "supplier.nation.region.name": [REGION]
    }},
    { "$group": {
        "_id": {
            "partkey": "$part.partkey",
            "suppkey": "$supplier.suppkey"
        },
        "min_supplycost": { "$min": "$supplycost" }  # Encontrar el mínimo costo de suministro
    }},
    { "$lookup": {
        "from": "part_supplier",
        "localField": "min_supplycost",
        "foreignField": "supplycost",
        "as": "matching_suppliers"
    }},
    { "$unwind": "$matching_suppliers" },
    { "$project": {
        "s_acctbal": "$matching_suppliers.supplier.acctbal",
        "s_name": "$matching_suppliers.supplier.name",
        "n_name": "$matching_suppliers.supplier.nation.name",
        "p_partkey": "$matching_suppliers.part.partkey",
        "p_mfgr": "$matching_suppliers.part.mfgr",
        "s_address": "$matching_suppliers.supplier.address",
        "s_phone": "$matching_suppliers.supplier.phone",
        "s_comment": "$matching_suppliers.supplier.comment"
    }},
    { "$sort": { "s_acctbal": -1, "n_name": 1, "s_name": 1, "p_partkey": 1 } }
])


output_file = 'query2_results.json'
with open(output_file, 'w') as f:
    json.dump(results2.to_list(), f, indent=4)