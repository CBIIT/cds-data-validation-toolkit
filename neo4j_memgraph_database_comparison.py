import json
from neo4j import GraphDatabase
import yaml

# --- CONFIGURATION ---
DB1_URI = "bolt://url1"
DB1_USER = "neo4j"
DB1_PASS = "password"

DB2_URI = "bolt://url2"
DB2_USER = "memgraph"
DB2_PASS = "password"

OUTPUT_FILE = "node_differences.json"
model = {}
schema_file = ["model-desc/model.yml", "model-desc/model-props.yml"]
for schema in schema_file:
    with open(schema) as f:
        temp_model = yaml.load(f, Loader=yaml.FullLoader)
        if temp_model:
            model.update(temp_model)

def get_id_property(node_name, model):
    #new_node_name = node_name[0]
    if isinstance(node_name, list):
        node_name = node_name[0]
    for prop in model["Nodes"][node_name]['Props']:
        property_defin = model["PropDefinitions"].get(prop)
        if property_defin is None:
            continue
        key = property_defin.get("Key")
        if key is None:
            continue
        elif key:
            return prop
    return None
        

def get_all_nodes(tx):
    query = "MATCH (n) RETURN labels(n) as labels, properties(n) as props"
    return list(tx.run(query))

def get_node_by_id(tx, node_name, id_property, node_id):
    query = f"MATCH (n: {node_name}) WHERE n.{id_property} = $id RETURN labels(n) as labels, properties(n) as props"
    result = tx.run(query, id = node_id)
    return result.single()

def compare_nodes(node1, node2):
    if node1 is None or node2 is None:
        return {"status": "missing", "node1": node1, "node2": node2}
    differences = {}
    # Compare labels
    if set(node1["labels"]) != set(node2["labels"]):
        differences["labels"] = {"db1": node1["labels"], "db2": node2["labels"]}
    # Compare properties
    props1 = node1["props"]
    props2 = node2["props"]
    all_keys = set(props1.keys()).union(props2.keys())
    prop_diff = {}
    for key in all_keys:
        if key not in ["created", "updated"]:
            v1 = props1.get(key)
            v2 = props2.get(key)
            ''''''
            if v1 == "":
                v1 = None
            if v2 == "":
                v2 = None
            if isinstance(v1, float) and isinstance(v2, str):
                v1 = str(v1)
            elif isinstance(v1, str) and isinstance(v2, float):
                v2 = str(v2)
            elif isinstance(v1, int) and isinstance(v2, str):
                v1 = str(v1)
            elif isinstance(v1, str) and isinstance(v2, int):
                v2 = str(v2)
            if v1 != v2:
                prop_diff[key] = {"db1": v1, "db2": v2}
    if prop_diff:
        differences["properties"] = prop_diff
    return differences if differences else None

# --- MAIN ---

def main():
    driver1 = GraphDatabase.driver(DB1_URI, auth=(DB1_USER, DB1_PASS))
    driver2 = GraphDatabase.driver(DB2_URI, auth=(DB2_USER, DB2_PASS))
    differences = []

    with driver1.session() as session1, driver2.session() as session2:
        print("starting comparison...")
        nodes1 = get_all_nodes(session1)
        for record in nodes1:
            node_name = record["labels"][0]
            id_property = get_id_property(node_name, model)
            node_id = record["props"].get(id_property)
            if id_property is None or node_id is None:
                print(id_property, node_id)
                continue
            node1 = {
                "id_property": id_property,
                "node_id": node_id,
                "labels": record["labels"],
                "props": record["props"]
            }
            id_property = get_id_property(node1["labels"], model)
            node2_record = get_node_by_id(session2, node_name, id_property, node_id)
            node2 = None
            if node2_record:
                node2 = {
                    "id_property": id_property,
                    "node_id": node_id,
                    "labels": node2_record["labels"],
                    "props": node2_record["props"]
                }
            diff = compare_nodes(node1, node2)
            if diff:
                print(f"differences found for node:{id_property, node_id, diff}") 
                differences.append({
                    "id_property": id_property,
                    "node_id": node_id,
                    "differences": diff
                })

    with open(OUTPUT_FILE, "w") as f:
        json.dump(differences, f, indent=4, sort_keys=True, default=str)

    print(f"Comparison complete. Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()