from tqdm.auto import tqdm

OUT_PATH = "out"
RELATIONSHIP = f"{OUT_PATH}/type_type_relationships"
NODES = f"{OUT_PATH}/type_nodes"

NAMESPACE = "Type"
HEADERS = [f":START_ID({NAMESPACE})", f":END_ID({NAMESPACE})", "StartCount", "EndCount"]
