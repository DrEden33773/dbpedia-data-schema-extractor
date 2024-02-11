import json, os, subprocess
from env import DATASET
from typing import Any
from tqdm.asyncio import tqdm_asyncio
from glob import glob

LPVTable = dict[str, dict[str, set[str]]]
""" `{label.type: {property: {value.type}}}` """
TypeDict = dict[str, set[str]]
""" `{label: {type}}` """


class LPVTableEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, dict):
                    for k, v in value.items():
                        if isinstance(v, set):
                            value[k] = list(v)
            return obj
        elif isinstance(obj, set):
            return list(obj)
        return super().default(obj)


class LPVTableDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, dict):
                    for k, v in value.items():
                        if isinstance(v, list):
                            value[k] = set(v)
            return obj
        elif isinstance(obj, list):
            return set(obj)
        return obj


class TypeDictEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, set):
                    obj[key] = list(value)
            return obj
        elif isinstance(obj, set):
            return list(obj)
        return super().default(obj)


class TypeDictDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, list):
                    obj[key] = set(value)
            return obj
        elif isinstance(obj, list):
            return set(obj)
        return obj


OUTPUT_PREFIX, OUTPUT_ATTRIBUTE = "dbpedia", "local"
OUT_PATH, DUMP_PATH = f"out", f"dump"

LINK_FILES = glob(f"{DATASET}/link*")
SPO_MAPPING_FILES = glob(f"{DATASET}/mappingbased-objects*")
USE_SPO_MAPPING_FILES = True

SPECIFIC_TYPE_FILE = f"{DATASET}/instance-types_inference=specific_lang=en.ttl"
TRANSITIVE_TYPE_FILE = f"{DATASET}/instance-types_inference=transitive_lang=en.ttl"


def pre_check():
    paths = ["out", "dump"]
    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)


class LocalSchemaExtractor:
    def update_additional_type_files(self):
        if USE_SPO_MAPPING_FILES:
            print(
                "Detected using `spo_mapping_file` instead of `link_files` as resource pool, skipping `update_additional_type_files` ..."
            )
            """
            That is because all `type_info` needed for `mapping_based_objects` are already included in `SPECIFIC_TYPE_FILE` and `TRANSITIVE_TYPE_FILE`.
            """
            return self

        DUMP_FILE = f"{DUMP_PATH}/additional_type_files.txt"

        if os.path.exists(DUMP_FILE):
            print(
                f"Loading additional_type_files from `{DUMP_FILE}`(dumped) ... ", end=""
            )
            with open(DUMP_FILE, "r") as f:
                for line in f:
                    self.type_files.add(line.strip())
            print("Done!")
            return self

        for file in LINK_FILES:
            num_of_lines = int(subprocess.check_output(["wc", "-l", file]).split()[0])
            with open(file, "r") as f:
                f.seek(0)
                with tqdm_asyncio(
                    total=num_of_lines, desc=f"Parsing `{file}`'s pred for `type`"
                ) as bar:
                    for line in f:
                        p = line.split()[1][1:-1]  # remove `<` and `>`
                        if p.split("#")[-1] == "type":
                            self.type_files.add(file)
                            break
                        bar.update(1)

        print(f"Serializing additional_type_files to txt ... ", end="")
        with open(DUMP_FILE, "w") as f:
            for file in self.type_files:
                f.write(f"{file}\n")
        print("Done!")

    def build_type_dict(self):
        DUMP_FILE = f"{DUMP_PATH}/type_dict.json"

        if os.path.exists(DUMP_FILE):
            print(f"Loading type_dict from `{DUMP_FILE}`(dumped) ... ", end="")
            with open(DUMP_FILE, "r") as f:
                self.type_dict = json.load(f, cls=TypeDictDecoder)
            print("Done!")
            return self

        for cnt, type_file in enumerate(self.type_files):
            num_of_lines = int(
                subprocess.check_output(["wc", "-l", type_file]).split()[0]
            )
            with tqdm_asyncio(
                total=num_of_lines,
                desc=f"Building type_dict from `{type_file}` ({cnt + 1}/{len(self.type_files)})",
            ) as bar:
                with open(type_file, "r") as f:
                    f.seek(0)
                    for line in f:
                        s, p, o = (
                            line.split()[0][1:-1],
                            line.split()[1][1:-1].split("#")[-1],
                            line.split()[2][1:-1],
                        )  # remove `<` and `>`
                        if p == "type":
                            if s not in self.type_dict:
                                self.type_dict[s] = set[str]()
                            self.type_dict[s].add(o)
                        bar.update(1)

        print(f"Serializing type_dict to json ... ", end="")
        json_data = json.dumps(self.type_dict, cls=TypeDictEncoder, indent=2)
        with open(DUMP_FILE, "w") as f:
            f.write(json_data)
        print("Done!")

        return self

    def generate_schema_edge(self):
        OUTPUT_FILE = f"{OUT_PATH}/{OUTPUT_PREFIX}_{OUTPUT_ATTRIBUTE}_schema_edges.txt"

        if os.path.exists(OUTPUT_FILE):
            num_of_lines = int(
                subprocess.check_output(["wc", "-l", OUTPUT_FILE]).split()[0]
            )
            with tqdm_asyncio(
                total=num_of_lines,
                desc=f"Loading schema_edge from `{OUTPUT_FILE}`(generated)",
            ) as bar:
                with open(OUTPUT_FILE, "r") as f:
                    f.seek(0)
                    for line in f:
                        s_type, p, o_type = (
                            line.split()[0],
                            line.split()[1],
                            line.split()[2],
                        )
                        if s_type not in self.schema_edge:
                            self.schema_edge[s_type] = {}
                            self.appeared_subject_types.add(s_type)
                        if p not in self.schema_edge[s_type]:
                            self.schema_edge[s_type][p] = set[str]()
                        if o_type not in self.schema_edge[s_type][p]:
                            self.schema_edge[s_type][p].add(o_type)
                            self.appeared_object_types.add(o_type)
                            self.num_of_schema_edges += 1
                    bar.update(1)
            return self

        RESOURCE_POOL_FILES = SPO_MAPPING_FILES if USE_SPO_MAPPING_FILES else LINK_FILES

        for cnt, file in enumerate(RESOURCE_POOL_FILES):
            num_of_lines = int(subprocess.check_output(["wc", "-l", file]).split()[0])
            with tqdm_asyncio(
                total=num_of_lines,
                desc=f"Generating schema_edge from `{file}` ({cnt + 1}/{len(RESOURCE_POOL_FILES)})",
            ) as bar:
                with open(file, "r") as f:
                    f.seek(0)
                    for line in f:
                        s, p, o = (
                            line.split()[0][1:-1],
                            line.split()[1][1:-1],
                            line.split()[2][1:-1],
                        )  # remove `<` and `>`
                        if (s not in self.type_dict) or (o not in self.type_dict):
                            bar.update(1)
                            continue
                        s_types, o_types = self.type_dict[s], self.type_dict[o]
                        for s_type in s_types:
                            for o_type in o_types:
                                if s_type not in self.schema_edge:
                                    self.schema_edge[s_type] = {}
                                    self.appeared_subject_types.add(s_type)
                                if p not in self.schema_edge[s_type]:
                                    self.schema_edge[s_type][p] = set[str]()
                                if o_type not in self.schema_edge[s_type][p]:
                                    self.schema_edge[s_type][p].add(o_type)
                                    self.appeared_object_types.add(o_type)
                                    self.num_of_schema_edges += 1
                        bar.update(1)

        with tqdm_asyncio(
            total=self.num_of_schema_edges,
            desc=f"Exporting schema_edge to `{OUTPUT_FILE}`",
        ) as bar:
            with open(OUTPUT_FILE, "w") as f:
                for s_type, p_dict in self.schema_edge.items():
                    for p, o_types in p_dict.items():
                        for o_type in o_types:
                            f.write(f"{s_type} {p} {o_type}\n")
                            bar.update(1)

        return self

    def generate_schema_vertex(self):
        OUTPUT_FILE = (
            f"{OUT_PATH}/{OUTPUT_PREFIX}_{OUTPUT_ATTRIBUTE}_schema_vertices.txt"
        )

        if os.path.exists(OUTPUT_FILE):
            print(f"`schema_vertex` has been generated, see {OUTPUT_FILE} ...")
            return self

        with tqdm_asyncio(
            total=len(self.appeared_subject_types) + len(self.appeared_object_types),
            desc=f"Generating schema_vertex",
        ) as bar:
            for s_type in self.appeared_subject_types:
                self.schema_vertex.add(s_type)
                bar.update(1)
            for o_type in self.appeared_object_types:
                self.schema_vertex.add(o_type)
                bar.update(1)

        with tqdm_asyncio(
            total=len(self.schema_vertex),
            desc=f"Exporting schema_vertex to `{OUTPUT_FILE}`",
        ) as bar:
            with open(OUTPUT_FILE, "w") as f:
                for v in self.schema_vertex:
                    f.write(f"{v}\n")
                    bar.update(1)

        return self

    def notify_done(self):
        print("Successfully get `schema_edge` and `schema_vertex` ...")
        print(
            f"See `schema_edge` at `{OUT_PATH}/{OUTPUT_PREFIX}_{OUTPUT_ATTRIBUTE}_schema_edge.txt`"
        )
        print(
            f"See `schema_vertex` at `{OUT_PATH}/{OUTPUT_PREFIX}_{OUTPUT_ATTRIBUTE}_schema_vertex.txt`"
        )

    def __init__(self) -> None:
        pre_check()
        self.type_dict = TypeDict()
        self.schema_edge = LPVTable()
        self.schema_vertex = set[str]()
        self.appeared_subject_types = set[str]()
        self.appeared_object_types = set[str]()
        self.type_files: set[str] = {SPECIFIC_TYPE_FILE, TRANSITIVE_TYPE_FILE}
        self.num_of_schema_edges = 0

    def exec(self):
        self.update_additional_type_files()
        self.build_type_dict()
        self.generate_schema_edge()
        self.generate_schema_vertex()
        self.notify_done()


if __name__ == "__main__":
    LocalSchemaExtractor().exec()
