import json, os, subprocess, asyncio, aiohttp
from env import DATASET
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON
from typing import Optional, Any
from tqdm.asyncio import tqdm_asyncio

LPVTable = dict[str, dict[str, set[str]]]
""" `{label.type: {property: [value.type]}}` """


PREFIX = "dbpedia"


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


class OnlineSchemaExtractor:
    def __init__(
        self,
        test_mode: bool = False,
        text_parse_mode: bool = True,
        record_limit: Optional[int] = None,
    ) -> None:
        """
        `load into graph then query` mode has been `deprecated` for it's TOO SLOW!

        `text parse` mode is in use instead!
        """

        self.test_mode, self.text_parse_mode = test_mode, text_parse_mode
        self.sparql = SPARQLWrapper("http://dbpedia.org/sparql")
        self.labels, self.fmt, self.out = f"{DATASET}/labels_lang=en.ttl", "ttl", "out"
        self.schema = LPVTable()
        self.record_limit = record_limit

        print("Building graphs from `labels` ... ", end="")
        self.g = (
            Graph().parse(source=self.labels, format=self.fmt)
            if (not test_mode) and (not text_parse_mode)
            else Graph()
        )
        print("Done!\n" if not text_parse_mode else "Skipped!\n")

    def test(self):
        subject = "http://dbpedia.org/resource/!!!"
        print(f"Querying `{subject}` ...")
        print("=" * 60)
        self.query_subject(subject)
        print("=" * 60)

    def query_subject(self, subject):
        """
        `if` : `<subject>-[property]-<value>`

        `do` : `<subject.type>-[property]-<value.type>`
        """

        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?subject_type ?predicate ?object ?object_type
        WHERE {{
            <{subject}> rdf:type ?subject_type .
            <{subject}> ?predicate ?object .
            ?object rdf:type ?object_type .
        }}
        """

        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()

        if self.test_mode:
            print(json.dumps(results, indent=2))
            return

        for result in results["results"]["bindings"]:  # type: ignore
            subject_type, predicate, object_type = (
                str(result["subject_type"]["value"]),  # type: ignore
                str(result["predicate"]["value"]),  # type: ignore
                str(result["object_type"]["value"]),  # type: ignore
            )
            if subject_type not in self.schema:
                self.schema[subject_type] = {}
            if predicate not in self.schema[subject_type]:
                self.schema[subject_type][predicate] = set[str]()
            self.schema[subject_type][predicate].add(object_type)

    async def async_query_subject(self, client, subject):
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?subject_type ?predicate ?object ?object_type
        WHERE {{
            <{subject}> rdf:type ?subject_type .
            <{subject}> ?predicate ?object .
            ?object rdf:type ?object_type .
        }}
        """

        async with client.get(
            "http://dbpedia.org/sparql", params={"query": query, "format": "json"}
        ) as response:
            results = await response.json()

        if self.test_mode:
            print(json.dumps(results, indent=2))
            return

        for result in results["results"]["bindings"]:  # type: ignore
            subject_type, predicate, object_type = (
                str(result["subject_type"]["value"]),  # type: ignore
                str(result["predicate"]["value"]),  # type: ignore
                str(result["object_type"]["value"]),  # type: ignore
            )
            if subject_type not in self.schema:
                self.schema[subject_type] = {}
            if predicate not in self.schema[subject_type]:
                self.schema[subject_type][predicate] = set[str]()
            self.schema[subject_type][predicate].add(object_type)

    async def query_subjects(self, subjects, notify_detailed_err: bool = False):
        """
        Query a list of subjects in parallel using coroutines.
        """
        try:
            async with aiohttp.ClientSession() as session:
                tasks = [self.async_query_subject(session, s) for s in subjects]
                with tqdm_asyncio(total=len(tasks)) as p_bar:
                    for f in asyncio.as_completed(tasks):
                        p_bar.update(1)
                        try:
                            await f
                        except Exception as e:
                            if notify_detailed_err:
                                print("Error on `task` occurred ...")
                                print("So, now we move forward to `next` task ...")
                            continue
        except Exception as e:
            print("Error on `ClientSession` occurred ...")
            print("So, we have no choice but to stop gathering data ...")
            return

    async def query_subjects_grouped(self, subjects, notify_detailed_err: bool = False):
        """
        Query a list of subjects in parallel using coroutines.
        Subjects have been separated into slices, each slice contains at most 1000 subjects.
        """
        SLICE_SIZE = 1000
        num_of_slices = len(subjects) // SLICE_SIZE + int(
            len(subjects) % SLICE_SIZE != 0
        )
        for i in range(num_of_slices):
            if_skip = False
            start = i * SLICE_SIZE
            end = (i + 1) * SLICE_SIZE
            slice = subjects[start:end]
            print(f"Gathering on slice[`{i+1}/{num_of_slices}`] ...")
            try:
                async with aiohttp.ClientSession() as session:
                    tasks = [self.async_query_subject(session, s) for s in slice]
                    with tqdm_asyncio(total=len(tasks)) as p_bar:
                        for f in asyncio.as_completed(tasks):
                            try:
                                await f
                                p_bar.update(1)
                            except Exception as e:
                                if notify_detailed_err:
                                    print("Error on `task` occurred ...")
                                    print("So, now we move forward to `next` task ...")
                                continue
            except Exception as e:
                print("Error on `slice` occurred ...")
                print("So, now we move forward to `next` slice ...")
                continue

    def toJSON(self):
        json_data = json.dumps(self.schema, cls=LPVTableEncoder, indent=2)
        if not os.path.exists(self.out):
            os.makedirs(self.out)
        with open(
            f"{self.out}/{PREFIX}_full_schema.json"
            if not self.record_limit
            else f"{self.out}/{PREFIX}_full_schema(record_scale_{self.record_limit:.2e}).json",
            "w",
        ) as f:
            f.write(json_data)

    def exec(self):
        if self.test_mode:
            self.test()
            return

        if not self.text_parse_mode:
            for s in self.g.subjects(unique=True):
                print(f"Querying `{s}` ... ", end="")
                self.query_subject(s)
                print("Done!")
        else:
            """
            1. directly read `labels`, line by line
            2. extract `<subject>` from each line, and remove `<` and `>`
            3. query `subject`
            4. record `subject` into a set, so we could skip [[3.]] if we have already queried `subject`
            """
            queried = set[str]()
            with open(self.labels, "r") as f:
                for line in f.readlines():
                    if self.record_limit and len(queried) >= self.record_limit:
                        break
                    s, p, o = (
                        line.split()[0][1:-1],
                        line.split()[1][1:-1],
                        line.split()[2][1:-1],
                    )
                    if s not in queried:
                        print(f"Querying `{s}` ... ", end="")
                        self.query_subject(s)
                        print("Done!")
                        queried.add(s)

        print("Saving to JSON ... ", end="")
        self.toJSON()
        print("Done!\n")
        print("Finished!")

    def concurrent_exec(self):
        if self.test_mode:
            self.test()
            return

        if not self.text_parse_mode:
            asyncio.get_event_loop().run_until_complete(
                self.query_subjects(self.g.subjects(unique=True))
            )
        else:
            queried = set[str]()
            num_of_lines = int(
                subprocess.check_output(f"wc -l {self.labels}", shell=True).split()[0]
            )
            print(
                f"Total number of lines (approximately equals to labels): {num_of_lines}"
            )
            print(
                f"Prepare to gather schema from `{self.record_limit if self.record_limit else num_of_lines}` labels ..."
            )
            subjects = []
            with open(self.labels, "r") as f:
                f.seek(0)
                for line in f.readlines():
                    if self.record_limit and len(queried) >= self.record_limit:
                        break
                    s, p, o = (
                        line.split()[0][1:-1],
                        line.split()[1][1:-1],
                        line.split()[2][1:-1],
                    )
                    if s not in queried:
                        subjects.append(s)
                        queried.add(s)
            print(
                f"Start gathering schema from `{len(subjects)}` labels ...",
            )
            asyncio.get_event_loop().run_until_complete(self.query_subjects(subjects))
            print("Gathering operation is done!")

        print("Saving to JSON ... ", end="")
        self.toJSON()
        print("Done!\n")

        print("Finished!")


if __name__ == "__main__":
    s = OnlineSchemaExtractor(record_limit=int(10))
    s.concurrent_exec()
