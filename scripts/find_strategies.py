"""LAYER 2: FIND strategies (Strategy pattern).

`FindStrategy` is the interface; each implementation decides which rows of a
part file are "found". The driver (find.py) and the Spark task
(tasks.scan_task) are the same for every strategy, so every strategy gets the
waves, resume, stop switch, tracker and reports for free, and every strategy
produces the same files.jsonl for Forget (Layer 3).

  HASH_MATCH      parse the XML in RAW_RESPONSE, hash the configured fields and
                  compare with the input hashes (the original behaviour)
  TRAN_ID_SUFFIX  no input: pick every row whose TRAN_ID ends with a suffix
                  (default "_c"); the XML is not read

Adding a strategy: subclass FindStrategy, add it to FIND_STRATEGIES, add the
name to the FIND_STRATEGY choice in jenkins/Jenkinsfile.find, add a test.

Executors only receive the strategy NAME and its plain-dict `shared` state, so
nothing unpicklable is broadcast. `match_row` is a staticmethod for that reason.

Shipped to both Glue jobs with --extra-py-files (see main.tf).
"""
import json
import re
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Optional, Set, Tuple

import common
import formats

DEFAULT_TAGS = {
    "PHONE": "phone,phoneNumber",
    "EMAIL": "email,emailAddress",
    "NAME": "name,fullName",
    "ADDRESS": "address,fullAddress",
}
DEFAULT_SUFFIX = "_c"


class FindStrategy(ABC):
    """The find interface. One instance per driver run; stateless."""

    name: str = ""
    needs_input: bool = False  # True = the batch needs --input_uri

    @abstractmethod
    def new_settings(self, config) -> dict:
        """Settings fixed for the whole batch (stored in batch.json). Rejects arguments that don't apply."""

    @abstractmethod
    def changed_settings(self, config, settings) -> List[str]:
        """Names of explicitly given arguments that differ from the stored settings (continued batch)."""

    def parse_input(self, document) -> list:
        """The input items, from the input JSON document. Only called when needs_input."""
        return []

    def fingerprint(self, settings, input_etag) -> str:
        """Identifies 'same strategy + same input'. Used as the processed-file tracker scope."""
        material = json.dumps({"settings": settings, "input_etag": input_etag}, sort_keys=True)
        return common.sha256_hex(material)

    @abstractmethod
    def shared(self, inputs, settings) -> Optional[dict]:
        """Plain dict broadcast to every Spark task. None = nothing can match, so scan nothing."""

    @staticmethod
    @abstractmethod
    def match_row(shared, row) -> Iterable[str]:
        """Match keys for one CSV row (empty = the row is not found)."""

    @abstractmethod
    def labels(self, inputs, key) -> List[str]:
        """run_ids recorded in files.jsonl for a match key. Raises on an unknown key."""

    @abstractmethod
    def build_results(self, inputs, settings, per_key, failed_files) -> Tuple[object, dict]:
        """(results.json body, {"total", "match", "no_match", "error"}).

        per_key: {match key: {s3 uri: [TRAN_ID, ...]}}.
        """


def get(name) -> FindStrategy:
    try:
        return FIND_STRATEGIES[name]()
    except KeyError:
        raise ValueError(f"Unknown find strategy {name!r}; expected one of {', '.join(FIND_STRATEGIES)}") from None


# =============================================================================
# HASH_MATCH
# =============================================================================
def build_lookup(requests) -> Dict[str, Dict[str, List[int]]]:
    """match_type -> hash -> indexes of the valid input items with that hash."""
    lookup: Dict[str, Dict[str, List[int]]] = {}
    for request in requests:
        if not request.error:
            lookup.setdefault(request.match_type, {}).setdefault(request.hash_key, []).append(request.index)
    return lookup


def build_tag_map(tag_settings: Dict[str, str], match_types) -> Dict[str, Set[str]]:
    """XML local tag name (lowercase) -> match types it can hold."""
    tag_map: Dict[str, Set[str]] = {}
    for match_type in match_types:
        tags = {t.strip().lower() for t in tag_settings[match_type].split(",") if t.strip()}
        if not tags:
            raise ValueError(f"Configure at least one XML tag for {match_type}")
        for tag in tags:
            tag_map.setdefault(tag, set()).add(match_type)
    return tag_map


def find_hits(xml, tag_map, lookup, hash_mode) -> Set[int]:
    """Return the indexes of input items whose hash matches a value in this XML."""
    if not xml.strip():
        return set()
    if re.search(r"<!DOCTYPE", xml, re.IGNORECASE):
        raise ValueError("XML document types are unsupported")
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        raise ValueError("Invalid XML in RAW_RESPONSE") from None
    hits: Set[int] = set()
    for element in root.iter():
        if not isinstance(element.tag, str):
            continue
        match_types = tag_map.get(element.tag.rsplit("}", 1)[-1].lower())
        if not match_types:
            continue
        text = "".join(element.itertext())
        for match_type in match_types:
            hashes = lookup.get(match_type)
            value = common.normalize(text, match_type, hash_mode) if hashes else ""
            if value:
                hits.update(hashes.get(common.sha256_hex(value), ()))
    return hits


class HashMatchStrategy(FindStrategy):
    name = "HASH_MATCH"
    needs_input = True

    def new_settings(self, config):
        if config.tran_id_suffix:
            raise ValueError("--tran_id_suffix applies only to TRAN_ID_SUFFIX")
        tags = dict(config.tags)
        build_tag_map(tags, formats.MATCH_TYPES)  # fail fast on an empty tag list
        return {"hash_mode": config.hash_mode or "normalized", "tags": tags}

    def changed_settings(self, config, settings):
        changed = []
        if config.hash_mode and config.hash_mode != settings.get("hash_mode"):
            changed.append("hash_mode")
        if config.tran_id_suffix:
            changed.append("tran_id_suffix")
        return changed

    def parse_input(self, document):
        return formats.parse_input(document)

    def shared(self, requests, settings):
        lookup = build_lookup(requests)
        if not lookup:
            return None
        tags = settings.get("tags") or DEFAULT_TAGS  # schema 3 batches stored no tags
        return {"lookup": lookup, "tag_map": build_tag_map(tags, lookup), "hash_mode": settings["hash_mode"]}

    @staticmethod
    def match_row(shared, row):
        return [str(index) for index in find_hits(row["RAW_RESPONSE"], shared["tag_map"], shared["lookup"],
                                                  shared["hash_mode"])]

    def labels(self, requests, key):
        try:
            request = requests[int(key)]
        except (ValueError, IndexError):
            request = None
        if request is None or request.error:
            raise ValueError("wave result refers to an unknown input item")
        return [request.run_id]

    def build_results(self, requests, settings, per_key, failed_files):
        results = []
        for request in requests:
            data_identifier = per_key.get(str(request.index), {})
            match_result, error = self._decide(request, data_identifier, failed_files)
            results.append(formats.result_entry(request, match_result, data_identifier, error))
        return results, _count([r["match_result"] for r in results])

    @staticmethod
    def _decide(request, data_identifier, failed_files):
        if request.error:
            return formats.ERROR, request.error
        if data_identifier:
            return formats.MATCH, None
        if failed_files:
            return formats.ERROR, f"{len(failed_files)} file(s) could not be scanned, so NO_MATCH cannot be confirmed"
        return formats.NO_MATCH, None


# =============================================================================
# TRAN_ID_SUFFIX
# =============================================================================
SUFFIX_KEY = "SUFFIX"


class TranIdSuffixStrategy(FindStrategy):
    name = "TRAN_ID_SUFFIX"
    needs_input = False

    def new_settings(self, config):
        if config.hash_mode:
            raise ValueError("--hash_mode applies only to HASH_MATCH")
        if config.input_uri:
            raise ValueError("TRAN_ID_SUFFIX does not read an input file; leave --input_uri empty")
        return {"tran_id_suffix": config.tran_id_suffix or DEFAULT_SUFFIX}

    def changed_settings(self, config, settings):
        changed = []
        if config.tran_id_suffix and config.tran_id_suffix != settings.get("tran_id_suffix"):
            changed.append("tran_id_suffix")
        if config.hash_mode:
            changed.append("hash_mode")
        return changed

    def shared(self, inputs, settings):
        return {"suffix": settings["tran_id_suffix"]}

    @staticmethod
    def match_row(shared, row):
        return [SUFFIX_KEY] if row["TRAN_ID"].endswith(shared["suffix"]) else []

    def labels(self, inputs, key):
        if key != SUFFIX_KEY:
            raise ValueError("wave result has an unknown match key")
        return []

    def build_results(self, inputs, settings, per_key, failed_files):
        data_identifier = per_key.get(SUFFIX_KEY, {})
        if data_identifier:
            match_result, error = formats.MATCH, None
            if failed_files:
                error = f"{len(failed_files)} file(s) could not be scanned; the list may be incomplete"
        elif failed_files:
            match_result = formats.ERROR
            error = f"{len(failed_files)} file(s) could not be scanned, so NO_MATCH cannot be confirmed"
        else:
            match_result, error = formats.NO_MATCH, None
        document = formats.suffix_result(settings["tran_id_suffix"], match_result, data_identifier, error)
        return document, _count([match_result])


def _count(match_results):
    return {
        "total": len(match_results),
        "match": sum(r == formats.MATCH for r in match_results),
        "no_match": sum(r == formats.NO_MATCH for r in match_results),
        "error": sum(r == formats.ERROR for r in match_results),
    }


FIND_STRATEGIES = {cls.name: cls for cls in (HashMatchStrategy, TranIdSuffixStrategy)}
DEFAULT = HashMatchStrategy.name
