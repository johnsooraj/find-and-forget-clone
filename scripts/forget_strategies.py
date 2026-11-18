"""LAYER 3: FORGET strategies (Strategy pattern).

Every strategy rewrites each part file ONCE (tasks.rewrite_archive: one GET,
one PUT, same key, same CSV name). They differ only in what happens to the
OLD S3 version, which still holds the removed rows on a versioned bucket:

  REPLACE_KEEP_VERSIONS       leave it (it expires only if the bucket has a rule)
  REPLACE_PURGE_OLD_VERSIONS  delete every non-current version now (cannot be undone)
  REPLACE_TAG_FOR_LIFECYCLE   tag the old version drop-glue-superseded=true, so a
                              tag-filtered NoncurrentVersionExpiration lifecycle
                              rule deletes it after N days (see main.tf)

Forget reads only Layer 2's files.jsonl, so every forget strategy works with
every find strategy.

Adding a strategy: subclass ForgetStrategy, add it to FORGET_STRATEGIES, add
the name to the FORGET_STRATEGY choice in jenkins/Jenkinsfile.forget, add a test.

Shipped to both Glue jobs with --extra-py-files (see main.tf).
"""
from abc import ABC, abstractmethod

import common

SUPERSEDED_TAG = "drop-glue-superseded"


class ForgetStrategy(ABC):
    name: str = ""
    has_post_step: bool = True  # False = nothing to do after the rewrite, so finished parts are skipped

    @abstractmethod
    def after_replace(self, s3, bucket, key, old_version_id, new_version_id) -> dict:
        """Runs after the part was replaced (also on a resumed run). Returns extra outcome fields.

        Must be safe to run again: a run may stop after this step but before
        the completion marker records it.
        """


def get(name) -> ForgetStrategy:
    try:
        return FORGET_STRATEGIES[name]()
    except KeyError:
        raise ValueError(f"Unknown forget strategy {name!r}; expected one of {', '.join(FORGET_STRATEGIES)}") from None


class KeepVersions(ForgetStrategy):
    name = "REPLACE_KEEP_VERSIONS"
    has_post_step = False

    def after_replace(self, s3, bucket, key, old_version_id, new_version_id):
        return {}


def purge_old_versions(s3, bucket, key, keep_version_id):
    """Delete every noncurrent version of key. Return the number deleted."""
    if not keep_version_id:
        return 0  # bucket is not versioned: nothing older is kept
    versions, key_marker, version_marker = [], None, None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": key}
        if key_marker:
            kwargs.update(KeyMarker=key_marker, VersionIdMarker=version_marker)
        page = s3.list_object_versions(**kwargs)
        versions += [v for v in page.get("Versions", []) if v["Key"] == key]
        if not page.get("IsTruncated"):
            break
        key_marker, version_marker = page["NextKeyMarker"], page["NextVersionIdMarker"]
    latest = [v for v in versions if v["IsLatest"]]
    if len(latest) != 1 or latest[0]["VersionId"] != keep_version_id:
        raise ValueError("Object changed after replacement; old versions were not purged")
    deleted = 0
    for version in versions:
        if not version["IsLatest"]:
            s3.delete_object(Bucket=bucket, Key=key, VersionId=version["VersionId"])
            deleted += 1
    return deleted


class PurgeOldVersions(ForgetStrategy):
    name = "REPLACE_PURGE_OLD_VERSIONS"

    def after_replace(self, s3, bucket, key, old_version_id, new_version_id):
        return {"versions_deleted": purge_old_versions(s3, bucket, key, new_version_id)}


class TagForLifecycle(ForgetStrategy):
    name = "REPLACE_TAG_FOR_LIFECYCLE"

    def after_replace(self, s3, bucket, key, old_version_id, new_version_id):
        if not old_version_id or not new_version_id:
            # Unversioned bucket: the PUT already overwrote the old data.
            return {"lifecycle_tagged": False, "note": "Bucket is not versioned; no old version is kept"}
        common.put_merged_tags(s3, bucket, key, {SUPERSEDED_TAG: "true"}, version_id=old_version_id)
        return {"lifecycle_tagged": True}


FORGET_STRATEGIES = {cls.name: cls for cls in (KeepVersions, PurgeOldVersions, TagForLifecycle)}
DEFAULT = KeepVersions.name
