"""LAYER 1: parse and validate the job arguments.

Turns the Glue arguments (set by Terraform and the Jenkins drop-downs) into a
typed, frozen config for Layer 2 (find.py) or Layer 3 (forget.py). Nothing
here touches S3. Every check that can be done without AWS is done here, so a
bad parameter fails before any work starts.

Shipped to both Glue jobs with --extra-py-files (see main.tf).
"""
import argparse
import re
from dataclasses import dataclass, field
from typing import Dict, Optional

import common
import find_strategies
import forget_strategies
import notify
import tracking

SUFFIX = re.compile(r"[A-Za-z0-9_.-]{1,32}")
TABLE = re.compile(r"[A-Za-z0-9_.-]{3,255}")


@dataclass(frozen=True)
class CommonConfig:
    region: str
    source_prefix: str
    result_prefix: str
    stop_uri: str
    batch_id: str
    files_per_wave: int
    max_waves: int  # 0 = no limit
    tracker: str
    tracker_table: Optional[str]
    notifier: str
    notify_to: str

    @property
    def base(self):
        """Where this batch's reports live: <result_prefix><batch_id>/"""
        return f"{self.result_prefix}{self.batch_id}/"


@dataclass(frozen=True)
class FindConfig(CommonConfig):
    # None = not given. A new batch needs them; a continued batch must not change them.
    find_strategy: Optional[str] = None
    input_uri: Optional[str] = None
    archive_uri: Optional[str] = None
    month_from: Optional[str] = None
    month_to: Optional[str] = None
    hash_mode: Optional[str] = None
    tran_id_suffix: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    retry_failed: bool = False


@dataclass(frozen=True)
class ForgetConfig(CommonConfig):
    forget_strategy: str = forget_strategies.DEFAULT


# ----------------------------------------------------------------------------- argparse
def _common_parser():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    # Fixed by Terraform (non-overridable Glue arguments).
    parser.add_argument("--region", required=True)
    parser.add_argument("--source_prefix", required=True)
    parser.add_argument("--result_prefix", required=True)
    parser.add_argument("--stop_uri", required=True)
    parser.add_argument("--tracker", default="NONE")
    parser.add_argument("--tracker_table", default="")
    # Defaults from Terraform; Jenkins may override per run.
    parser.add_argument("--files_per_wave", default="100")
    parser.add_argument("--max_waves", default="5")
    parser.add_argument("--notifier", default="LOG")
    parser.add_argument("--notify_to", default="")
    parser.add_argument("--batch_id")
    return parser


def parse_find_args(argv=None):
    parser = _common_parser()
    parser.add_argument("--find_strategy")
    parser.add_argument("--input_uri")
    parser.add_argument("--archive_uri")
    parser.add_argument("--month_from")
    parser.add_argument("--month_to")
    parser.add_argument("--hash_mode")
    parser.add_argument("--tran_id_suffix")
    parser.add_argument("--retry_failed", default="false")
    for match_type, default in find_strategies.DEFAULT_TAGS.items():
        parser.add_argument(f"--{match_type.lower()}_tags", default=default)
    args, _ = parser.parse_known_args(argv)  # Glue adds its own arguments
    return find_config(args)


def parse_forget_args(argv=None):
    parser = _common_parser()
    parser.add_argument("--forget_strategy", default=forget_strategies.DEFAULT)
    args, _ = parser.parse_known_args(argv)
    return forget_config(args)


# ----------------------------------------------------------------------------- validation
def _common(args):
    for name in ("source_prefix", "result_prefix"):
        value = getattr(args, name)
        if not isinstance(value, str) or not value.startswith("s3://") or not value.endswith("/"):
            raise ValueError(f"--{name} must be an s3:// prefix ending in /")
    common.location(args.stop_uri)
    tracker = _choice(getattr(args, "tracker", None) or "NONE", tracking.TRACKERS, "--tracker")
    table = getattr(args, "tracker_table", None) or None
    if tracker == tracking.DYNAMODB and (not table or not TABLE.fullmatch(table)):
        raise ValueError("--tracker DYNAMODB needs --tracker_table (a DynamoDB table name)")
    return dict(
        region=args.region,
        source_prefix=args.source_prefix,
        result_prefix=args.result_prefix,
        stop_uri=args.stop_uri,
        batch_id=common.check_batch_id(args.batch_id),
        files_per_wave=common.parse_int(args.files_per_wave, "--files_per_wave", 1, common.MAX_FILES_PER_WAVE),
        max_waves=common.parse_int(args.max_waves, "--max_waves", 0, 1_000_000),
        tracker=tracker,
        tracker_table=table,
        notifier=_choice(getattr(args, "notifier", None) or "LOG", notify.NOTIFIERS, "--notifier"),
        notify_to=getattr(args, "notify_to", None) or "",
    )


def _choice(value, allowed, name):
    text = str(value).strip().upper()
    if text not in allowed:
        raise ValueError(f"{name} must be one of {', '.join(allowed)} (got {value!r})")
    return text


def find_config(args) -> FindConfig:
    """argparse.Namespace (or any object with the same attributes) -> FindConfig."""
    if isinstance(args, FindConfig):
        return args
    values = _common(args)
    strategy = getattr(args, "find_strategy", None) or None
    if strategy:
        strategy = _choice(strategy, tuple(find_strategies.FIND_STRATEGIES), "--find_strategy")
    month_from = common.parse_month(getattr(args, "month_from", None), "--month_from")
    month_to = common.parse_month(getattr(args, "month_to", None), "--month_to")
    if month_from and month_to and month_from > month_to:
        raise ValueError("--month_from is after --month_to")
    hash_mode = getattr(args, "hash_mode", None) or None
    if hash_mode and hash_mode not in common.HASH_MODES:
        raise ValueError(f"--hash_mode must be one of {', '.join(common.HASH_MODES)}")
    suffix = getattr(args, "tran_id_suffix", None) or None
    if suffix is not None and not SUFFIX.fullmatch(suffix):
        raise ValueError("--tran_id_suffix must be 1-32 letters, digits, _ . or -")
    tags = {t: getattr(args, t.lower() + "_tags", None) or default
            for t, default in find_strategies.DEFAULT_TAGS.items()}
    return FindConfig(
        **values,
        find_strategy=strategy,
        input_uri=getattr(args, "input_uri", None) or None,
        archive_uri=common.resolve_archive_uri(getattr(args, "archive_uri", None), args.source_prefix),
        month_from=month_from,
        month_to=month_to,
        hash_mode=hash_mode,
        tran_id_suffix=suffix,
        tags=tags,
        retry_failed=common.parse_bool(getattr(args, "retry_failed", "false"), "--retry_failed"),
    )


def forget_config(args) -> ForgetConfig:
    if isinstance(args, ForgetConfig):
        return args
    values = _common(args)
    strategy = _choice(getattr(args, "forget_strategy", None) or forget_strategies.DEFAULT,
                       tuple(forget_strategies.FORGET_STRATEGIES), "--forget_strategy")
    return ForgetConfig(**values, forget_strategy=strategy)
