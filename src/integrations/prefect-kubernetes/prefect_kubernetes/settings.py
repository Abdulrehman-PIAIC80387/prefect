from __future__ import annotations

import json
from functools import partial
from typing import Annotated, Optional, Union

from pydantic import AliasChoices, AliasPath, BeforeValidator, Field

from prefect.settings.base import PrefectBaseSettings, build_settings_config
from prefect.types import validate_set_T_from_delim_string


def _validate_secret_env_vars(
    value: dict[str, dict[str, str]] | str | None,
) -> dict[str, dict[str, str]]:
    """Validate and parse secret env var mappings.

    Accepts:
      - None → empty dict
      - dict already in the correct shape
      - JSON string: '{"ENV_VAR": {"name": "secret-name", "key": "secret-key"}}'
      - Compact string: 'ENV_VAR:secret-name:secret-key,ENV_VAR2:secret-name2:secret-key2'
    """
    if value is None:
        return {}
    if isinstance(value, dict):
        _check_secret_env_vars_not_empty(value)
        return value
    # Try JSON first
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            _check_secret_env_vars_not_empty(parsed)
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    # Fall back to compact colon-delimited format
    result: dict[str, dict[str, str]] = {}
    for entry in value.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) != 3:
            raise ValueError(
                f"Invalid secret_env_vars entry '{entry}': expected format "
                "'ENV_VAR_NAME:secret-name:secret-key'"
            )
        env_var, secret_name, secret_key = (p.strip() for p in parts)
        if not env_var or not secret_name or not secret_key:
            raise ValueError(
                f"Invalid secret_env_vars entry '{entry}': env var name, "
                "secret name, and secret key must all be non-empty"
            )
        result[env_var] = {"name": secret_name, "key": secret_key}
    return result


def _check_secret_env_vars_not_empty(
    mapping: dict[str, dict[str, str]],
) -> None:
    for env_var, ref in mapping.items():
        name = ref.get("name", "")
        key = ref.get("key", "")
        if not env_var or not name or not key:
            raise ValueError(
                f"Invalid secret_env_vars entry for '{env_var}': "
                "env var name, secret name, and secret key must all be non-empty"
            )


SecretEnvVars = Annotated[
    Union[dict[str, dict[str, str]], str, None],
    BeforeValidator(_validate_secret_env_vars),
]


def _validate_label_filters(value: dict[str, str] | str | None) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    split_value = value.split(",")
    return {
        k.strip(): v.strip() for k, v in (item.split("=", 1) for item in split_value)
    }


LabelFilters = Annotated[
    Union[dict[str, str], str, None], BeforeValidator(_validate_label_filters)
]
Namespaces = Annotated[
    Union[set[str], str, None],
    BeforeValidator(partial(validate_set_T_from_delim_string, type_=str)),
]


class KubernetesObserverSettings(PrefectBaseSettings):
    model_config = build_settings_config(("integrations", "kubernetes", "observer"))

    enabled: bool = Field(
        default=True,
        description="Whether the Kubernetes observer is enabled to watch for Prefect-submitted Kubernetes pod and job events.",
    )

    replicate_pod_events: bool = Field(
        default=True,
        description="Whether the Kubernetes observer should replicate Prefect-submitted Kubernetes pod events, which can be used for Prefect Automations.",
    )

    namespaces: Namespaces = Field(
        default_factory=set,
        description="The namespaces to watch for Prefect-submitted Kubernetes "
        "jobs and pods. If not provided, the watch will be cluster-wide.",
    )

    additional_label_filters: LabelFilters = Field(
        default_factory=dict,
        description="Additional label filters to apply to the watch for "
        "Prefect-submitted Kubernetes jobs and pods. If not provided, the watch will "
        "include all pods and jobs with the `prefect.io/flow-run-id` label. Labels "
        "should be provided in the format `key=value`.",
    )

    startup_event_concurrency: int = Field(
        default=5,
        description="Maximum number of concurrent API calls when checking for "
        "duplicate events during observer startup. This helps prevent overloading "
        "the API server when there are many existing pods/jobs in the cluster.",
    )

    forward_crashed_run_logs: bool = Field(
        default=True,
        description="Whether to fetch and forward container logs for flow runs "
        "that crashed before establishing connectivity to the Prefect server "
        "(e.g., OOMKilled during import, bad entrypoint, missing dependencies).",
    )

    forward_crashed_run_logs_tail_lines: int = Field(
        default=500,
        ge=1,
        description="Number of tail lines to fetch from crashed pod containers "
        "when forwarding logs.",
    )


class KubernetesWorkerCreateJobRetrySettings(PrefectBaseSettings):
    model_config = build_settings_config(
        ("integrations", "kubernetes", "worker", "create_job_retry")
    )

    max_retries: int = Field(
        default=3,
        ge=1,
        description="The maximum number of attempts to retry creating a Kubernetes job before giving up.",
    )

    delay_seconds: int = Field(
        default=1,
        ge=0,
        description="The fixed delay in seconds between retries when creating a Kubernetes job.",
    )

    jitter_min_seconds: int = Field(
        default=0,
        ge=0,
        description="The minimum jitter in seconds to add to the delay between retries when creating a Kubernetes job.",
    )

    jitter_max_seconds: int = Field(
        default=3,
        ge=0,
        description="The maximum jitter in seconds to add to the delay between retries when creating a Kubernetes job.",
    )


class KubernetesWorkerSettings(PrefectBaseSettings):
    model_config = build_settings_config(("integrations", "kubernetes", "worker"))

    api_key_secret_name: Optional[str] = Field(
        default=None,
        description="The name of the secret the worker's API key is stored in.",
    )

    api_key_secret_key: Optional[str] = Field(
        default=None,
        description="The key of the secret the worker's API key is stored in.",
    )

    api_auth_string_secret_name: Optional[str] = Field(
        default=None,
        description="The name of the secret the worker's API auth string is stored in.",
    )

    api_auth_string_secret_key: Optional[str] = Field(
        default=None,
        description="The key of the secret the worker's API auth string is stored in.",
    )

    secret_env_vars: SecretEnvVars = Field(
        default_factory=dict,
        description=(
            "A mapping of environment variable names to Kubernetes secret references. "
            "Each entry replaces the plaintext env var in the Job manifest with a "
            "valueFrom.secretKeyRef. Accepts a JSON object "
            '(\'{"ENV_VAR": {"name": "secret-name", "key": "secret-key"}}\') '
            "or a compact string ('ENV_VAR:secret-name:secret-key,...')."
        ),
    )

    create_secret_for_api_key: bool = Field(
        default=False,
        description="If `True`, the worker will create a secret in the same namespace as created Kubernetes jobs to store the Prefect API key.",
        validation_alias=AliasChoices(
            AliasPath("create_secret_for_api_key"),
            "prefect_integrations_kubernetes_worker_create_secret_for_api_key",
            "prefect_kubernetes_worker_store_prefect_api_in_secret",
        ),
    )

    add_tcp_keepalive: bool = Field(
        default=True,
        description="If `True`, the worker will add TCP keepalive to the Kubernetes client.",
        validation_alias=AliasChoices(
            AliasPath("add_tcp_keepalive"),
            "prefect_integrations_kubernetes_worker_add_tcp_keepalive",
            "prefect_kubernetes_worker_add_tcp_keepalive",
        ),
    )

    create_job_retry: KubernetesWorkerCreateJobRetrySettings = Field(
        description="Settings for controlling retry behavior when creating Kubernetes jobs.",
        default_factory=KubernetesWorkerCreateJobRetrySettings,
    )


class KubernetesSettings(PrefectBaseSettings):
    model_config = build_settings_config(("integrations", "kubernetes"))

    cluster_uid: Optional[str] = Field(
        default=None,
        description="A unique identifier for the current cluster being used.",
        validation_alias=AliasChoices(
            AliasPath("cluster_uid"),
            "prefect_integrations_kubernetes_cluster_uid",
            "prefect_kubernetes_cluster_uid",
        ),
    )

    worker: KubernetesWorkerSettings = Field(
        description="Settings for controlling Kubernetes worker behavior.",
        default_factory=KubernetesWorkerSettings,
    )

    observer: KubernetesObserverSettings = Field(
        description="Settings for controlling Kubernetes observer behavior.",
        default_factory=KubernetesObserverSettings,
    )
