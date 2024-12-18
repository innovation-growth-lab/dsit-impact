"""Project settings. There is no need to edit this file unless you want to change values
from the Kedro defaults. For further information, including these default values, see
https://kedro.readthedocs.io/en/stable/kedro_project_setup/settings.html."""

CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "base",
    "config_patterns": {
        "parameters": ["parameters*", "parameters*/**", "**/parameters*"],
    }
}

GTR_ENDPOINTS = ["projects", "publications", "organisations", "funds"]