import importlib


def _import_optional_dependency(name):
    try:
        module = importlib.import_module(name)
    except ImportError as e:
        raise ImportError(f"Missing optional dependency '{name}'") from e
    return module
